from __future__ import annotations

import csv
import hashlib
import importlib
import json
import os
import tempfile
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand import __version__

IMERG_SHORT_NAME = "GPM_3IMERGHH"
IMERG_VERSION = "07"
RUN_NAME = "Final"
HALF_HOUR_FACTOR = 0.5
MAX_DOWNLOAD_ATTEMPTS = 3
HALF_HOURS_PER_HOUR = 2


class ImergError(ValueError):
    """Raised when IMERG feasibility processing cannot continue."""


class ImergClient(Protocol):
    def search_granules(
        self,
        short_name: str,
        version: str,
        temporal: tuple[datetime, datetime],
        bounding_box: tuple[float, float, float, float] | None,
    ) -> list[Mapping[str, Any]]: ...

    def download(self, url: str, output_path: Path) -> None: ...


class EarthaccessImergClient:
    """Thin adapter that isolates GeoDemand from Earthaccess result layouts."""

    def __init__(self, interactive: bool = False, module: Any | None = None) -> None:
        try:
            self._earthaccess = module or importlib.import_module("earthaccess")
        except Exception as exc:
            raise ImergError(
                "NASA support is unavailable. Install the 'imerg' optional dependency group."
            ) from exc
        self._interactive = interactive

    def search_granules(
        self,
        short_name: str,
        version: str,
        temporal: tuple[datetime, datetime],
        bounding_box: tuple[float, float, float, float] | None,
    ) -> list[Mapping[str, Any]]:
        arguments: dict[str, Any] = {
            "short_name": short_name,
            "version": version,
            "temporal": tuple(value.isoformat() for value in temporal),
        }
        if bounding_box is not None:
            arguments["bounding_box"] = bounding_box
        try:
            results = self._earthaccess.search_data(**arguments)
        except Exception as exc:
            raise ImergError(f"NASA CMR granule search failed: {exc}") from exc
        return [_earthaccess_granule(row, bounding_box) for row in results]

    def download(self, url: str, output_path: Path) -> None:
        auth = _auth_state()
        if not auth["available"] and not self._interactive:
            raise ImergError(
                "Earthdata authentication is unavailable. Configure an environment strategy, "
                "a user-managed .netrc, or explicitly request interactive authentication."
            )
        strategy = "interactive" if self._interactive else str(auth["strategy"])
        if strategy in {"EARTHDATA_TOKEN", "EARTHDATA_USERNAME_PASSWORD"}:
            strategy = "environment"
        self._earthaccess.login(strategy=strategy, persist=False)
        paths = self._earthaccess.download(
            [url], local_path=output_path.parent, threads=1, show_progress=False
        )
        if not paths:
            raise ImergError(f"Earthaccess returned no downloaded file for {url}")
        downloaded = Path(paths[0])
        if not downloaded.exists():
            raise ImergError(f"Earthaccess reported a missing downloaded file: {downloaded}")
        downloaded.replace(output_path)


def selfcheck_imerg(working_root: Path, interactive: bool = False) -> dict[str, Any]:
    working_root.mkdir(parents=True, exist_ok=True)
    auth = _auth_state()
    return {
        "package_version": __version__,
        "earthaccess_import": _check_import("earthaccess"),
        "h5py_import": _check_import("h5py"),
        "xarray_import": _check_import("xarray"),
        "temporary_file_write": _check_temp_write(working_root),
        "authentication": auth,
        "earthdata_service_status": {"ok": False, "reason": "network check is opt-in"},
        "cmr_search": {"ok": False, "reason": "network check is opt-in"},
        "collection": {
            "ok": False,
            "short_name": IMERG_SHORT_NAME,
            "version": IMERG_VERSION,
            "reason": "collection search not run",
        },
        "real_granule_decode": {"ok": False, "reason": "no real granule supplied"},
        "interactive_auth_requested": interactive,
    }


def inventory_imerg(
    sample_path: Path,
    output_root: Path,
    client: ImergClient | None = None,
    max_episodes: int | None = None,
    short_name: str = IMERG_SHORT_NAME,
    version: str = IMERG_VERSION,
) -> dict[str, Path]:
    rows = _read_rows(sample_path)
    if max_episodes is not None:
        rows = rows[:max_episodes]
    granules: dict[str, dict[str, Any]] = {}
    availability: Counter[str] = Counter()
    for row in rows:
        start, end = _window(row)
        found = _search(client, short_name, version, start, end, _bbox(row))
        if not found:
            availability["missing"] += 1
            continue
        for item in found:
            granule_id = str(item["granule_id"])
            existing = granules.get(granule_id)
            episode_id = str(row["episode_id"])
            if existing:
                existing["requesting_episode_ids"] += f";{episode_id}"
                continue
            availability["available"] += 1
            granules[granule_id] = {
                "collection_short_name": short_name,
                "collection_version": version,
                "collection_run": RUN_NAME,
                "cmr_concept_id": str(item.get("concept_id", "")),
                "granule_id": granule_id,
                "beginning_time_utc": _iso(item["beginning_time_utc"]),
                "ending_time_utc": _iso(item["ending_time_utc"]),
                "production_time_utc": _iso(item.get("production_time_utc")),
                "bounding_information": json.dumps(item.get("bounding_box", ""), sort_keys=True),
                "access_url": str(item.get("access_url", "")),
                "expected_file_size": int(item.get("file_size") or 0),
                "requesting_episode_ids": episode_id,
                "availability_status": "available",
                "selection_reason": "cmr_temporal_spatial_match",
            }
    output_dir = output_root / "inventory"
    output_dir.mkdir(parents=True, exist_ok=True)
    inventory_path = output_dir / "imerg_inventory.parquet"
    plan_path = output_dir / "imerg_download_plan.csv"
    availability_path = output_dir / "imerg_product_availability.csv"
    summary_path = output_dir / "imerg_inventory_summary.json"
    inventory_rows = sorted(granules.values(), key=lambda row: row["granule_id"])
    for row in inventory_rows:
        row["requesting_episode_ids"] = ";".join(
            sorted(set(str(row["requesting_episode_ids"]).split(";")))
        )
    _write_parquet(inventory_path, inventory_rows)
    _write_csv(plan_path, inventory_rows, _inventory_fields())
    availability_rows = [
        {"availability_status": key, "count": value} for key, value in sorted(availability.items())
    ]
    _write_csv(availability_path, availability_rows, ["availability_status", "count"])
    _write_json(
        summary_path,
        {
            "collection_short_name": short_name,
            "collection_version": version,
            "collection_run": RUN_NAME,
            "unique_granule_count": len(inventory_rows),
            "estimated_download_bytes": sum(
                int(row["expected_file_size"]) for row in inventory_rows
            ),
        },
    )
    return {
        "imerg_inventory": inventory_path,
        "imerg_download_plan": plan_path,
        "imerg_product_availability": availability_path,
        "imerg_inventory_summary": summary_path,
    }


def fetch_imerg(
    download_plan: Path,
    output_root: Path,
    client: ImergClient | None = None,
    max_episodes: int | None = None,
    max_bytes: int | None = None,
) -> dict[str, Path]:
    rows = _read_csv(download_plan)
    if max_episodes is not None:
        allowed = sorted(
            {
                episode_id
                for row in rows
                for episode_id in row.get("requesting_episode_ids", "").split(";")
                if episode_id
            }
        )[:max_episodes]
        rows = [
            row
            for row in rows
            if set(row.get("requesting_episode_ids", "").split(";")) & set(allowed)
        ]
    cache_dir = output_root / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    downloaded = 0
    for row in rows:
        size = int(row.get("expected_file_size") or 0)
        if max_bytes is not None and downloaded + size > max_bytes:
            failure_rows.append({**row, "failure_reason": "max_bytes_exceeded"})
            continue
        cache_path = cache_dir / _cache_name(row["granule_id"], row["access_url"])
        cache_hit = cache_path.exists() and (not size or cache_path.stat().st_size == size)
        if not cache_hit:
            if client is None:
                failure_rows.append({**row, "failure_reason": "download_client_unavailable"})
                continue
            try:
                _download_atomic(client, row["access_url"], cache_path)
            except Exception as exc:
                failure_rows.append({**row, "failure_reason": f"{type(exc).__name__}: {exc}"})
                continue
            if size and cache_path.stat().st_size != size:
                actual_size = cache_path.stat().st_size
                cache_path.unlink(missing_ok=True)
                failure_rows.append(
                    {
                        **row,
                        "failure_reason": (
                            f"content_length_mismatch: expected {size}, received {actual_size}"
                        ),
                    }
                )
                continue
            downloaded += cache_path.stat().st_size
        manifest_rows.append(
            {
                **row,
                "cache_path": str(cache_path),
                "cache_hit": cache_hit,
                "sha256": _sha256(cache_path),
                "local_size": cache_path.stat().st_size,
                "authentication_strategy": _auth_state()["strategy"],
            }
        )
    manifest_path = output_root / "imerg_file_manifest.parquet"
    failures_path = output_root / "imerg_download_failures.csv"
    summary_path = output_root / "imerg_fetch_summary.json"
    _write_parquet(manifest_path, manifest_rows)
    _write_csv(
        failures_path,
        failure_rows,
        list(failure_rows[0]) if failure_rows else ["granule_id", "failure_reason"],
    )
    _write_json(summary_path, {"downloaded_bytes": downloaded, "manifest_rows": len(manifest_rows)})
    return {
        "imerg_file_manifest": manifest_path,
        "imerg_download_failures": failures_path,
        "imerg_fetch_summary": summary_path,
    }


def estimate_imerg_fetch(
    download_plan: Path,
    max_episodes: int | None = None,
    max_bytes: int | None = None,
) -> dict[str, int]:
    rows = _bounded_plan_rows(_read_csv(download_plan), max_episodes)
    selected = 0
    selected_bytes = 0
    skipped_by_bytes = 0
    for row in rows:
        size = int(row.get("expected_file_size") or 0)
        if max_bytes is not None and selected_bytes + size > max_bytes:
            skipped_by_bytes += 1
            continue
        selected += 1
        selected_bytes += size
    return {
        "planned_granules": selected,
        "estimated_download_bytes": selected_bytes,
        "skipped_by_max_bytes": skipped_by_bytes,
    }


def extract_imerg(sample_path: Path, file_manifest: Path, output_root: Path) -> dict[str, Path]:
    del sample_path, file_manifest, output_root
    raise ImergError(
        "real_extraction_not_implemented: IMERG HDF5 decoding and geometry-aware "
        "episode extraction are pending; no metrics were written."
    )


def rate_to_half_hour_mm(
    rate_mm_per_hour: float | None, fill_value: float | None = None
) -> float | None:
    if rate_mm_per_hour is None:
        return None
    if fill_value is not None and rate_mm_per_hour == fill_value:
        return None
    if rate_mm_per_hour < 0:
        raise ImergError(f"Negative IMERG precipitation rate is not valid: {rate_mm_per_hour}")
    return rate_mm_per_hour * HALF_HOUR_FACTOR


def aggregate_half_hours_to_hourly(values: Sequence[float | None]) -> list[float | None]:
    hourly: list[float | None] = []
    for index in range(0, len(values), 2):
        pair = values[index : index + 2]
        if len(pair) < HALF_HOURS_PER_HOUR or pair[0] is None or pair[1] is None:
            hourly.append(None)
        else:
            hourly.append(pair[0] + pair[1])
    return hourly


def _search(
    client: ImergClient | None,
    short_name: str,
    version: str,
    start: datetime,
    end: datetime,
    bbox: tuple[float, float, float, float] | None,
) -> list[Mapping[str, Any]]:
    if client is None:
        return []
    return client.search_granules(short_name, version, (start, end), bbox)


def _earthaccess_granule(
    granule: Mapping[str, Any],
    requested_bbox: tuple[float, float, float, float] | None,
) -> dict[str, Any]:
    meta = cast(Mapping[str, Any], granule.get("meta", {}))
    umm = cast(Mapping[str, Any], granule.get("umm", {}))
    temporal = cast(Mapping[str, Any], umm.get("TemporalExtent", {}))
    range_time = cast(Mapping[str, Any], temporal.get("RangeDateTime", {}))
    data_granule = cast(Mapping[str, Any], umm.get("DataGranule", {}))
    links_method = getattr(granule, "data_links", None)
    links = list(links_method()) if callable(links_method) else []
    size_method = getattr(granule, "size", None)
    size_mb = float(size_method()) if callable(size_method) else 0.0
    granule_id = str(umm.get("GranuleUR") or meta.get("concept-id") or "")
    if not granule_id:
        raise ImergError("NASA CMR returned a granule without GranuleUR or concept identifier.")
    return {
        "granule_id": granule_id,
        "concept_id": str(meta.get("concept-id", "")),
        "beginning_time_utc": str(range_time.get("BeginningDateTime", "")),
        "ending_time_utc": str(range_time.get("EndingDateTime", "")),
        "production_time_utc": str(data_granule.get("ProductionDateTime", "")),
        "bounding_box": requested_bbox or (),
        "access_url": links[0] if links else "",
        "file_size": round(size_mb * 1024 * 1024),
    }


def _window(row: Mapping[str, Any]) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(str(row["episode_start_date"])).replace(tzinfo=UTC)
    end = datetime.fromisoformat(str(row["episode_end_date"])).replace(tzinfo=UTC)
    return start - timedelta(hours=6), end + timedelta(hours=6)


def _bbox(row: Mapping[str, Any]) -> tuple[float, float, float, float] | None:
    lon = row.get("representative_longitude")
    lat = row.get("representative_latitude")
    if lon is None or lat is None:
        return None
    return (float(lon) - 0.5, float(lat) - 0.5, float(lon) + 0.5, float(lat) + 0.5)


def _download_atomic(client: ImergClient, url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=path.parent) as handle:
        tmp_path = Path(handle.name)
    try:
        for attempt in range(MAX_DOWNLOAD_ATTEMPTS):
            try:
                client.download(url, tmp_path)
                break
            except Exception:
                if attempt == MAX_DOWNLOAD_ATTEMPTS - 1:
                    raise
                time.sleep(0.25 * 2**attempt)
        tmp_path.replace(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _auth_state() -> dict[str, Any]:
    if os.getenv("EARTHDATA_TOKEN"):
        return {"available": True, "strategy": "EARTHDATA_TOKEN"}
    if os.getenv("EARTHDATA_USERNAME") and os.getenv("EARTHDATA_PASSWORD"):
        return {"available": True, "strategy": "EARTHDATA_USERNAME_PASSWORD"}
    netrc_path = os.getenv("NETRC")
    if (netrc_path and Path(netrc_path).exists()) or any(
        (Path.home() / name).exists() for name in (".netrc", "_netrc")
    ):
        return {"available": True, "strategy": "netrc"}
    return {"available": False, "strategy": "none"}


def _check_import(module: str) -> dict[str, Any]:
    try:
        imported = importlib.import_module(module)
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}
    return {"ok": True, "version": str(getattr(imported, "__version__", "unknown"))}


def _check_temp_write(root: Path) -> dict[str, Any]:
    try:
        with tempfile.NamedTemporaryFile(dir=root, delete=True) as handle:
            handle.write(b"ok")
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def _cache_name(granule_id: str, url: str) -> str:
    suffix = Path(url).name or f"{granule_id}.h5"
    return f"{hashlib.sha256(granule_id.encode()).hexdigest()[:16]}_{suffix}"


def _inventory_fields() -> list[str]:
    return [
        "collection_short_name",
        "collection_version",
        "collection_run",
        "cmr_concept_id",
        "granule_id",
        "beginning_time_utc",
        "ending_time_utc",
        "production_time_utc",
        "bounding_information",
        "access_url",
        "expected_file_size",
        "requesting_episode_ids",
        "availability_status",
        "selection_reason",
    ]


def _iso(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def _read_rows(path: Path) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _bounded_plan_rows(
    rows: list[dict[str, str]], max_episodes: int | None
) -> list[dict[str, str]]:
    if max_episodes is None:
        return rows
    allowed = sorted(
        {
            episode_id
            for row in rows
            for episode_id in row.get("requesting_episode_ids", "").split(";")
            if episode_id
        }
    )[:max_episodes]
    allowed_set = set(allowed)
    return [
        row for row in rows if set(row.get("requesting_episode_ids", "").split(";")) & allowed_set
    ]


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
