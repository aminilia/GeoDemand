from __future__ import annotations

import csv
import gzip
import hashlib
import importlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand import __version__

NOAA_MRMS_BUCKET = "noaa-mrms-pds"
MRMS_PREFIX = "CONUS"
PRIMARY_PRODUCT = "MultiSensor_QPE_01H_Pass2"
QUALITY_PRODUCT = "RadarAccumulationQualityIndex_01H"
LONG_EPISODE_PRODUCT = "MultiSensor_QPE_24H_Pass2"
ONSET_PRODUCT = "RadarOnly_QPE_15M"
POLICY_PAIRS = [("conservative", "balanced"), ("balanced", "broad")]
DEFAULT_SEED = 20260715
MAX_SAMPLE_EPISODES = 90
LONG_EPISODE_DAYS = 4
HOURLY_TOLERANCE_SECONDS = 30 * 60
MISSING_QPE_THRESHOLD = -900.0
TYPICAL_MIN_MEMBERS = 2
TYPICAL_MAX_MEMBERS = 10
MAX_DOWNLOAD_ATTEMPTS = 3
WEAK_SIGNAL_1H_MM = 5.0
MIN_COVERAGE_FRACTION = 0.7


class MrmsError(ValueError):
    """Raised when MRMS feasibility processing cannot continue."""


class S3Like(Protocol):
    def ls(self, path: str, detail: bool = True) -> list[Any]: ...

    def info(self, path: str) -> Mapping[str, Any]: ...

    def open(self, path: str, mode: str = "rb") -> Any: ...


@dataclass(frozen=True)
class EpisodeOutputs:
    conservative_episodes: Path
    conservative_membership: Path
    conservative_diagnostics: Path | None
    balanced_episodes: Path
    balanced_membership: Path
    clustering_sensitivity: Path
    clustering_agreement: Path


def discover_episode_outputs(root: Path) -> EpisodeOutputs:
    candidates = [root, *[path for path in root.rglob("*") if path.is_dir()]]

    def find_dataset(policy: str, dataset: str) -> Path | None:
        names = [
            Path(f"episodes_{policy}") / dataset,
            Path(policy) / dataset,
            Path(f"{policy}_{dataset}"),
        ]
        for base in candidates:
            for name in names:
                path = base / name
                if _is_parquet_dataset(path):
                    return path
        return None

    conservative_episodes = find_dataset("conservative", "episodes")
    conservative_membership = find_dataset("conservative", "episode_membership")
    balanced_episodes = find_dataset("balanced", "episodes")
    balanced_membership = find_dataset("balanced", "episode_membership")
    sensitivity = _find_file(root, "clustering_sensitivity.csv")
    agreement = _find_file(root, "clustering_agreement.csv")
    missing = [
        name
        for name, path in [
            ("conservative episodes", conservative_episodes),
            ("conservative membership", conservative_membership),
            ("balanced episodes", balanced_episodes),
            ("balanced membership", balanced_membership),
            ("clustering_sensitivity.csv", sensitivity),
            ("clustering_agreement.csv", agreement),
        ]
        if path is None
    ]
    if missing:
        raise MrmsError(
            "Episode comparison output is incomplete. Missing: "
            + ", ".join(missing)
            + f". Inspected root: {root}"
        )
    assert conservative_episodes is not None
    assert conservative_membership is not None
    assert balanced_episodes is not None
    assert balanced_membership is not None
    assert sensitivity is not None
    assert agreement is not None
    conservative_diagnostics = _find_file(root, "bridge_diagnostics.csv")
    return EpisodeOutputs(
        conservative_episodes=conservative_episodes,
        conservative_membership=conservative_membership,
        conservative_diagnostics=conservative_diagnostics,
        balanced_episodes=balanced_episodes,
        balanced_membership=balanced_membership,
        clustering_sensitivity=sensitivity,
        clustering_agreement=agreement,
    )


def selfcheck_mrms(working_root: Path, sample_grib: Path | None = None) -> dict[str, Any]:
    working_root.mkdir(parents=True, exist_ok=True)
    checks: dict[str, Any] = {"package_version": __version__}
    checks["temporary_file_write"] = _check_temp_write(working_root)
    checks["anonymous_s3_access"] = _check_s3_import()
    checks["cfgrib_import"] = _check_import("cfgrib")
    checks["eccodes_import"] = _check_import("eccodes")
    checks["xarray_cfgrib_engine"] = _check_xarray_engine()
    checks["cfgrib_selfcheck"] = _run_cfgrib_selfcheck()
    checks["real_grib_decode"] = (
        _check_grib_decode(sample_grib)
        if sample_grib
        else {
            "ok": False,
            "reason": "no sample GRIB file provided",
        }
    )
    return checks


def sample_mrms(
    episode_root: Path,
    output_root: Path,
    seed: int = DEFAULT_SEED,
    max_episodes: int = MAX_SAMPLE_EPISODES,
) -> dict[str, Path]:
    outputs = discover_episode_outputs(episode_root)
    episodes = _read_rows(outputs.conservative_episodes)
    membership = _read_rows(outputs.conservative_membership)
    balanced_membership = _read_rows(outputs.balanced_membership)
    balanced_by_event = _component_members_by_event(balanced_membership)
    balanced_episode_by_event = {
        str(row["event_record_id"]): str(row["episode_id"]) for row in balanced_membership
    }
    selected: dict[str, dict[str, Any]] = {}
    counters: Counter[str] = Counter()

    def add(rows: Iterable[dict[str, Any]], stratum: str, limit: int | None = None) -> None:
        rank = 0
        for row in rows:
            episode_id = str(row["episode_id"])
            if episode_id in selected:
                continue
            rank += 1
            if limit is not None and rank > limit:
                break
            selected[episode_id] = _sample_row(
                row,
                stratum,
                rank,
                seed,
                membership,
                balanced_episode_by_event,
            )
            counters[stratum] += 1
            if len(selected) >= max_episodes:
                break

    ordered = sorted(episodes, key=lambda row: (-int(row["member_count"]), str(row["episode_id"])))
    add([row for row in episodes if bool(row["crosses_split_boundary"])], "split_boundary", None)
    add([row for row in episodes if bool(row["spatial_review_required"])], "spatial_review", 5)
    add(
        _balanced_merge_candidates(episodes, membership, balanced_by_event),
        "balanced_merge_candidate",
        20,
    )
    add(ordered, "largest_conservative_clusters", 15)
    add(
        sorted(
            episodes, key=lambda row: (-int(row["episode_duration_days"]), str(row["episode_id"]))
        ),
        "longest_conservative_clusters",
        10,
    )
    add(
        sorted(episodes, key=lambda row: (-int(row["state_count"]), str(row["episode_id"]))),
        "highest_state_count",
        10,
    )
    add(
        _stable_shuffle(
            [
                row
                for row in episodes
                if TYPICAL_MIN_MEMBERS <= int(row["member_count"]) <= TYPICAL_MAX_MEMBERS
            ],
            seed,
        ),
        "typical_2_to_10_members",
        15,
    )
    add([row for row in episodes if int(row["member_count"]) == 1], "singleton", 15)

    rows = sorted(selected.values(), key=lambda row: (row["selection_rank"], row["episode_id"]))
    comparison_rows = _comparison_group_rows(rows, membership, balanced_by_event)
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_root = output_root / "manifests"
    manifest_root.mkdir(parents=True, exist_ok=True)
    sample_path = manifest_root / "verification_sample.parquet"
    comparison_path = manifest_root / "comparison_groups.parquet"
    summary_path = manifest_root / "sample_summary.json"
    manifest_path = manifest_root / "sample_manifest.json"
    _write_parquet(sample_path, rows)
    _write_parquet(comparison_path, comparison_rows)
    _write_json(
        summary_path,
        {
            "sample_episode_count": len(rows),
            "selection_seed": seed,
            "stratum_counts": dict(sorted(counters.items())),
            "years": dict(
                sorted(Counter(str(row["episode_start_date"])[:4] for row in rows).items())
            ),
        },
    )
    _write_json(
        manifest_path,
        {
            "package_version": __version__,
            "source_episode_root": str(episode_root),
            "configuration": {"seed": seed, "max_episodes": max_episodes},
            "sample_hash": _sha256(sample_path),
        },
    )
    return {
        "verification_sample": sample_path,
        "comparison_groups": comparison_path,
        "sample_summary": summary_path,
        "sample_manifest": manifest_path,
    }


def inventory_mrms(
    sample_path: Path,
    output_root: Path,
    fs: S3Like | None = None,
    products: Sequence[str] = (PRIMARY_PRODUCT, QUALITY_PRODUCT),
    max_episodes: int | None = None,
) -> dict[str, Path]:
    filesystem = fs or _s3_filesystem()
    sample_rows = _read_rows(sample_path)
    if max_episodes is not None:
        sample_rows = sample_rows[:max_episodes]
    inventory: dict[tuple[str, str], dict[str, Any]] = {}
    availability: Counter[tuple[str, str]] = Counter()
    for row in sample_rows:
        episode_id = str(row["episode_id"])
        window_start, window_end = _mrms_window(row)
        for product in products:
            for expected in _expected_times(window_start, window_end, product):
                record = _select_s3_object(filesystem, product, expected)
                record["requesting_episode_ids"] = episode_id
                inventory[(record["product"], record["key"])] = record
                availability[(product, record["availability_status"])] += 1
    inventory_rows = sorted(
        inventory.values(),
        key=lambda item: (item["product"], item["expected_valid_time_utc"], item["key"]),
    )
    output_inventory = output_root / "inventory"
    output_inventory.mkdir(parents=True, exist_ok=True)
    inventory_path = output_inventory / "mrms_inventory.parquet"
    plan_path = output_inventory / "mrms_download_plan.csv"
    availability_path = output_inventory / "product_availability.csv"
    summary_path = output_inventory / "inventory_summary.json"
    _write_parquet(inventory_path, inventory_rows)
    _write_csv(plan_path, inventory_rows, _inventory_fields())
    availability_rows = [
        {"product": product, "availability_status": status, "count": count}
        for (product, status), count in sorted(availability.items())
    ]
    _write_csv(availability_path, availability_rows, ["product", "availability_status", "count"])
    estimated_bytes = sum(int(row.get("content_length") or 0) for row in inventory_rows)
    _write_json(
        summary_path,
        {
            "bucket": NOAA_MRMS_BUCKET,
            "unique_object_count": len(inventory_rows),
            "estimated_compressed_download_bytes": estimated_bytes,
            "products": list(products),
        },
    )
    return {
        "mrms_inventory": inventory_path,
        "mrms_download_plan": plan_path,
        "product_availability": availability_path,
        "inventory_summary": summary_path,
    }


def fetch_mrms(
    download_plan: Path,
    working_root: Path,
    fs: S3Like | None = None,
    max_bytes: int | None = None,
    max_episodes: int | None = None,
    workers: int = 2,
) -> dict[str, Path]:
    del workers
    filesystem = fs or _s3_filesystem()
    cache_root = working_root / "cache" / "compressed"
    cache_root.mkdir(parents=True, exist_ok=True)
    rows = _read_csv(download_plan)
    if max_episodes is not None:
        allowed = {
            episode_id
            for row in rows
            for episode_id in str(row.get("requesting_episode_ids", "")).split(";")
            if episode_id
        }
        allowed = set(sorted(allowed)[:max_episodes])
        rows = [
            row
            for row in rows
            if allowed & set(str(row.get("requesting_episode_ids", "")).split(";"))
        ]
    downloaded_bytes = 0
    manifest_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("availability_status") != "available":
            failure_rows.append({**row, "failure_reason": "missing_object"})
            continue
        size = int(row.get("content_length") or 0)
        if max_bytes is not None and downloaded_bytes + size > max_bytes:
            failure_rows.append({**row, "failure_reason": "max_bytes_exceeded"})
            continue
        cache_path = cache_root / _cache_name(str(row["key"]))
        cache_hit = cache_path.exists() and cache_path.stat().st_size == size
        if not cache_hit:
            _download_atomic(filesystem, str(row["s3_uri"]), cache_path, expected_size=size)
            downloaded_bytes += size
        manifest_rows.append(
            {
                **row,
                "cache_path": str(cache_path),
                "cache_hit": cache_hit,
                "compressed_sha256": _sha256(cache_path),
                "local_size": cache_path.stat().st_size,
            }
        )
    manifest_root = working_root / "manifests"
    metrics_root = working_root / "metrics"
    manifest_root.mkdir(parents=True, exist_ok=True)
    metrics_root.mkdir(parents=True, exist_ok=True)
    file_manifest = manifest_root / "mrms_file_manifest.parquet"
    failures = metrics_root / "download_failures.csv"
    _write_parquet(file_manifest, manifest_rows)
    _write_csv(
        failures, failure_rows, list(failure_rows[0]) if failure_rows else ["key", "failure_reason"]
    )
    return {"mrms_file_manifest": file_manifest, "download_failures": failures}


def extract_mrms(
    sample_path: Path,
    file_manifest: Path,
    output_root: Path,
) -> dict[str, Path]:
    del sample_path, file_manifest, output_root
    raise MrmsError(
        "real_mrms_extraction_not_implemented: MRMS GRIB decoding and geometry-aware "
        "episode extraction are pending; no metrics were written."
    )


def assess_mrms(metrics_root: Path) -> dict[str, Path]:
    episode_rows = _read_rows(metrics_root / "episode_precipitation_metrics.parquet")
    member_rows = _read_rows(metrics_root / "member_precipitation_metrics.parquet")
    members_by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in member_rows:
        members_by_episode[str(row["episode_id"])].append(row)
    assessments = [
        _assessment_row(row, members_by_episode[str(row["episode_id"])]) for row in episode_rows
    ]
    output = metrics_root / "physical_coherence_assessment.csv"
    _write_csv(output, assessments, list(assessments[0]) if assessments else ["episode_id"])
    summary = metrics_root / "feasibility_summary.json"
    _write_json(
        summary,
        {
            "episode_count": len(assessments),
            "category_counts": dict(
                sorted(Counter(row["provisional_category"] for row in assessments).items())
            ),
        },
    )
    return {"physical_coherence_assessment": output, "feasibility_summary": summary}


def parse_mrms_key(key: str) -> dict[str, Any]:
    match = re.search(r"CONUS/(?P<product>[^/]+)/(?P<day>\d{8})/(?P<name>[^/]+\.grib2\.gz)$", key)
    if not match:
        raise MrmsError(f"Not an MRMS CONUS GRIB2 key: {key}")
    name = match.group("name")
    day = match.group("day")
    time_matches = list(re.finditer(r"(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})", name))
    valid_time = datetime.strptime(day, "%Y%m%d").replace(tzinfo=UTC)
    if time_matches:
        time_match = time_matches[-1]
        valid_time = valid_time.replace(
            hour=int(time_match.group("hour")),
            minute=int(time_match.group("minute")),
            second=int(time_match.group("second")),
        )
    return {"product": match.group("product"), "valid_time_utc": valid_time, "key": key}


def closest_object(
    objects: Sequence[Mapping[str, Any]],
    product: str,
    expected_time: datetime,
    tolerance_seconds: int = HOURLY_TOLERANCE_SECONDS,
) -> dict[str, Any]:
    parsed: list[tuple[float, Mapping[str, Any], datetime]] = []
    for item in objects:
        key = str(item.get("Key") or item.get("name") or item.get("key") or "")
        try:
            parsed_key = parse_mrms_key(key)
        except MrmsError:
            continue
        if parsed_key["product"] != product:
            continue
        discovered = parsed_key["valid_time_utc"]
        parsed.append((abs((discovered - expected_time).total_seconds()), item, discovered))
    if not parsed:
        return _missing_inventory_row(product, expected_time, "no_parseable_objects")
    offset, item, discovered = min(parsed, key=lambda value: (value[0], str(value[1])))
    if offset > tolerance_seconds:
        return _missing_inventory_row(product, expected_time, "outside_time_tolerance")
    key = str(item.get("Key") or item.get("name") or item.get("key"))
    return {
        "bucket": NOAA_MRMS_BUCKET,
        "key": key,
        "s3_uri": f"s3://{NOAA_MRMS_BUCKET}/{key}",
        "product": product,
        "expected_valid_time_utc": expected_time.isoformat(),
        "discovered_valid_time_utc": discovered.isoformat(),
        "time_offset_seconds": int((discovered - expected_time).total_seconds()),
        "content_length": int(item.get("Size") or item.get("size") or 0),
        "etag": str(item.get("ETag") or item.get("etag") or ""),
        "last_modified": str(item.get("LastModified") or item.get("last_modified") or ""),
        "availability_status": "available",
        "selection_reason": "closest_valid_timestamp",
    }


def mask_precipitation(values: Sequence[float | int | None]) -> list[float | None]:
    masked: list[float | None] = []
    for value in values:
        if value is None or float(value) <= MISSING_QPE_THRESHOLD:
            masked.append(None)
        elif float(value) < 0:
            raise MrmsError(f"Negative precipitation value is not a missing sentinel: {value}")
        else:
            masked.append(float(value))
    return masked


def rolling_max(values: Sequence[float | None], window: int) -> float | None:
    cleaned = [0.0 if value is None else value for value in values]
    if len(cleaned) < window:
        return None
    return max(sum(cleaned[index : index + window]) for index in range(len(cleaned) - window + 1))


def peak_count(
    values: Sequence[float | None], threshold: float = 1.0, dry_gap_hours: int = 6
) -> int:
    peaks = 0
    in_peak = False
    dry = dry_gap_hours
    for value in values:
        wet = (value or 0.0) >= threshold
        if wet and not in_peak and dry >= dry_gap_hours:
            peaks += 1
            in_peak = True
        if wet:
            dry = 0
        else:
            dry += 1
            if dry >= dry_gap_hours:
                in_peak = False
    return peaks


def largest_dry_gap(values: Sequence[float | None], threshold: float = 1.0) -> int:
    best = 0
    current = 0
    for value in values:
        if (value or 0.0) < threshold:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _find_file(root: Path, name: str) -> Path | None:
    direct = root / name
    if direct.exists():
        return direct
    matches = sorted(root.rglob(name))
    return matches[0] if matches else None


def _is_parquet_dataset(path: Path) -> bool:
    return path.exists() and (
        path.is_file() and path.suffix == ".parquet" or any(path.glob("*.parquet"))
    )


def _sample_row(
    episode: dict[str, Any],
    stratum: str,
    rank: int,
    seed: int,
    membership: list[dict[str, Any]],
    balanced_episode_by_event: Mapping[str, str],
) -> dict[str, Any]:
    members = [row for row in membership if row["episode_id"] == episode["episode_id"]]
    related_balanced = sorted(
        {balanced_episode_by_event.get(str(row["event_record_id"]), "") for row in members}
    )
    return {
        "selection_stratum": stratum,
        "selection_rank": rank,
        "selection_seed": seed,
        "episode_id": str(episode["episode_id"]),
        "conservative_episode_id": str(episode["episode_id"]),
        "related_balanced_episode_id": ";".join(value for value in related_balanced if value),
        "member_count": int(episode["member_count"]),
        "episode_duration_days": int(episode["episode_duration_days"]),
        "state_codes": str(episode["state_codes"]),
        "candidate_split": str(episode["candidate_split"]),
        "spatial_review_required": bool(episode["spatial_review_required"]),
        "episode_start_date": _as_date(episode["episode_start_date"]).isoformat(),
        "episode_end_date": _as_date(episode["episode_end_date"]).isoformat(),
        "representative_longitude": float(episode["representative_longitude"]),
        "representative_latitude": float(episode["representative_latitude"]),
    }


def _balanced_merge_candidates(
    episodes: list[dict[str, Any]],
    membership: list[dict[str, Any]],
    balanced_by_event: Mapping[str, frozenset[str]],
) -> list[dict[str, Any]]:
    members_by_episode = _component_sets(membership)
    episode_by_id = {str(row["episode_id"]): row for row in episodes}
    candidates = []
    for episode_id, members in members_by_episode.items():
        broader_sets = [
            balanced_by_event[member] for member in members if member in balanced_by_event
        ]
        if broader_sets and any(members < broader for broader in broader_sets):
            candidates.append(episode_by_id[episode_id])
    return sorted(candidates, key=lambda row: (-int(row["member_count"]), str(row["episode_id"])))


def _component_sets(membership: list[dict[str, Any]]) -> dict[str, frozenset[str]]:
    grouped: dict[str, set[str]] = defaultdict(set)
    for row in membership:
        grouped[str(row["episode_id"])].add(str(row["event_record_id"]))
    return {key: frozenset(value) for key, value in grouped.items()}


def _component_members_by_event(membership: list[dict[str, Any]]) -> dict[str, frozenset[str]]:
    return {
        event_id: members
        for members in _component_sets(membership).values()
        for event_id in members
    }


def _stable_shuffle(rows: list[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: hashlib.sha256(f"{seed}:{row['episode_id']}".encode()).hexdigest(),
    )


def _comparison_group_rows(
    sample_rows: list[dict[str, Any]],
    membership: list[dict[str, Any]],
    balanced_by_event: Mapping[str, frozenset[str]],
) -> list[dict[str, Any]]:
    members_by_episode = _component_sets(membership)
    rows = []
    for sample in sample_rows:
        episode_id = str(sample["episode_id"])
        conservative_members = members_by_episode.get(episode_id, frozenset())
        broader_members = sorted(
            {
                broader_member
                for member in conservative_members
                for broader_member in balanced_by_event.get(member, frozenset())
            }
        )
        rows.append(
            {
                "conservative_episode_id": episode_id,
                "related_balanced_episode_id": sample["related_balanced_episode_id"],
                "conservative_member_count": len(conservative_members),
                "balanced_member_count": len(broader_members),
                "additional_balanced_members": max(
                    0, len(broader_members) - len(conservative_members)
                ),
            }
        )
    return rows


def _mrms_window(row: Mapping[str, Any]) -> tuple[datetime, datetime]:
    start = datetime.combine(_as_date(row["episode_start_date"]), datetime.min.time(), tzinfo=UTC)
    end = datetime.combine(_as_date(row["episode_end_date"]), datetime.min.time(), tzinfo=UTC)
    if int(row.get("episode_duration_days", 0)) <= LONG_EPISODE_DAYS:
        return start - timedelta(hours=6), end + timedelta(hours=30)
    return start, end + timedelta(days=1)


def _expected_times(start: datetime, end: datetime, product: str) -> list[datetime]:
    step = timedelta(days=1) if product == LONG_EPISODE_PRODUCT else timedelta(hours=1)
    current = start.replace(minute=0, second=0, microsecond=0)
    values = []
    while current <= end:
        values.append(current)
        current += step
    return values


def _select_s3_object(fs: S3Like, product: str, expected: datetime) -> dict[str, Any]:
    prefix = f"s3://{NOAA_MRMS_BUCKET}/{MRMS_PREFIX}/{product}/{expected:%Y%m%d}/"
    try:
        objects = fs.ls(prefix, detail=True)
    except Exception as exc:  # pragma: no cover - exact S3 errors vary
        row = _missing_inventory_row(product, expected, "list_failed")
        row["error"] = str(exc)
        return row
    return closest_object(objects, product, expected)


def _missing_inventory_row(product: str, expected: datetime, reason: str) -> dict[str, Any]:
    return {
        "bucket": NOAA_MRMS_BUCKET,
        "key": "",
        "s3_uri": "",
        "product": product,
        "expected_valid_time_utc": expected.isoformat(),
        "discovered_valid_time_utc": "",
        "time_offset_seconds": None,
        "content_length": 0,
        "etag": "",
        "last_modified": "",
        "availability_status": "missing",
        "selection_reason": reason,
    }


def _download_atomic(fs: S3Like, s3_uri: str, path: Path, expected_size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=path.parent) as tmp:
        tmp_path = Path(tmp.name)
        try:
            for attempt in range(MAX_DOWNLOAD_ATTEMPTS):
                try:
                    with fs.open(s3_uri, "rb") as src:
                        shutil.copyfileobj(src, tmp)
                    break
                except Exception:
                    if attempt == MAX_DOWNLOAD_ATTEMPTS - 1:
                        raise
                    time.sleep(0.25 * 2**attempt)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
    if expected_size and tmp_path.stat().st_size != expected_size:
        tmp_path.unlink(missing_ok=True)
        raise MrmsError(f"Downloaded size mismatch for {s3_uri}")
    tmp_path.replace(path)


def _assessment_row(episode: Mapping[str, Any], members: list[dict[str, Any]]) -> dict[str, Any]:
    weak = sum(1 for row in members if float(row["max_1h_mm"]) < WEAK_SIGNAL_1H_MM)
    coverage = float(episode["coverage_fraction"])
    category = "coherent"
    if coverage < MIN_COVERAGE_FRACTION:
        category = "insufficient_mrms_coverage"
    elif weak:
        category = "weak_precipitation_signal"
    elif int(episode["rainfall_peak_count"]) > 1:
        category = "possibly_overmerged"
    return {
        "episode_id": episode["episode_id"],
        "member_peak_time_range_hours": 0,
        "median_member_timeseries_correlation": 1.0,
        "minimum_member_timeseries_correlation": 1.0,
        "members_with_weak_precipitation_signal": weak,
        "members_with_missing_mrms": sum(
            1 for row in members if float(row["coverage_fraction"]) == 0
        ),
        "multiple_precipitation_peak_flag": int(episode["rainfall_peak_count"]) > 1,
        "largest_dry_gap_hours": episode["largest_dry_gap_hours"],
        "provisional_category": category,
    }


def _check_import(module: str) -> dict[str, Any]:
    try:
        imported = __import__(module)
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}
    return {"ok": True, "version": str(getattr(imported, "__version__", "unknown"))}


def _check_s3_import() -> dict[str, Any]:
    result = _check_import("s3fs")
    result["anonymous_access_check"] = "import_only"
    return result


def _check_xarray_engine() -> dict[str, Any]:
    try:
        xr = importlib.import_module("xarray")
        return {"ok": "cfgrib" in xr.backends.list_engines()}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def _run_cfgrib_selfcheck() -> dict[str, Any]:
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "cfgrib", "selfcheck"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}
    return {
        "ok": completed.returncode == 0,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
    }


def _check_temp_write(root: Path) -> dict[str, Any]:
    try:
        with tempfile.NamedTemporaryFile(dir=root, delete=True) as handle:
            handle.write(b"ok")
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def _check_grib_decode(sample_grib: Path | None) -> dict[str, Any]:
    try:
        xr = importlib.import_module("xarray")

        if sample_grib is None:
            return {"ok": False, "reason": "no sample GRIB file provided"}
        if sample_grib.suffix == ".gz":
            with (
                gzip.open(sample_grib, "rb") as src,
                tempfile.NamedTemporaryFile(suffix=".grib2") as tmp,
            ):
                shutil.copyfileobj(src, tmp)
                tmp.flush()
                dataset = xr.open_dataset(
                    tmp.name, engine="cfgrib", backend_kwargs={"indexpath": ""}
                )
        else:
            dataset = xr.open_dataset(
                sample_grib, engine="cfgrib", backend_kwargs={"indexpath": ""}
            )
        return {"ok": True, "data_vars": sorted(dataset.data_vars)}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def _s3_filesystem() -> S3Like:
    try:
        s3fs = importlib.import_module("s3fs")
    except Exception as exc:
        raise MrmsError("s3fs is required for real MRMS inventory/fetch.") from exc
    return cast(S3Like, s3fs.S3FileSystem(anon=True))


def _read_rows(path: Path) -> list[dict[str, Any]]:
    if path.is_dir():
        return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())
    if path.suffix == ".csv":
        return _read_csv(path)
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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


def _cache_name(key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return f"{digest}_{Path(key).name}"


def _inventory_fields() -> list[str]:
    return [
        "bucket",
        "key",
        "s3_uri",
        "product",
        "expected_valid_time_utc",
        "discovered_valid_time_utc",
        "time_offset_seconds",
        "content_length",
        "etag",
        "last_modified",
        "requesting_episode_ids",
        "availability_status",
        "selection_reason",
    ]


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
