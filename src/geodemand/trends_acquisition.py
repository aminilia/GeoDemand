from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand import __version__
from geodemand.trends import TrendsError

ACQUISITION_BACKENDS = ("manual_csv", "pytrends")
MANUAL_STORE = "raw"
PYTRENDS_STORE = "raws"
MIN_PAIR_COUNT = 2


def inventory_acquisition_stores(
    data_root: Path,
    output_root: Path,
) -> dict[str, Path]:
    raw_store = data_root / MANUAL_STORE
    pytrends_store = data_root / PYTRENDS_STORE
    if not raw_store.exists():
        raise TrendsError(f"Acquisition store missing: {raw_store}")
    if not pytrends_store.exists():
        raise TrendsError(f"Acquisition store missing: {pytrends_store}")

    output_root.mkdir(parents=True, exist_ok=True)
    inventories = {
        "manual_store": _inventory_manual_store(raw_store),
        "pytrends_store": _inventory_pytrends_store(pytrends_store),
    }
    for name, payload in inventories.items():
        _write_json(output_root / f"{name}.json", payload)
        _write_parquet(output_root / f"{name}.parquet", payload["files"])
    _write_json(
        output_root / "acquisition_inventory_summary.json",
        {
            "package_version": __version__,
            "stores": {
                name: {
                    "file_count": len(payload["files"]),
                    "request_count": payload["request_count"],
                    "sha256": payload["inventory_sha256"],
                }
                for name, payload in inventories.items()
            },
        },
    )
    return {name: output_root / f"{name}.json" for name in inventories}


def compare_acquisition_reproducibility(
    manual_root: Path,
    pytrends_root: Path,
    output_root: Path,
    plan_path: Path | None = None,
) -> dict[str, Path]:
    manual_files = _load_store_csvs(manual_root)
    pytrends_files = _load_store_csvs(pytrends_root)
    shared = sorted(set(manual_files) & set(pytrends_files))
    if not shared:
        raise TrendsError("No shared request_ids were found between acquisition stores.")
    output_root.mkdir(parents=True, exist_ok=True)
    metrics_rows: list[dict[str, Any]] = []
    diagnostics_rows: list[dict[str, Any]] = []
    for request_id in shared:
        manual_rows = _read_trends_rows(manual_files[request_id], "manual_csv")
        pytrends_rows = _read_trends_rows(pytrends_files[request_id], "pytrends")
        manual_by_concept = _group_series(manual_rows)
        py_by_concept = _group_series(pytrends_rows)
        for concept_id in sorted(set(manual_by_concept) & set(py_by_concept)):
            left = manual_by_concept[concept_id]
            right = py_by_concept[concept_id]
            overlap = sorted(set(left) & set(right))
            if len(overlap) < MIN_PAIR_COUNT:
                continue
            left_values = [left[day] for day in overlap]
            right_values = [right[day] for day in overlap]
            metrics_rows.append(
                {
                    "request_id": request_id,
                    "concept_id": concept_id,
                    "shared_day_count": len(overlap),
                    "pearson_r": _pearson(left_values, right_values),
                    "spearman_r": _spearman(left_values, right_values),
                    "mae": _mae(left_values, right_values),
                    "rmse": _rmse(left_values, right_values),
                    "zero_agreement": _zero_agreement(left_values, right_values),
                    "peak_value_agreement": _peak_value_agreement(left_values, right_values),
                    "peak_date_agreement": _peak_date_agreement(left, right),
                    "peak_date_displacement_days": _peak_date_displacement(left, right),
                    "suppression_agreement": _suppression_agreement(left, right),
                }
            )
        diagnostics_rows.append(
            {
                "request_id": request_id,
                "manual_concepts": len(manual_by_concept),
                "pytrends_concepts": len(py_by_concept),
                "shared_concepts": len(set(manual_by_concept) & set(py_by_concept)),
                "manual_rows": len(manual_rows),
                "pytrends_rows": len(pytrends_rows),
            }
        )
    metrics_rows = sorted(metrics_rows, key=lambda row: (row["request_id"], row["concept_id"]))
    diagnostics_rows = sorted(diagnostics_rows, key=lambda row: row["request_id"])
    _write_parquet(output_root / "acquisition_reproducibility.parquet", metrics_rows)
    _write_csv(output_root / "acquisition_reproducibility.csv", metrics_rows)
    _write_csv(output_root / "acquisition_panel_diagnostics.csv", diagnostics_rows)
    summary = {
        "package_version": __version__,
        "paired_request_count": len(shared),
        "metric_row_count": len(metrics_rows),
        "diagnostic_row_count": len(diagnostics_rows),
        "plan_path": str(plan_path) if plan_path else None,
        "validation_status": "unvalidated_legacy_comparison_not_for_1_1",
    }
    _write_json(output_root / "acquisition_reproducibility_summary.json", summary)
    return {
        "reproducibility_parquet": output_root / "acquisition_reproducibility.parquet",
        "reproducibility_csv": output_root / "acquisition_reproducibility.csv",
        "panel_diagnostics": output_root / "acquisition_panel_diagnostics.csv",
        "summary": output_root / "acquisition_reproducibility_summary.json",
    }


def export_pytrends(
    plan_path: Path,
    output_root: Path,
    *,
    pytrends_client: Any | None = None,
    request_ids: set[str] | None = None,
) -> dict[str, Path]:
    raise TrendsError(
        "PyTrends export is disabled for 1.1: query mapping and immutable repeat provenance "
        "are not validated. Use official/manual exports for the primary analysis."
    )


def _inventory_manual_store(raw_store: Path) -> dict[str, Any]:
    files = []
    for csv_path in sorted(raw_store.glob("*.csv")):
        sidecar_json = raw_store / f"{csv_path.stem}.json"
        sidecar = (
            json.loads(sidecar_json.read_text(encoding="utf-8")) if sidecar_json.exists() else {}
        )
        files.append(
            {
                "acquisition_backend": str(sidecar.get("backend", "manual_csv")),
                "request_id": csv_path.stem,
                "repeat_id": str(sidecar.get("repeat_id") or ""),
                "source_file": csv_path.name,
                "sidecar_file": sidecar_json.name if sidecar_json.exists() else None,
                "sha256": _sha256(csv_path),
                "size_bytes": csv_path.stat().st_size,
                "retrieval_timestamp": str(
                    sidecar.get("export_timestamp_utc") or sidecar.get("export_date") or ""
                ),
            }
        )
    return _inventory_payload(raw_store, files)


def _inventory_pytrends_store(pytrends_store: Path) -> dict[str, Any]:
    files = []
    for csv_path in sorted(pytrends_store.glob("*.csv")):
        if csv_path.stem.startswith(("trends_request_plan", "trends_run_log")):
            continue
        files.append(
            {
                "acquisition_backend": "pytrends",
                "request_id": csv_path.stem,
                "repeat_id": "",
                "source_file": csv_path.name,
                "sidecar_file": None,
                "sha256": _sha256(csv_path),
                "size_bytes": csv_path.stat().st_size,
                "retrieval_timestamp": None,
                "retrieval_timestamp_status": "unknown_no_acquisition_metadata",
            }
        )
    return _inventory_payload(pytrends_store, files)


def _inventory_payload(root: Path, files: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "package_version": __version__,
        "root": str(root),
        "file_count": len(files),
        "request_count": len({row["request_id"] for row in files if row.get("request_id")}),
        "files": files,
    }
    payload["inventory_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return payload


def _load_store_csvs(store: Path) -> dict[str, Path]:
    return {path.stem: path for path in sorted(store.glob("*.csv"))}


def _read_trends_rows(path: Path, backend: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames or []
        if "isPartial" not in headers and backend == "pytrends":
            raise TrendsError(f"PyTrends export missing isPartial: {path}")
        for raw in reader:
            date_text = str(raw.get("date") or raw.get("Day") or "").strip()
            for concept, value in raw.items():
                if concept in {"date", "Day", "isPartial"}:
                    continue
                if value in {"", None}:
                    continue
                rows.append(
                    {
                        "date": date_text,
                        "concept_id": concept,
                        "value": float(value),
                        "is_partial": str(raw.get("isPartial") or "").casefold() == "true",
                    }
                )
    return rows


def _group_series(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, float]]:
    grouped: dict[str, dict[str, float]] = defaultdict(dict)
    for row in rows:
        grouped[str(row["concept_id"])][str(row["date"])] = float(row["value"])
    return grouped


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) < MIN_PAIR_COUNT:
        return None
    mean_left = statistics.fmean(left)
    mean_right = statistics.fmean(right)
    numerator = sum((x - mean_left) * (y - mean_right) for x, y in zip(left, right, strict=True))
    denom_left = math.sqrt(sum((x - mean_left) ** 2 for x in left))
    denom_right = math.sqrt(sum((y - mean_right) ** 2 for y in right))
    return numerator / (denom_left * denom_right) if denom_left and denom_right else None


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    return _pearson(_rank(left), _rank(right))


def _rank(values: Sequence[float]) -> list[float]:
    ordered = sorted((value, index) for index, value in enumerate(values))
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        j = index
        while j < len(ordered) and ordered[j][0] == ordered[index][0]:
            j += 1
        rank = (index + j - 1) / 2 + 1
        for _, original in ordered[index:j]:
            ranks[original] = rank
        index = j
    return ranks


def _mae(left: Sequence[float], right: Sequence[float]) -> float:
    return statistics.fmean(abs(x - y) for x, y in zip(left, right, strict=True))


def _rmse(left: Sequence[float], right: Sequence[float]) -> float:
    return math.sqrt(statistics.fmean((x - y) ** 2 for x, y in zip(left, right, strict=True)))


def _zero_agreement(left: Sequence[float], right: Sequence[float]) -> float:
    matches = sum((x == 0) == (y == 0) for x, y in zip(left, right, strict=True))
    return matches / len(left)


def _peak_value_agreement(left: Sequence[float], right: Sequence[float]) -> bool:
    return max(left) == max(right)


def _peak_date_agreement(left: Mapping[str, float], right: Mapping[str, float]) -> bool:
    return _peak_date(left) == _peak_date(right)


def _peak_date_displacement(left: Mapping[str, float], right: Mapping[str, float]) -> int | None:
    left_peak = _peak_date(left)
    right_peak = _peak_date(right)
    if left_peak is None or right_peak is None:
        return None
    return abs((_as_date(left_peak) - _as_date(right_peak)).days)


def _suppression_agreement(left: Mapping[str, float], right: Mapping[str, float]) -> bool:
    overlap = sorted(set(left) & set(right))
    return all((left[day] == 0) == (right[day] == 0) for day in overlap)


def _peak_date(series: Mapping[str, float]) -> str | None:
    if not series:
        return None
    peak = max(series.values())
    return min(day for day, value in series.items() if value == peak)


def _as_date(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _write_csv(
    path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str] | None = None
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(fields or (list(rows[0]) if rows else []))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([dict(row) for row in rows]) if rows else pa.table({})
    pq.write_table(table, path, compression="zstd")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
