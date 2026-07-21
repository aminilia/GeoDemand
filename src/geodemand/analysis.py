from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand import __version__
from geodemand.trends import read_request_plan_rows

ANALYSIS_DATASET_VERSION = "1.0A-v1"
AnalysisGrain = tuple[str, str, str]
PlanGrain = tuple[str, str]
AggregateGrain = tuple[str, str]
JoinStatus = Literal["matched", "unmatched"]
KeyT = TypeVar("KeyT", bound=tuple[str, ...])

PLAN_FIELDS = {
    "request_id",
    "episode_id",
    "geography",
    "batch_id",
    "terminology_version",
}
EPISODE_METRIC_FIELDS = {"request_id", "concept_id"}
REPEAT_METRIC_FIELDS = {"request_id", "repeat_id", "concept_id"}
PHASE_METRIC_FIELDS = {"request_id", "repeat_id", "concept_id"}


class AnalysisError(ValueError):
    """Raised when an analytical dataset cannot be built deterministically."""


def build_analysis_dataset(
    *,
    request_plan_path: Path,
    episode_metrics_path: Path,
    repeat_metrics_path: Path,
    phase_metrics_path: Path,
    output_root: Path,
) -> dict[str, Path]:
    """Build the deterministic Trends event-study analytical dataset."""
    plan_rows = read_request_plan_rows(request_plan_path, required_fields=PLAN_FIELDS)
    episode_rows = _read_parquet_rows(episode_metrics_path, EPISODE_METRIC_FIELDS)
    repeat_rows = _read_parquet_rows(repeat_metrics_path, REPEAT_METRIC_FIELDS)
    phase_rows = _read_parquet_rows(phase_metrics_path, PHASE_METRIC_FIELDS)

    plan_index = _plan_index(plan_rows, request_plan_path)
    episode_index = _unique_index(
        episode_rows,
        episode_metrics_path,
        key_name="episode_response_metric_key",
        key_func=_aggregate_key,
    )
    repeat_index = _unique_index(
        repeat_rows,
        repeat_metrics_path,
        key_name="repeat_response_metric_key",
        key_func=_analysis_key,
    )
    phase_index = _unique_index(
        phase_rows,
        phase_metrics_path,
        key_name="phase_response_metric_key",
        key_func=_analysis_key,
    )

    integrated: list[dict[str, Any]] = []
    join_rows: list[dict[str, Any]] = []
    for key, phase in sorted(phase_index.items()):
        request_id, repeat_id, concept_id = key
        plan = plan_index.get((request_id, repeat_id)) or plan_index.get((request_id, ""))
        repeat = repeat_index.get(key)
        episode = episode_index.get((request_id, concept_id))
        statuses = {
            "plan_join_status": _join_status(plan),
            "repeat_metrics_join_status": _join_status(repeat),
            "episode_metrics_join_status": _join_status(episode),
        }
        join_rows.append(
            {
                "request_id": request_id,
                "repeat_id": repeat_id,
                "concept_id": concept_id,
                **statuses,
            }
        )
        integrated.append(
            _integrated_row(
                key,
                plan=plan,
                episode=episode,
                repeat=repeat,
                phase=phase,
                statuses=statuses,
            )
        )

    output_root.mkdir(parents=True, exist_ok=True)
    dataset_path = output_root / "analysis_dataset.parquet"
    join_summary_path = output_root / "join_summary.json"
    unmatched_path = output_root / "unmatched_keys.csv"
    provenance_path = output_root / "provenance.json"
    _write_parquet(dataset_path, integrated)
    _write_csv(unmatched_path, [row for row in join_rows if "unmatched" in set(row.values())])
    join_summary = _join_summary(
        integrated,
        join_rows,
        input_counts={
            "request_plan_rows": len(plan_rows),
            "episode_response_metric_rows": len(episode_rows),
            "repeat_response_metric_rows": len(repeat_rows),
            "phase_response_metric_rows": len(phase_rows),
        },
    )
    _write_json(join_summary_path, join_summary)
    _write_json(
        provenance_path,
        {
            "analysis_dataset_version": ANALYSIS_DATASET_VERSION,
            "package_version": __version__,
            "build_timestamp_utc": datetime.now(UTC).isoformat(),
            "analytical_row_grain": ["request_id", "repeat_id", "concept_id"],
            "join_cardinalities": {
                "phase_response_metrics": "base_unique_by_request_repeat_concept",
                "repeat_response_metrics": "one_to_one_by_request_repeat_concept",
                "episode_response_metrics": "many_to_one_by_request_concept",
                "request_plan": "many_to_one_by_request_repeat_or_request",
            },
            "inputs": {
                "request_plan": _file_provenance(request_plan_path),
                "episode_response_metrics": _file_provenance(episode_metrics_path),
                "repeat_response_metrics": _file_provenance(repeat_metrics_path),
                "phase_response_metrics": _file_provenance(phase_metrics_path),
            },
            "outputs": {
                "analysis_dataset": _file_provenance(dataset_path),
                "join_summary": _file_provenance(join_summary_path),
                "unmatched_keys": _file_provenance(unmatched_path),
            },
            "row_counts": join_summary["row_counts"],
        },
    )
    return {
        "dataset": dataset_path,
        "join_summary": join_summary_path,
        "unmatched_keys": unmatched_path,
        "provenance": provenance_path,
    }


def _integrated_row(
    key: AnalysisGrain,
    *,
    plan: Mapping[str, Any] | None,
    episode: Mapping[str, Any] | None,
    repeat: Mapping[str, Any] | None,
    phase: Mapping[str, Any],
    statuses: Mapping[str, JoinStatus],
) -> dict[str, Any]:
    request_id, repeat_id, concept_id = key
    episode_source = plan or phase or episode or repeat or {}
    return {
        "schema_version": ANALYSIS_DATASET_VERSION,
        "request_id": request_id,
        "repeat_id": repeat_id,
        "concept_id": concept_id,
        "episode_id": _first_value("episode_id", plan, phase, episode, repeat),
        "geography": _first_value("geography", plan, phase, episode, repeat),
        "geography_level": _first_value("geography_level", plan, phase, episode, repeat),
        "batch_id": _first_value("batch_id", plan, phase, episode, repeat),
        "comparison_batch_id": _first_value("comparison_batch_id", plan, phase, episode, repeat),
        "request_role": _first_value("request_role", plan, phase, episode, repeat),
        "terminology_version": _first_value("terminology_version", plan, phase, episode, repeat),
        "planned_repeat_id": plan.get("planned_repeat_id") if plan else None,
        "expected_output_filename": plan.get("expected_output_filename") if plan else None,
        "event_start_date": _first_value("event_start_date", plan, phase, episode, repeat),
        "event_end_date": _first_value("event_end_date", plan, phase, episode, repeat),
        "baseline_start_date": _first_value("baseline_start_date", plan, phase, episode, repeat),
        "baseline_end_date": _first_value("baseline_end_date", plan, phase, episode, repeat),
        "post_start_date": _first_value("post_start_date", plan, phase, episode, repeat),
        "post_end_date": _first_value("post_end_date", plan, phase, episode, repeat),
        "metric_quality_status": _first_value("metric_quality_status", phase, repeat, episode),
        "metric_quality_reasons": _first_value("metric_quality_reasons", phase, repeat, episode),
        "repeat_count": _first_value("repeat_count", phase, episode),
        "repeat_ids": episode.get("repeat_ids") if episode else None,
        "repeat_stability_flag": _first_value("repeat_stability_flag", phase, repeat, episode),
        "repeat_stability_status": phase.get("repeat_stability_status"),
        "maximum_repeat_phase_lift_stddev": phase.get("maximum_repeat_phase_lift_stddev"),
        "baseline_valid_day_count": _first_value(
            "baseline_valid_day_count", phase, repeat, episode
        ),
        "event_valid_day_count": _first_value("event_valid_day_count", repeat, episode),
        "post_valid_day_count": _first_value("post_valid_day_count", repeat, episode),
        "baseline_mean": _first_value("baseline_mean", phase, repeat, episode),
        "baseline_median": _first_value("baseline_median", phase, repeat, episode),
        "baseline_standard_deviation": _first_value(
            "baseline_standard_deviation", phase, repeat, episode
        ),
        "baseline_mad": _first_value("baseline_mad", phase, repeat, episode),
        "event_maximum": _first_value("event_maximum", repeat, episode),
        "post_maximum": _first_value("post_maximum", repeat, episode),
        "absolute_peak_lift": _first_value("absolute_peak_lift", repeat, episode),
        "ratio_peak_lift": _first_value("ratio_peak_lift", repeat, episode),
        "z_score_peak_lift": _first_value("z_score_peak_lift", repeat, episode),
        "robust_peak_lift": _first_value("robust_peak_lift", repeat, episode),
        "zero_fraction": _first_value("zero_fraction", phase, repeat, episode),
        "suppression_flag": _first_value("suppression_flag", phase, repeat, episode),
        "peak_date": _first_value("peak_date", phase, repeat, episode),
        "peak_phase": phase.get("peak_phase"),
        "global_peak_date": phase.get("global_peak_date"),
        "global_peak_phase": phase.get("global_peak_phase"),
        "global_peak_value": phase.get("global_peak_value"),
        "dominant_event_response_phase": phase.get("dominant_event_response_phase"),
        "dominant_standardized_phase_lift": phase.get("dominant_standardized_phase_lift"),
        "dominant_robust_phase_lift": phase.get("dominant_robust_phase_lift"),
        **_phase_values(phase),
        **statuses,
        "analytical_row_grain": "request_id;repeat_id;concept_id",
        "source_episode_id": episode_source.get("episode_id"),
    }


def _phase_values(row: Mapping[str, Any]) -> dict[str, Any]:
    phases = ("anticipatory", "immediate", "early_recovery", "extended_recovery")
    suffixes = ("mean", "maximum", "peak_lift", "standardized_lift", "robust_lift")
    return {
        f"{phase}_{suffix}": row.get(f"{phase}_{suffix}") for phase in phases for suffix in suffixes
    }


def _plan_index(
    rows: Sequence[Mapping[str, Any]], path: Path
) -> dict[PlanGrain, Mapping[str, Any]]:
    keys = [(_plan_key(row), row) for row in rows]
    duplicates = _duplicates([key for key, _ in keys])
    if duplicates:
        raise AnalysisError(
            "duplicate_normalized_keys: "
            f"key_name=request_plan_key duplicates={_stringify_keys(duplicates)} path={path}"
        )
    return dict(keys)


def _unique_index(
    rows: Sequence[Mapping[str, Any]],
    path: Path,
    *,
    key_name: str,
    key_func: Callable[[Mapping[str, Any]], KeyT],
) -> dict[KeyT, Mapping[str, Any]]:
    keys = [(key_func(row), row) for row in rows]
    duplicates = _duplicates([key for key, _ in keys])
    if duplicates:
        raise AnalysisError(
            "duplicate_normalized_keys: "
            f"key_name={key_name} duplicates={_stringify_keys(duplicates)} path={path}"
        )
    return dict(keys)


def _plan_key(row: Mapping[str, Any]) -> PlanGrain:
    repeat = str(row.get("planned_repeat_id") or row.get("repeat_id") or "")
    return _normalized(row, "request_id"), repeat.strip()


def _analysis_key(row: Mapping[str, Any]) -> AnalysisGrain:
    return (
        _normalized(row, "request_id"),
        _normalized(row, "repeat_id"),
        _normalized(row, "concept_id"),
    )


def _aggregate_key(row: Mapping[str, Any]) -> AggregateGrain:
    return _normalized(row, "request_id"), _normalized(row, "concept_id")


def _normalized(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    return "" if value is None else str(value).strip()


def _duplicates(keys: Sequence[tuple[str, ...]]) -> list[tuple[str, ...]]:
    counts = Counter(keys)
    return sorted(key for key, count in counts.items() if count > 1)


def _stringify_keys(keys: Sequence[tuple[str, ...]]) -> list[str]:
    return ["|".join(key) for key in keys[:20]]


def _read_parquet_rows(path: Path, required_fields: set[str]) -> list[dict[str, Any]]:
    if not path.exists():
        raise AnalysisError(f"Input Parquet is missing: {path}")
    table = pq.read_table(path)
    missing = sorted(required_fields - set(table.column_names))
    if missing:
        raise AnalysisError(
            "analysis_input_missing_fields: "
            f"missing={missing} available={list(table.column_names)} path={path}"
        )
    return cast(list[dict[str, Any]], table.to_pylist())


def _join_status(value: object | None) -> JoinStatus:
    return "matched" if value is not None else "unmatched"


def _first_value(field: str, *rows: Mapping[str, Any] | None) -> Any:
    for row in rows:
        if row is not None and row.get(field) not in {None, ""}:
            return row[field]
    return None


def _join_summary(
    rows: Sequence[Mapping[str, Any]],
    join_rows: Sequence[Mapping[str, Any]],
    *,
    input_counts: Mapping[str, int],
) -> dict[str, Any]:
    status_fields = (
        "plan_join_status",
        "repeat_metrics_join_status",
        "episode_metrics_join_status",
    )
    unmatched = {
        field: sum(row[field] == "unmatched" for row in join_rows) for field in status_fields
    }
    return {
        "analysis_dataset_version": ANALYSIS_DATASET_VERSION,
        "analytical_row_grain": ["request_id", "repeat_id", "concept_id"],
        "row_counts": {
            **dict(input_counts),
            "analysis_dataset_rows": len(rows),
            "unmatched_key_rows": sum(
                any(row[field] == "unmatched" for field in status_fields) for row in join_rows
            ),
        },
        "join_cardinalities": {
            "request_plan": "many_to_one_by_request_repeat_or_request",
            "episode_response_metrics": "many_to_one_by_request_concept",
            "repeat_response_metrics": "one_to_one_by_request_repeat_concept",
            "phase_response_metrics": "base_unique_by_request_repeat_concept",
        },
        "unmatched_counts": unmatched,
        "all_required_joins_matched": all(value == 0 for value in unmatched.values()),
    }


def _file_provenance(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "file_name": path.name,
        "byte_count": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    ordered = _deterministic_rows(rows)
    table = pa.Table.from_pylist(ordered)
    pq.write_table(table, path, compression="zstd")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        if fields:
            writer.writeheader()
            writer.writerows(_deterministic_rows(rows))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _deterministic_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in rows],
        key=lambda row: (
            str(row.get("request_id", "")),
            str(row.get("repeat_id", "")),
            str(row.get("concept_id", "")),
            json.dumps(row, sort_keys=True, default=str),
        ),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
