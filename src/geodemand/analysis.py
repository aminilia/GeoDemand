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
from geodemand.trends import TrendsError, read_request_plan_rows

ANALYSIS_DATASET_VERSION = "1.0A-v1"
AnalysisGrain = tuple[str, str, str]
PlanGrain = tuple[str, str]
AggregateGrain = tuple[str, str]
JoinStatus = Literal["matched", "unmatched"]
KeyT = TypeVar("KeyT", bound=tuple[str, ...])

WINDOW_FIELDS = {
    "event_start_date",
    "event_end_date",
    "baseline_start_date",
    "baseline_end_date",
    "post_start_date",
    "post_end_date",
}
LINEAGE_FIELDS = {
    "episode_id",
    "geography",
    "geography_level",
    "batch_id",
    "comparison_batch_id",
    "request_role",
    "terminology_version",
    *WINDOW_FIELDS,
}
PLAN_FIELDS = {
    "request_id",
    "concept_ids",
    *LINEAGE_FIELDS,
}
SCIENTIFIC_METRIC_FIELDS = {
    "baseline_valid_day_count",
    "event_valid_day_count",
    "post_valid_day_count",
    "baseline_mean",
    "baseline_median",
    "baseline_standard_deviation",
    "baseline_mad",
    "event_maximum",
    "post_maximum",
    "absolute_peak_lift",
    "ratio_peak_lift",
    "z_score_peak_lift",
    "robust_peak_lift",
    "zero_fraction",
    "suppression_flag",
    "metric_quality_status",
    "metric_quality_reasons",
    "repeat_stability_flag",
    "peak_date",
}
EPISODE_METRIC_FIELDS = {
    "request_id",
    "concept_id",
    "repeat_count",
    "repeat_ids",
    *LINEAGE_FIELDS,
    *SCIENTIFIC_METRIC_FIELDS,
}
REPEAT_METRIC_FIELDS = {
    "request_id",
    "repeat_id",
    "concept_id",
    "export_attempt_id",
    *LINEAGE_FIELDS,
    *SCIENTIFIC_METRIC_FIELDS,
}
PHASE_NAMES = ("anticipatory", "immediate", "early_recovery", "extended_recovery")
PHASE_METRIC_FIELDS = {
    "request_id",
    "repeat_id",
    "concept_id",
    "export_attempt_id",
    "treated_geography",
    "semantic_family",
    "proxy_type",
    "standardized_peak_lift",
    "peak_lead_lag_days",
    "dominant_event_response_phase",
    "dominant_standardized_phase_lift",
    "dominant_robust_phase_lift",
    "peak_phase",
    "global_peak_date",
    "global_peak_phase",
    "global_peak_value",
    "baseline_valid_day_count",
    "baseline_mean",
    "baseline_median",
    "baseline_standard_deviation",
    "baseline_mad",
    "metric_quality_status",
    "repeat_count",
    "repeat_stability_status",
    "maximum_repeat_phase_lift_stddev",
    *(f"{phase}_valid_day_count" for phase in PHASE_NAMES),
    *(
        f"{phase}_{suffix}"
        for phase in PHASE_NAMES
        for suffix in ("mean", "maximum", "peak_lift", "standardized_lift", "robust_lift")
    ),
    *LINEAGE_FIELDS,
}
UNMATCHED_FIELDS = (
    "source",
    "direction",
    "request_id",
    "repeat_id",
    "concept_id",
    "missing_join",
)
ANALYSIS_STRING_FIELDS = {
    "schema_version",
    "request_id",
    "repeat_id",
    "concept_id",
    "episode_id",
    "geography",
    "geography_level",
    "batch_id",
    "comparison_batch_id",
    "request_role",
    "terminology_version",
    "treated_geography",
    "semantic_family",
    "proxy_type",
    "export_attempt_id",
    "planned_repeat_id",
    "expected_output_filename",
    *WINDOW_FIELDS,
    "metric_quality_status",
    "metric_quality_reasons",
    "repeat_ids",
    "repeat_stability_status",
    "peak_date",
    "peak_phase",
    "global_peak_date",
    "global_peak_phase",
    "dominant_event_response_phase",
    "plan_join_status",
    "repeat_metrics_join_status",
    "episode_metrics_join_status",
    "analytical_row_grain",
    "source_episode_id",
}
ANALYSIS_COUNT_FIELDS = {
    "repeat_count",
    "baseline_valid_day_count",
    "event_valid_day_count",
    "post_valid_day_count",
    "peak_lead_lag_days",
    *(f"{phase}_valid_day_count" for phase in PHASE_NAMES),
}
ANALYSIS_BOOLEAN_FIELDS = {"repeat_stability_flag", "suppression_flag"}
ANALYSIS_OUTPUT_FIELDS = (
    "schema_version",
    "request_id",
    "repeat_id",
    "concept_id",
    "episode_id",
    "geography",
    "geography_level",
    "batch_id",
    "comparison_batch_id",
    "request_role",
    "terminology_version",
    "treated_geography",
    "semantic_family",
    "proxy_type",
    "export_attempt_id",
    "planned_repeat_id",
    "expected_output_filename",
    "event_start_date",
    "event_end_date",
    "baseline_start_date",
    "baseline_end_date",
    "post_start_date",
    "post_end_date",
    "metric_quality_status",
    "metric_quality_reasons",
    "repeat_count",
    "repeat_ids",
    "repeat_stability_flag",
    "repeat_stability_status",
    "maximum_repeat_phase_lift_stddev",
    "baseline_valid_day_count",
    "event_valid_day_count",
    "post_valid_day_count",
    "baseline_mean",
    "baseline_median",
    "baseline_standard_deviation",
    "baseline_mad",
    "event_maximum",
    "post_maximum",
    "absolute_peak_lift",
    "ratio_peak_lift",
    "z_score_peak_lift",
    "robust_peak_lift",
    "standardized_peak_lift",
    "peak_lead_lag_days",
    "zero_fraction",
    "suppression_flag",
    "peak_date",
    "peak_phase",
    "global_peak_date",
    "global_peak_phase",
    "global_peak_value",
    "dominant_event_response_phase",
    "dominant_standardized_phase_lift",
    "dominant_robust_phase_lift",
    *(
        f"{phase}_{suffix}"
        for phase in PHASE_NAMES
        for suffix in ("mean", "maximum", "peak_lift", "standardized_lift", "robust_lift")
    ),
    *(f"{phase}_valid_day_count" for phase in PHASE_NAMES),
    "plan_join_status",
    "repeat_metrics_join_status",
    "episode_metrics_join_status",
    "analytical_row_grain",
    "source_episode_id",
)


def _analysis_field(field: str) -> pa.Field:
    if field in ANALYSIS_STRING_FIELDS:
        return pa.field(field, pa.string())
    if field in ANALYSIS_COUNT_FIELDS:
        return pa.field(field, pa.int64())
    if field in ANALYSIS_BOOLEAN_FIELDS:
        return pa.field(field, pa.bool_())
    return pa.field(field, pa.float64())


ANALYSIS_DATASET_SCHEMA = pa.schema([_analysis_field(field) for field in ANALYSIS_OUTPUT_FIELDS])


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
    try:
        plan_rows = read_request_plan_rows(request_plan_path, required_fields=PLAN_FIELDS)
    except TrendsError as exc:
        raise AnalysisError(f"analysis_request_plan_error: {exc}") from exc
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
    unmatched_records: list[dict[str, str]] = []
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
        if plan is not None:
            _validate_planned_concept(plan, concept_id, key, request_plan_path)
        _validate_lineage(key, plan=plan, phase=phase, repeat=repeat, episode=episode)
        join_rows.append(
            {
                "request_id": request_id,
                "repeat_id": repeat_id,
                "concept_id": concept_id,
                **statuses,
            }
        )
        for source, status in statuses.items():
            if status == "unmatched":
                unmatched_records.append(
                    _unmatched_record(
                        source="phase_response_metrics",
                        direction="base_missing_join",
                        key=key,
                        missing_join=source.removesuffix("_join_status"),
                    )
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

    phase_keys = set(phase_index)
    repeat_keys = set(repeat_index)
    for (request_id, repeat_id), plan in sorted(plan_index.items()):
        planned_concepts = _concept_ids(plan, request_plan_path)
        applicable_repeats = (
            [repeat_id]
            if repeat_id
            else sorted(
                {key[1] for key in phase_keys | repeat_keys if key[0] == request_id and key[1]}
            )
            or [""]
        )
        for applicable_repeat in applicable_repeats:
            for concept_id in sorted(planned_concepts):
                expected = (request_id, applicable_repeat, concept_id)
                if expected not in phase_keys:
                    unmatched_records.append(
                        _unmatched_record(
                            source="request_plan",
                            direction="orphan_input",
                            key=expected,
                            missing_join="phase_response_metrics",
                        )
                    )
    for key in sorted(set(repeat_index) - phase_keys):
        unmatched_records.append(
            _unmatched_record(
                source="repeat_response_metrics",
                direction="orphan_input",
                key=key,
                missing_join="phase_response_metrics",
            )
        )
    phase_aggregate_keys = {(key[0], key[2]) for key in phase_keys}
    for request_id, concept_id in sorted(set(episode_index) - phase_aggregate_keys):
        unmatched_records.append(
            _unmatched_record(
                source="episode_response_metrics",
                direction="orphan_input",
                key=(request_id, "", concept_id),
                missing_join="phase_response_metrics",
            )
        )

    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise AnalysisError(
            f"analysis_output_write_error: unable_to_create_output_root path={output_root}"
        ) from exc
    dataset_path = output_root / "analysis_dataset.parquet"
    join_summary_path = output_root / "join_summary.json"
    unmatched_path = output_root / "unmatched_keys.csv"
    provenance_path = output_root / "provenance.json"
    _write_analysis_parquet(dataset_path, integrated)
    _write_csv(unmatched_path, unmatched_records, fields=UNMATCHED_FIELDS)
    join_summary = _join_summary(
        integrated,
        join_rows,
        unmatched_records=unmatched_records,
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
        "treated_geography": _first_value("treated_geography", phase, plan, repeat, episode),
        "semantic_family": _first_value("semantic_family", phase, repeat, episode),
        "proxy_type": _first_value("proxy_type", phase, repeat, episode),
        "export_attempt_id": _first_value("export_attempt_id", phase, repeat, episode),
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
        "standardized_peak_lift": phase.get("standardized_peak_lift"),
        "peak_lead_lag_days": phase.get("peak_lead_lag_days"),
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
    values = {
        f"{phase}_{suffix}": row.get(f"{phase}_{suffix}") for phase in phases for suffix in suffixes
    }
    values.update(
        {f"{phase}_valid_day_count": row.get(f"{phase}_valid_day_count") for phase in phases}
    )
    return values


def _plan_index(
    rows: Sequence[Mapping[str, Any]], path: Path
) -> dict[PlanGrain, Mapping[str, Any]]:
    keys = [(_plan_key(row), row) for row in rows]
    for key, _ in keys:
        if not key[0]:
            _require_key((key[0],), key_name="request_plan_request_id", path=path)
    duplicates = _duplicates([key for key, _ in keys])
    if duplicates:
        raise AnalysisError(
            "duplicate_normalized_keys: "
            f"key_name=request_plan_key duplicates={_stringify_keys(duplicates)} path={path}"
        )
    by_request: dict[str, set[str]] = {}
    for request_id, repeat_id in (key for key, _ in keys):
        by_request.setdefault(request_id, set()).add(repeat_id)
    ambiguous = sorted(
        request_id
        for request_id, repeat_ids in by_request.items()
        if "" in repeat_ids and len(repeat_ids) > 1
    )
    if ambiguous:
        raise AnalysisError(
            f"ambiguous_request_plan_fallback: request_ids={ambiguous[:20]} path={path}"
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
    for key, _ in keys:
        _require_key(key, key_name=key_name, path=path)
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


def _require_key(key: tuple[str, ...], *, key_name: str, path: Path) -> None:
    blank_positions = [index for index, value in enumerate(key) if not value]
    if blank_positions:
        raise AnalysisError(
            "analysis_input_empty_key: "
            f"key_name={key_name} key={list(key)} blank_positions={blank_positions} path={path}"
        )


def _concept_ids(plan: Mapping[str, Any], path: Path) -> set[str]:
    concepts = {
        item.strip() for item in str(plan.get("concept_ids") or "").split(";") if item.strip()
    }
    if not concepts:
        raise AnalysisError(
            "analysis_input_empty_key: key_name=request_plan_concept_ids "
            f"request_id={_normalized(plan, 'request_id')} path={path}"
        )
    return concepts


def _validate_planned_concept(
    plan: Mapping[str, Any], concept_id: str, key: AnalysisGrain, path: Path
) -> None:
    planned = _concept_ids(plan, path)
    if concept_id not in planned:
        raise AnalysisError(
            "analysis_unplanned_concept: "
            f"key={'|'.join(key)} concept_id={concept_id} planned={sorted(planned)} path={path}"
        )


def _lineage_value(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _validate_lineage(
    key: AnalysisGrain,
    *,
    plan: Mapping[str, Any] | None,
    phase: Mapping[str, Any],
    repeat: Mapping[str, Any] | None,
    episode: Mapping[str, Any] | None,
) -> None:
    sources = {
        "request_plan": plan,
        "phase_response_metrics": phase,
        "repeat_response_metrics": repeat,
        "episode_response_metrics": episode,
    }
    conflicts: dict[str, dict[str, str]] = {}
    for field in sorted(LINEAGE_FIELDS):
        values = {
            source: _lineage_value(row.get(field))
            for source, row in sources.items()
            if row is not None and _lineage_value(row.get(field))
        }
        if len(set(values.values())) > 1:
            conflicts[field] = values
    if conflicts:
        raise AnalysisError(
            "analysis_lineage_conflict: "
            f"key={'|'.join(key)} conflicts={json.dumps(conflicts, sort_keys=True)}"
        )


def _unmatched_record(
    *, source: str, direction: str, key: AnalysisGrain, missing_join: str
) -> dict[str, str]:
    return {
        "source": source,
        "direction": direction,
        "request_id": key[0],
        "repeat_id": key[1],
        "concept_id": key[2],
        "missing_join": missing_join,
    }


def _duplicates(keys: Sequence[tuple[str, ...]]) -> list[tuple[str, ...]]:
    counts = Counter(keys)
    return sorted(key for key, count in counts.items() if count > 1)


def _stringify_keys(keys: Sequence[tuple[str, ...]]) -> list[str]:
    return ["|".join(key) for key in keys[:20]]


def _read_parquet_rows(path: Path, required_fields: set[str]) -> list[dict[str, Any]]:
    if path.suffix.casefold() not in {".parquet", ".pq"}:
        raise AnalysisError(
            f"unsupported_analysis_metric_format: suffix={path.suffix!r} path={path}"
        )
    if not path.exists():
        raise AnalysisError(f"Input Parquet is missing: {path}")
    try:
        table = pq.read_table(path)
    except (OSError, pa.ArrowException) as exc:
        raise AnalysisError(f"analysis_input_read_error: path={path} detail={exc}") from exc
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
        if row is None:
            continue
        value = row.get(field)
        if value is not None and value != "":
            return value
    return None


def _join_summary(
    rows: Sequence[Mapping[str, Any]],
    join_rows: Sequence[Mapping[str, Any]],
    *,
    unmatched_records: Sequence[Mapping[str, Any]],
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
    reverse = Counter(
        str(row["source"]) for row in unmatched_records if row["direction"] == "orphan_input"
    )
    return {
        "analysis_dataset_version": ANALYSIS_DATASET_VERSION,
        "analytical_row_grain": ["request_id", "repeat_id", "concept_id"],
        "row_counts": {
            **dict(input_counts),
            "analysis_dataset_rows": len(rows),
            "unmatched_key_rows": len(unmatched_records),
        },
        "join_cardinalities": {
            "request_plan": "many_to_one_by_request_repeat_or_request",
            "episode_response_metrics": "many_to_one_by_request_concept",
            "repeat_response_metrics": "one_to_one_by_request_repeat_concept",
            "phase_response_metrics": "base_unique_by_request_repeat_concept",
        },
        "unmatched_counts": unmatched,
        "orphan_counts": {
            "request_plan": reverse["request_plan"],
            "request_plan_missing_concepts": reverse["request_plan"],
            "repeat_response_metrics": reverse["repeat_response_metrics"],
            "episode_response_metrics": reverse["episode_response_metrics"],
        },
        "all_required_joins_matched": not unmatched_records,
    }


def _file_provenance(path: Path) -> dict[str, Any]:
    try:
        return {
            "path": str(path),
            "file_name": path.name,
            "byte_count": path.stat().st_size,
            "sha256": _sha256(path),
        }
    except OSError as exc:
        raise AnalysisError(f"analysis_provenance_error: path={path}") from exc


def _write_analysis_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    try:
        ordered = _deterministic_rows(rows)
        table = pa.Table.from_pylist(ordered, schema=ANALYSIS_DATASET_SCHEMA)
    except (pa.ArrowException, TypeError, ValueError, OverflowError) as exc:
        raise AnalysisError(f"analysis_output_build_error: path={path} detail={exc}") from exc
    try:
        pq.write_table(table, path, compression="zstd")
    except (OSError, pa.ArrowException) as exc:
        raise AnalysisError(f"analysis_output_write_error: path={path} detail={exc}") from exc


def _write_csv(
    path: Path, rows: Sequence[Mapping[str, Any]], *, fields: Sequence[str] | None = None
) -> None:
    ordered_fields = list(fields or sorted({key for row in rows for key in row}))
    try:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=ordered_fields, lineterminator="\n")
            if ordered_fields:
                writer.writeheader()
                writer.writerows(_deterministic_rows(rows))
    except (OSError, csv.Error, TypeError, ValueError) as exc:
        raise AnalysisError(f"analysis_output_write_error: path={path} detail={exc}") from exc


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    try:
        serialized = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    except (TypeError, ValueError, OverflowError) as exc:
        raise AnalysisError(f"analysis_output_build_error: path={path} detail={exc}") from exc
    try:
        path.write_text(serialized, encoding="utf-8")
    except OSError as exc:
        raise AnalysisError(f"analysis_output_write_error: path={path} detail={exc}") from exc


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
