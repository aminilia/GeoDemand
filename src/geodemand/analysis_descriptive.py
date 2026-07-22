from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand import __version__
from geodemand.analysis import ANALYSIS_DATASET_VERSION

DESCRIPTIVE_ANALYSIS_VERSION = "1.0B-descriptive-v2"
METRIC_CONFIGURATION_VERSION = "1.0B-metrics-v1"
ANALYTICAL_GRAIN = ("request_id", "repeat_id", "concept_id")
REQUIRED_LINEAGE = ("episode_id", "geography", "semantic_family", "request_role")
REQUIRED_FIELDS = frozenset((*ANALYTICAL_GRAIN, *REQUIRED_LINEAGE))
GROUP_LINEAGE = ("episode_id", "geography", "semantic_family", "request_role")
RELATIVE_DIFFERENCE_EPSILON = 1e-12
MINIMUM_COMMON_CONCEPTS = 3
MINIMUM_INDEPENDENT_REQUESTS = 20
PAIR_SIZE = 2

DESCRIPTIVE_METRICS = (
    "baseline_mean",
    "baseline_median",
    "baseline_standard_deviation",
    "baseline_mad",
    "event_maximum",
    "post_maximum",
    "absolute_peak_lift",
    "peak_lead_lag_days",
    "zero_fraction",
    "global_peak_value",
    "anticipatory_mean",
    "anticipatory_maximum",
    "anticipatory_peak_lift",
    "immediate_mean",
    "immediate_maximum",
    "immediate_peak_lift",
    "early_recovery_mean",
    "early_recovery_maximum",
    "early_recovery_peak_lift",
    "extended_recovery_mean",
    "extended_recovery_maximum",
    "extended_recovery_peak_lift",
)

REPEAT_DIAGNOSTIC_METRICS = (
    *DESCRIPTIVE_METRICS[:7],
    "ratio_peak_lift",
    "z_score_peak_lift",
    "robust_peak_lift",
    "standardized_peak_lift",
    *DESCRIPTIVE_METRICS[7:10],
    "dominant_standardized_phase_lift",
    "dominant_robust_phase_lift",
    *DESCRIPTIVE_METRICS[10:13],
    "anticipatory_standardized_lift",
    "anticipatory_robust_lift",
    *DESCRIPTIVE_METRICS[13:16],
    "immediate_standardized_lift",
    "immediate_robust_lift",
    *DESCRIPTIVE_METRICS[16:19],
    "early_recovery_standardized_lift",
    "early_recovery_robust_lift",
    *DESCRIPTIVE_METRICS[19:22],
    "extended_recovery_standardized_lift",
    "extended_recovery_robust_lift",
)

COUNT_AND_FLAG_METRICS = (
    "repeat_count",
    "repeat_stability_flag",
    "maximum_repeat_phase_lift_stddev",
    "baseline_valid_day_count",
    "event_valid_day_count",
    "post_valid_day_count",
    "suppression_flag",
    "anticipatory_valid_day_count",
    "immediate_valid_day_count",
    "early_recovery_valid_day_count",
    "extended_recovery_valid_day_count",
)

METRIC_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("analysis_scope", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("raw_repeat_row_count", pa.int64()),
        pa.field("valid_repeat_value_count", pa.int64()),
        pa.field("missing_repeat_value_count", pa.int64()),
        pa.field("non_finite_repeat_value_count", pa.int64()),
        pa.field("request_concept_unit_count", pa.int64()),
        pa.field("valid_request_concept_unit_count", pa.int64()),
        pa.field("independent_request_count", pa.int64()),
        pa.field("independent_episode_count", pa.int64()),
        pa.field("independent_geography_count", pa.int64()),
        pa.field("concept_count", pa.int64()),
        pa.field("minimum", pa.float64()),
        pa.field("maximum", pa.float64()),
        pa.field("mean", pa.float64()),
        pa.field("median", pa.float64()),
        pa.field("sample_standard_deviation", pa.float64()),
        pa.field("first_quartile", pa.float64()),
        pa.field("third_quartile", pa.float64()),
        pa.field("interquartile_range", pa.float64()),
        pa.field("zero_count", pa.int64()),
        pa.field("positive_count", pa.int64()),
        pa.field("negative_count", pa.int64()),
    ]
)

REQUEST_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("request_id", pa.string()),
        pa.field("episode_id", pa.string()),
        pa.field("geography", pa.string()),
        pa.field("concept_id", pa.string()),
        pa.field("semantic_family", pa.string()),
        pa.field("request_role", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("repeat_count", pa.int64()),
        pa.field("valid_repeat_count", pa.int64()),
        pa.field("missing_count", pa.int64()),
        pa.field("non_finite_count", pa.int64()),
        pa.field("repeat_mean", pa.float64()),
        pa.field("repeat_median", pa.float64()),
        pa.field("repeat_minimum", pa.float64()),
        pa.field("repeat_maximum", pa.float64()),
        pa.field("within_request_standard_deviation", pa.float64()),
    ]
)

GROUP_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("grouping_dimension", pa.string()),
        pa.field("group_value", pa.string()),
        pa.field("concept_id", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("raw_repeat_row_count", pa.int64()),
        pa.field("valid_repeat_value_count", pa.int64()),
        pa.field("missing_repeat_value_count", pa.int64()),
        pa.field("non_finite_repeat_value_count", pa.int64()),
        pa.field("request_concept_unit_count", pa.int64()),
        pa.field("valid_request_concept_unit_count", pa.int64()),
        pa.field("independent_request_count", pa.int64()),
        pa.field("mean", pa.float64()),
        pa.field("median", pa.float64()),
        pa.field("minimum", pa.float64()),
        pa.field("maximum", pa.float64()),
        pa.field("sample_standard_deviation", pa.float64()),
    ]
)

FAMILY_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("semantic_family", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("raw_repeat_row_count", pa.int64()),
        pa.field("valid_repeat_value_count", pa.int64()),
        pa.field("missing_repeat_value_count", pa.int64()),
        pa.field("non_finite_repeat_value_count", pa.int64()),
        pa.field("request_concept_unit_count", pa.int64()),
        pa.field("valid_request_concept_unit_count", pa.int64()),
        pa.field("independent_request_count", pa.int64()),
        pa.field("concept_count", pa.int64()),
        pa.field("mean", pa.float64()),
        pa.field("median", pa.float64()),
        pa.field("minimum", pa.float64()),
        pa.field("maximum", pa.float64()),
        pa.field("sample_standard_deviation", pa.float64()),
    ]
)

PAIR_SCHEMA = pa.schema(
    [
        pa.field("request_id", pa.string()),
        pa.field("episode_id", pa.string()),
        pa.field("geography", pa.string()),
        pa.field("concept_id", pa.string()),
        pa.field("semantic_family", pa.string()),
        pa.field("request_role", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("first_repeat_id", pa.string()),
        pa.field("second_repeat_id", pa.string()),
        pa.field("first_value", pa.float64()),
        pa.field("second_value", pa.float64()),
        pa.field("signed_difference", pa.float64()),
        pa.field("absolute_difference", pa.float64()),
        pa.field("pair_mean", pa.float64()),
        pa.field("within_pair_sample_standard_deviation", pa.float64()),
        pa.field("relative_difference", pa.float64()),
        pa.field("relative_difference_eligible", pa.bool_()),
        pa.field("relative_difference_exclusion_reason", pa.string()),
    ]
)

RANK_SCHEMA = pa.schema(
    [
        pa.field("request_id", pa.string()),
        pa.field("episode_id", pa.string()),
        pa.field("geography", pa.string()),
        pa.field("semantic_family_scope", pa.string()),
        pa.field("request_role", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("first_repeat_id", pa.string()),
        pa.field("second_repeat_id", pa.string()),
        pa.field("common_concept_count", pa.int64()),
        pa.field("eligible", pa.bool_()),
        pa.field("spearman_rank_correlation", pa.float64()),
        pa.field("exclusion_reason", pa.string()),
        pa.field("independent_repeated_unit", pa.string()),
    ]
)

EXCLUSION_FIELDS = (
    "analysis_component",
    "metric",
    "request_id",
    "concept_id",
    "reason",
    "missing_count",
    "valid_count",
)


class DescriptiveAnalysisError(ValueError):
    """Raised when descriptive analysis cannot safely produce its contract."""


def describe_signal(*, dataset_path: Path, output_root: Path) -> dict[str, Path]:
    table, rows = _read_dataset(dataset_path)
    duplicate_count = _validate_rows(rows)
    _validate_metric_configuration(table)
    lineage_complete = _lineage_complete(rows)

    exclusions: list[dict[str, Any]] = []
    request_summary = _request_summaries(rows)
    metric_summary = [
        _metric_summary(metric, rows, request_summary) for metric in DESCRIPTIVE_METRICS
    ]
    geography_summary = _group_summaries(request_summary, "geography")
    episode_summary = _group_summaries(request_summary, "episode_id")
    family_summary = _family_summaries(request_summary)
    paired, pair_exclusions = _paired_diagnostics(rows)
    rank, rank_exclusions = _rank_agreement(rows)
    exclusions.extend(_metric_exclusions(table))
    exclusions.extend(_value_exclusions(rows))
    exclusions.extend(pair_exclusions)
    exclusions.extend(rank_exclusions)
    exclusions = _sort_rows(exclusions, EXCLUSION_FIELDS)

    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_output_write_error: path={output_root} detail={exc}"
        ) from exc
    paths = {
        "descriptive_signal_summary": output_root / "descriptive_signal_summary.parquet",
        "request_concept_summary": output_root / "request_concept_summary.parquet",
        "geography_concept_summary": output_root / "geography_concept_summary.parquet",
        "episode_concept_summary": output_root / "episode_concept_summary.parquet",
        "semantic_family_summary": output_root / "semantic_family_summary.parquet",
        "paired_repeat_diagnostics": output_root / "paired_repeat_diagnostics.parquet",
        "concept_rank_agreement": output_root / "concept_rank_agreement.parquet",
        "descriptive_exclusions": output_root / "descriptive_exclusions.csv",
        "descriptive_summary": output_root / "descriptive_summary.json",
        "provenance": output_root / "provenance.json",
    }
    _write_parquet(paths["descriptive_signal_summary"], metric_summary, METRIC_SUMMARY_SCHEMA)
    _write_parquet(paths["request_concept_summary"], request_summary, REQUEST_SUMMARY_SCHEMA)
    _write_parquet(paths["geography_concept_summary"], geography_summary, GROUP_SUMMARY_SCHEMA)
    _write_parquet(paths["episode_concept_summary"], episode_summary, GROUP_SUMMARY_SCHEMA)
    _write_parquet(paths["semantic_family_summary"], family_summary, FAMILY_SUMMARY_SCHEMA)
    _write_parquet(paths["paired_repeat_diagnostics"], paired, PAIR_SCHEMA)
    _write_parquet(paths["concept_rank_agreement"], rank, RANK_SCHEMA)
    _write_exclusions(paths["descriptive_exclusions"], exclusions)

    requests = sorted({str(row["request_id"]) for row in rows})
    episodes = sorted({str(row["episode_id"]) for row in rows})
    geographies = sorted({str(row["geography"]) for row in rows})
    repeat_groups = _repeat_groups(rows)
    repeated_requests = sorted(
        {
            request
            for (request, _concept), repeat_ids in repeat_groups.items()
            if len(repeat_ids) >= PAIR_SIZE
        }
    )
    eligible_groups = sum(len(repeat_ids) == PAIR_SIZE for repeat_ids in repeat_groups.values())
    output_row_counts = {
        "descriptive_signal_summary": len(metric_summary),
        "request_concept_summary": len(request_summary),
        "geography_concept_summary": len(geography_summary),
        "episode_concept_summary": len(episode_summary),
        "semantic_family_summary": len(family_summary),
        "paired_repeat_diagnostics": len(paired),
        "concept_rank_agreement": len(rank),
        "descriptive_exclusions": len(exclusions),
        "descriptive_summary": 1,
        "provenance": 1,
    }
    summary = {
        "analysis_scope": "descriptive_and_exploratory_only",
        "principal_summary_unit": "request_id + concept_id repeat mean",
        "analytical_grain": list(ANALYTICAL_GRAIN),
        "dataset": {
            "path": str(dataset_path),
            "sha256": _sha256(dataset_path),
            "row_count": table.num_rows,
            "column_count": table.num_columns,
        },
        "duplicate_grain_count": duplicate_count,
        "independent_request_count": len(requests),
        "independent_episode_count": len(episodes),
        "independent_geography_count": len(geographies),
        "repeated_request_count": len(repeated_requests),
        "repeat_eligible_request_concept_group_count": eligible_groups,
        "metrics_included_in_descriptive_summaries": list(DESCRIPTIVE_METRICS),
        "metrics_included_in_repeat_diagnostics": list(REPEAT_DIAGNOSTIC_METRICS),
        "exclusions_by_reason": dict(sorted(Counter(row["reason"] for row in exclusions).items())),
        "output_row_counts": output_row_counts,
        "lineage_complete": lineage_complete,
        "warnings": _warnings(
            rows=rows,
            independent_request_count=len(requests),
            repeated_requests=repeated_requests,
            eligible_repeat_group_count=eligible_groups,
            lineage_complete=lineage_complete,
        ),
    }
    _write_json(paths["descriptive_summary"], summary)
    output_hashes = {
        name: {"row_count": output_row_counts.get(name), "sha256": _sha256(path)}
        for name, path in paths.items()
        if name != "provenance"
    }
    provenance = {
        "command": "geodemand analysis describe-signal",
        "package_version": __version__,
        "integrated_dataset_version": _dataset_version(rows),
        "analytical_grain": list(ANALYTICAL_GRAIN),
        "metric_configuration_version": METRIC_CONFIGURATION_VERSION,
        "descriptive_analysis_version": DESCRIPTIVE_ANALYSIS_VERSION,
        "principal_summary_unit": "request_id + concept_id repeat mean",
        "input": {
            "path": str(dataset_path),
            "byte_count": _byte_count(dataset_path),
            "sha256": _sha256(dataset_path),
            "row_count": table.num_rows,
        },
        "outputs": output_hashes,
        "exclusions": summary["exclusions_by_reason"],
    }
    _write_json(paths["provenance"], provenance)
    return paths


def _read_dataset(path: Path) -> tuple[pa.Table, list[dict[str, Any]]]:
    if path.suffix.casefold() not in {".parquet", ".pq"}:
        raise DescriptiveAnalysisError(
            f"descriptive_input_read_error: unsupported_suffix={path.suffix!r} path={path}"
        )
    try:
        table = pq.read_table(path)
        rows = cast(list[dict[str, Any]], table.to_pylist())
    except (OSError, pa.ArrowException, TypeError, ValueError) as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_input_read_error: path={path} detail={exc}"
        ) from exc
    missing = sorted(REQUIRED_FIELDS - set(table.column_names))
    if missing:
        raise DescriptiveAnalysisError(
            f"descriptive_input_missing_fields: missing={missing} path={path}"
        )
    return table, rows


def _validate_rows(rows: Sequence[Mapping[str, Any]]) -> int:
    keys: list[tuple[str, str, str]] = []
    for index, row in enumerate(rows):
        normalized: list[str] = []
        for field in REQUIRED_FIELDS:
            value = row.get(field)
            if not isinstance(value, str) or not value.strip():
                raise DescriptiveAnalysisError(
                    f"descriptive_invalid_key: row={index} field={field} value={value!r}"
                )
        for field in ANALYTICAL_GRAIN:
            normalized.append(cast(str, row[field]).strip())
        keys.append(cast(tuple[str, str, str], tuple(normalized)))
    duplicates = [key for key, count in Counter(keys).items() if count > 1]
    if duplicates:
        raise DescriptiveAnalysisError(
            f"descriptive_duplicate_grain: count={len(keys) - len(set(keys))} "
            f"examples={sorted(duplicates)[:20]}"
        )
    _validate_group_lineage(rows)
    return 0


def _validate_group_lineage(rows: Sequence[Mapping[str, Any]]) -> None:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].append(row)
    for key, group in sorted(grouped.items()):
        conflicts = {
            field: sorted({str(row[field]).strip() for row in group})
            for field in GROUP_LINEAGE
            if len({str(row[field]).strip() for row in group}) > 1
        }
        if conflicts:
            raise DescriptiveAnalysisError(
                f"descriptive_output_build_error: conflicting_lineage key={key} "
                f"conflicts={conflicts}"
            )


def _validate_metric_configuration(table: pa.Table) -> None:
    configured = set(DESCRIPTIVE_METRICS) | set(REPEAT_DIAGNOSTIC_METRICS)
    missing = sorted(configured - set(table.column_names))
    if missing:
        raise DescriptiveAnalysisError(
            f"descriptive_input_missing_fields: configured_metrics={missing}"
        )
    non_numeric = sorted(
        field.name
        for field in table.schema
        if field.name in configured
        and not (pa.types.is_integer(field.type) or pa.types.is_floating(field.type))
    )
    if non_numeric:
        raise DescriptiveAnalysisError(
            f"descriptive_output_build_error: configured_metrics_not_numeric={non_numeric}"
        )


def _metric_summary(
    metric: str,
    rows: Sequence[Mapping[str, Any]],
    request_summaries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    units = [row for row in request_summaries if row["metric"] == metric]
    valid_units = [row for row in units if _is_finite(row.get("repeat_mean"))]
    values = [float(row["repeat_mean"]) for row in valid_units]
    stats = _stats(values)
    return {
        "analysis_scope": "request_concept_repeat_mean_descriptive_only",
        "metric": metric,
        "raw_repeat_row_count": len(rows),
        "valid_repeat_value_count": sum(_is_finite(row.get(metric)) for row in rows),
        "missing_repeat_value_count": sum(row.get(metric) is None for row in rows),
        "non_finite_repeat_value_count": sum(
            row.get(metric) is not None and not _is_finite(row.get(metric)) for row in rows
        ),
        "request_concept_unit_count": len(units),
        "valid_request_concept_unit_count": len(valid_units),
        "independent_request_count": len({str(row["request_id"]) for row in valid_units}),
        "independent_episode_count": len({str(row["episode_id"]) for row in valid_units}),
        "independent_geography_count": len({str(row["geography"]) for row in valid_units}),
        "concept_count": len({str(row["concept_id"]) for row in valid_units}),
        **stats,
        "zero_count": sum(value == 0 for value in values),
        "positive_count": sum(value > 0 for value in values),
        "negative_count": sum(value < 0 for value in values),
    }


def _request_summaries(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped = _request_concept_groups(rows)
    output: list[dict[str, Any]] = []
    for (request_id, concept_id), group in sorted(grouped.items()):
        lineage = group[0]
        repeat_count = len({str(row["repeat_id"]) for row in group})
        for metric in DESCRIPTIVE_METRICS:
            values = [float(row[metric]) for row in group if _is_finite(row.get(metric))]
            output.append(
                {
                    "request_id": request_id,
                    "episode_id": str(lineage["episode_id"]),
                    "geography": str(lineage["geography"]),
                    "concept_id": concept_id,
                    "semantic_family": str(lineage["semantic_family"]),
                    "request_role": str(lineage["request_role"]),
                    "metric": metric,
                    "repeat_count": repeat_count,
                    "valid_repeat_count": len(values),
                    "missing_count": sum(row.get(metric) is None for row in group),
                    "non_finite_count": sum(
                        row.get(metric) is not None and not _is_finite(row.get(metric))
                        for row in group
                    ),
                    "repeat_mean": _mean(values),
                    "repeat_median": _median(values),
                    "repeat_minimum": min(values) if values else None,
                    "repeat_maximum": max(values) if values else None,
                    "within_request_standard_deviation": (
                        statistics.stdev(values) if len(values) >= PAIR_SIZE else None
                    ),
                }
            )
    return _sort_rows(output, REQUEST_SUMMARY_SCHEMA.names)


def _group_summaries(
    request_summaries: Sequence[Mapping[str, Any]], field: str
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in request_summaries:
        grouped[(str(row[field]), str(row["concept_id"]))].append(row)
    output: list[dict[str, Any]] = []
    for (group_value, concept_id), group in sorted(grouped.items()):
        for metric in DESCRIPTIVE_METRICS:
            units = [row for row in group if row["metric"] == metric]
            valid_units = [row for row in units if _is_finite(row.get("repeat_mean"))]
            values = [float(row["repeat_mean"]) for row in valid_units]
            output.append(
                {
                    "grouping_dimension": field,
                    "group_value": group_value,
                    "concept_id": concept_id,
                    "metric": metric,
                    "raw_repeat_row_count": sum(int(row["repeat_count"]) for row in units),
                    "valid_repeat_value_count": sum(
                        int(row["valid_repeat_count"]) for row in units
                    ),
                    "missing_repeat_value_count": sum(int(row["missing_count"]) for row in units),
                    "non_finite_repeat_value_count": sum(
                        int(row["non_finite_count"]) for row in units
                    ),
                    "request_concept_unit_count": len(units),
                    "valid_request_concept_unit_count": len(valid_units),
                    "independent_request_count": len(
                        {str(row["request_id"]) for row in valid_units}
                    ),
                    "mean": _mean(values),
                    "median": _median(values),
                    "minimum": min(values) if values else None,
                    "maximum": max(values) if values else None,
                    "sample_standard_deviation": (
                        statistics.stdev(values) if len(values) >= PAIR_SIZE else None
                    ),
                }
            )
    return _sort_rows(output, GROUP_SUMMARY_SCHEMA.names)


def _family_summaries(
    request_summaries: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in request_summaries:
        grouped[str(row["semantic_family"])].append(row)
    output: list[dict[str, Any]] = []
    for family, group in sorted(grouped.items()):
        for metric in DESCRIPTIVE_METRICS:
            units = [row for row in group if row["metric"] == metric]
            valid_units = [row for row in units if _is_finite(row.get("repeat_mean"))]
            values = [float(row["repeat_mean"]) for row in valid_units]
            output.append(
                {
                    "semantic_family": family,
                    "metric": metric,
                    "raw_repeat_row_count": sum(int(row["repeat_count"]) for row in units),
                    "valid_repeat_value_count": sum(
                        int(row["valid_repeat_count"]) for row in units
                    ),
                    "missing_repeat_value_count": sum(int(row["missing_count"]) for row in units),
                    "non_finite_repeat_value_count": sum(
                        int(row["non_finite_count"]) for row in units
                    ),
                    "request_concept_unit_count": len(units),
                    "valid_request_concept_unit_count": len(valid_units),
                    "independent_request_count": len(
                        {str(row["request_id"]) for row in valid_units}
                    ),
                    "concept_count": len({str(row["concept_id"]) for row in valid_units}),
                    "mean": _mean(values),
                    "median": _median(values),
                    "minimum": min(values) if values else None,
                    "maximum": max(values) if values else None,
                    "sample_standard_deviation": (
                        statistics.stdev(values) if len(values) >= PAIR_SIZE else None
                    ),
                }
            )
    return _sort_rows(output, FAMILY_SUMMARY_SCHEMA.names)


def _paired_diagnostics(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    output: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    for (request_id, concept_id), group in sorted(_request_concept_groups(rows).items()):
        lineage = group[0]
        repeat_ids = {str(row["repeat_id"]) for row in group}
        for metric in REPEAT_DIAGNOSTIC_METRICS:
            valid = sorted(
                (
                    (str(row["repeat_id"]), float(row[metric]))
                    for row in group
                    if _is_finite(row.get(metric))
                ),
                key=lambda item: item[0],
            )
            missing = sum(row.get(metric) is None for row in group)
            if len(repeat_ids) > PAIR_SIZE:
                exclusions.append(
                    _exclusion(
                        "paired_repeat",
                        metric,
                        request_id,
                        concept_id,
                        "more_than_two_repeats_requires_nonpaired_summary",
                        missing,
                        len(valid),
                    )
                )
                continue
            if len(valid) != PAIR_SIZE:
                reason = (
                    "fewer_than_two_valid_repeats"
                    if len(valid) < PAIR_SIZE
                    else "more_than_two_repeats_requires_nonpaired_summary"
                )
                exclusions.append(
                    _exclusion(
                        "paired_repeat", metric, request_id, concept_id, reason, missing, len(valid)
                    )
                )
                continue
            first, second = valid
            relative_eligible = abs(first[1]) > RELATIVE_DIFFERENCE_EPSILON
            relative_reason = "" if relative_eligible else "zero_or_near_zero_denominator"
            if not relative_eligible:
                exclusions.append(
                    _exclusion(
                        "paired_relative_difference",
                        metric,
                        request_id,
                        concept_id,
                        relative_reason,
                        missing,
                        len(valid),
                    )
                )
            difference = second[1] - first[1]
            output.append(
                {
                    "request_id": request_id,
                    "episode_id": str(lineage["episode_id"]),
                    "geography": str(lineage["geography"]),
                    "concept_id": concept_id,
                    "semantic_family": str(lineage["semantic_family"]),
                    "request_role": str(lineage["request_role"]),
                    "metric": metric,
                    "first_repeat_id": first[0],
                    "second_repeat_id": second[0],
                    "first_value": first[1],
                    "second_value": second[1],
                    "signed_difference": difference,
                    "absolute_difference": abs(difference),
                    "pair_mean": statistics.mean((first[1], second[1])),
                    "within_pair_sample_standard_deviation": statistics.stdev(
                        (first[1], second[1])
                    ),
                    "relative_difference": difference / abs(first[1])
                    if relative_eligible
                    else None,
                    "relative_difference_eligible": relative_eligible,
                    "relative_difference_exclusion_reason": relative_reason,
                }
            )
    return _sort_rows(output, PAIR_SCHEMA.names), exclusions


def _rank_agreement(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_request: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_request[str(row["request_id"])].append(row)
    output: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    for request_id, group in sorted(by_request.items()):
        repeat_ids = sorted({str(row["repeat_id"]) for row in group})
        if len(repeat_ids) < PAIR_SIZE:
            for metric in REPEAT_DIAGNOSTIC_METRICS:
                exclusions.append(
                    _exclusion(
                        "concept_rank_agreement",
                        metric,
                        request_id,
                        "",
                        "fewer_than_two_valid_repeats",
                        0,
                        len(repeat_ids),
                    )
                )
            continue
        if len(repeat_ids) > PAIR_SIZE:
            lineage = group[0]
            for metric in REPEAT_DIAGNOSTIC_METRICS:
                reason = "more_than_two_repeats_requires_multi_repeat_rank_summary"
                exclusions.append(
                    _exclusion(
                        "concept_rank_agreement",
                        metric,
                        request_id,
                        "",
                        reason,
                        0,
                        len(repeat_ids),
                    )
                )
                output.append(
                    {
                        "request_id": request_id,
                        "episode_id": str(lineage["episode_id"]),
                        "geography": str(lineage["geography"]),
                        "semantic_family_scope": "all_available_concepts",
                        "request_role": str(lineage["request_role"]),
                        "metric": metric,
                        "first_repeat_id": "",
                        "second_repeat_id": "",
                        "common_concept_count": 0,
                        "eligible": False,
                        "spearman_rank_correlation": None,
                        "exclusion_reason": reason,
                        "independent_repeated_unit": "request_id",
                    }
                )
            continue
        lineage = group[0]
        first_repeat, second_repeat = repeat_ids
        for metric in REPEAT_DIAGNOSTIC_METRICS:
            first = {
                str(row["concept_id"]): float(row[metric])
                for row in group
                if str(row["repeat_id"]) == first_repeat and _is_finite(row.get(metric))
            }
            second = {
                str(row["concept_id"]): float(row[metric])
                for row in group
                if str(row["repeat_id"]) == second_repeat and _is_finite(row.get(metric))
            }
            common = sorted(set(first) & set(second))
            eligible = len(common) >= MINIMUM_COMMON_CONCEPTS
            reason = "" if eligible else "insufficient_common_concepts_for_rank_agreement"
            correlation = (
                _spearman([first[key] for key in common], [second[key] for key in common])
                if eligible
                else None
            )
            if eligible and correlation is None:
                eligible = False
                reason = "constant_rank_vector"
            if not eligible:
                exclusions.append(
                    _exclusion(
                        "concept_rank_agreement", metric, request_id, "", reason, 0, len(common)
                    )
                )
            output.append(
                {
                    "request_id": request_id,
                    "episode_id": str(lineage["episode_id"]),
                    "geography": str(lineage["geography"]),
                    "semantic_family_scope": "all_available_concepts",
                    "request_role": str(lineage["request_role"]),
                    "metric": metric,
                    "first_repeat_id": first_repeat,
                    "second_repeat_id": second_repeat,
                    "common_concept_count": len(common),
                    "eligible": eligible,
                    "spearman_rank_correlation": correlation,
                    "exclusion_reason": reason,
                    "independent_repeated_unit": "request_id",
                }
            )
    return _sort_rows(output, RANK_SCHEMA.names), exclusions


def _spearman(first: Sequence[float], second: Sequence[float]) -> float | None:
    if len(first) != len(second) or len(first) < PAIR_SIZE:
        return None
    return _pearson(_average_ranks(first), _average_ranks(second))


def _average_ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    ranks = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[end]] == values[order[start]]:
            end += 1
        rank = (start + 1 + end) / 2.0
        for position in range(start, end):
            ranks[order[position]] = rank
        start = end
    return ranks


def _pearson(first: Sequence[float], second: Sequence[float]) -> float | None:
    first_mean = statistics.mean(first)
    second_mean = statistics.mean(second)
    numerator = sum(
        (x - first_mean) * (y - second_mean) for x, y in zip(first, second, strict=True)
    )
    first_ss = sum((x - first_mean) ** 2 for x in first)
    second_ss = sum((y - second_mean) ** 2 for y in second)
    denominator = math.sqrt(first_ss * second_ss)
    return numerator / denominator if denominator else None


def _metric_exclusions(table: pa.Table) -> list[dict[str, Any]]:
    numeric = {
        field.name
        for field in table.schema
        if pa.types.is_integer(field.type)
        or pa.types.is_floating(field.type)
        or pa.types.is_boolean(field.type)
    }
    selected = set(DESCRIPTIVE_METRICS) | set(REPEAT_DIAGNOSTIC_METRICS)
    return [
        _exclusion("metric_selection", metric, "", "", "metric_not_primary_continuous", 0, 0)
        for metric in sorted(numeric - selected)
    ]


def _value_exclusions(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for metric in sorted(set(DESCRIPTIVE_METRICS) | set(REPEAT_DIAGNOSTIC_METRICS)):
        missing = sum(row.get(metric) is None for row in rows)
        non_finite = sum(
            row.get(metric) is not None and not _is_finite(row.get(metric)) for row in rows
        )
        valid = sum(_is_finite(row.get(metric)) for row in rows)
        if missing:
            output.append(
                _exclusion("metric_values", metric, "", "", "missing_value", missing, valid)
            )
        if non_finite:
            output.append(
                _exclusion("metric_values", metric, "", "", "non_finite_value", non_finite, valid)
            )
    return output


def _warnings(
    *,
    rows: Sequence[Mapping[str, Any]],
    independent_request_count: int,
    repeated_requests: Sequence[str],
    eligible_repeat_group_count: int,
    lineage_complete: bool,
) -> list[str]:
    warnings = [
        "Results are descriptive and exploratory; repeated rows are not independent "
        "population samples."
    ]
    if independent_request_count < MINIMUM_INDEPENDENT_REQUESTS:
        warnings.append(
            f"{independent_request_count} independent requests do not support formal "
            f"population inference; the configured minimum is {MINIMUM_INDEPENDENT_REQUESTS}."
        )
    if repeated_requests:
        repeated_geographies = sorted(
            {str(row["geography"]) for row in rows if str(row["request_id"]) in repeated_requests}
        )
        request_noun = "request" if len(repeated_requests) == 1 else "requests"
        warnings.append(
            f"Repeat diagnostics come from {len(repeated_requests)} repeated {request_noun} "
            f"in geographies {repeated_geographies}."
        )
        warnings.append(
            f"{eligible_repeat_group_count} request/concept pairs are nested within "
            f"{len(repeated_requests)} repeated {request_noun}; they are not independent "
            "repeated requests."
        )
    else:
        warnings.append("No repeated requests are available for paired diagnostics.")
    warnings.append("General repeat reliability is not established.")
    if not lineage_complete:
        warnings.append(
            "Integrated lineage is incomplete and must be repaired before analysis claims."
        )
    warnings.append("No p-values, bootstrap intervals, permutation tests, or ICC were calculated.")
    return warnings


def _lineage_complete(rows: Sequence[Mapping[str, Any]]) -> bool:
    status_fields = (
        "plan_join_status",
        "repeat_metrics_join_status",
        "episode_metrics_join_status",
    )
    return all(field in row and row[field] == "matched" for row in rows for field in status_fields)


def _request_concept_groups(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], list[Mapping[str, Any]]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].append(row)
    return grouped


def _repeat_groups(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], set[str]]:
    grouped: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in rows:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].add(str(row["repeat_id"]))
    return grouped


def _stats(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return dict.fromkeys(
            (
                "minimum",
                "maximum",
                "mean",
                "median",
                "sample_standard_deviation",
                "first_quartile",
                "third_quartile",
                "interquartile_range",
            ),
            None,
        )
    ordered = sorted(values)
    quartiles = (
        statistics.quantiles(ordered, n=4, method="inclusive")
        if len(ordered) >= PAIR_SIZE
        else [ordered[0]] * 3
    )
    return {
        "minimum": ordered[0],
        "maximum": ordered[-1],
        "mean": statistics.mean(ordered),
        "median": statistics.median(ordered),
        "sample_standard_deviation": (
            statistics.stdev(ordered) if len(ordered) >= PAIR_SIZE else None
        ),
        "first_quartile": quartiles[0],
        "third_quartile": quartiles[2],
        "interquartile_range": quartiles[2] - quartiles[0],
    }


def _mean(values: Sequence[float]) -> float | None:
    return statistics.mean(values) if values else None


def _median(values: Sequence[float]) -> float | None:
    return statistics.median(values) if values else None


def _is_finite(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _exclusion(
    component: str,
    metric: str,
    request_id: str,
    concept_id: str,
    reason: str,
    missing: int,
    valid: int,
) -> dict[str, Any]:
    return {
        "analysis_component": component,
        "metric": metric,
        "request_id": request_id,
        "concept_id": concept_id,
        "reason": reason,
        "missing_count": missing,
        "valid_count": valid,
    }


def _sort_rows(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> list[dict[str, Any]]:
    return sorted(
        (dict(row) for row in rows),
        key=lambda row: tuple(str(row.get(field, "")) for field in fields),
    )


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]], schema: pa.Schema) -> None:
    try:
        table = pa.Table.from_pylist(list(rows), schema=schema)
    except (pa.ArrowException, TypeError, ValueError, OverflowError) as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_output_build_error: path={path} detail={exc}"
        ) from exc
    try:
        pq.write_table(table, path, compression="zstd")
    except (OSError, pa.ArrowException) as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_output_write_error: path={path} detail={exc}"
        ) from exc


def _write_exclusions(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    try:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=EXCLUSION_FIELDS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    except (OSError, csv.Error, TypeError, ValueError) as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_output_write_error: path={path} detail={exc}"
        ) from exc


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    try:
        serialized = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    except (TypeError, ValueError, OverflowError) as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_output_build_error: path={path} detail={exc}"
        ) from exc
    try:
        path.write_text(serialized, encoding="utf-8")
    except OSError as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_output_write_error: path={path} detail={exc}"
        ) from exc


def _dataset_version(rows: Sequence[Mapping[str, Any]]) -> str:
    versions = sorted(
        {str(row.get("schema_version", "")) for row in rows if row.get("schema_version")}
    )
    return versions[0] if len(versions) == 1 else ANALYSIS_DATASET_VERSION


def _byte_count(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_input_read_error: path={path} detail={exc}"
        ) from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise DescriptiveAnalysisError(
            f"descriptive_input_read_error: path={path} detail={exc}"
        ) from exc
    return digest.hexdigest()
