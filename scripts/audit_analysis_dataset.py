from __future__ import annotations

import argparse
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
import yaml

GRAIN = ("request_id", "repeat_id", "concept_id")
GROUP_FIELDS = (
    "request_id",
    "episode_id",
    "geography",
    "concept_id",
    "terminology_version",
    "request_role",
    "semantic_family",
)
IDENTIFIERS = ("request_id", "repeat_id", "concept_id", "episode_id", "geography")
SEED = 20260721
BOOTSTRAP_ITERATIONS = 10_000
PERMUTATION_ITERATIONS = 10_000
MINIMUM_REPEATS = 2
MINIMUM_INDEPENDENT_REQUESTS = 20


class AuditError(ValueError):
    """Raised when the statistical design audit cannot safely proceed."""


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit a Milestone 1.0A integrated dataset.")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--terms",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "config" / "trends_terms.yaml",
    )
    args = parser.parse_args()
    audit_dataset(args.dataset, args.output_dir, args.terms)
    return 0


def audit_dataset(dataset_path: Path, output_dir: Path, terms_path: Path) -> dict[str, Path]:
    """Write deterministic, descriptive design-audit artifacts."""
    integrated_dir = dataset_path.parent
    join_path = integrated_dir / "join_summary.json"
    unmatched_path = integrated_dir / "unmatched_keys.csv"
    provenance_path = integrated_dir / "provenance.json"
    for path in (dataset_path, join_path, unmatched_path, provenance_path, terms_path):
        if not path.exists():
            raise AuditError(f"audit_input_missing: path={path}")
    try:
        table = pq.read_table(dataset_path)
        join_summary = json.loads(join_path.read_text(encoding="utf-8"))
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        terms_payload = yaml.safe_load(terms_path.read_text(encoding="utf-8"))
        with unmatched_path.open(encoding="utf-8", newline="") as handle:
            unmatched = list(csv.DictReader(handle))
    except (OSError, UnicodeError, json.JSONDecodeError, yaml.YAMLError, pa.ArrowException) as exc:
        raise AuditError(f"audit_input_read_error: detail={exc}") from exc

    missing_grain = sorted(set(GRAIN) - set(table.column_names))
    if missing_grain:
        raise AuditError(f"audit_missing_grain_fields: missing={missing_grain}")
    rows = cast(list[dict[str, Any]], table.to_pylist())
    grain_counts = Counter(tuple(str(row[field]) for field in GRAIN) for row in rows)
    duplicates = sorted(key for key, count in grain_counts.items() if count > 1)
    if duplicates:
        raise AuditError(
            "audit_duplicate_analytical_grain: "
            f"count={sum(grain_counts[key] - 1 for key in duplicates)} examples={duplicates[:20]}"
        )

    column_audit = _column_audit(table)
    metric_viability = _metric_viability(table, rows)
    group_coverage = _group_coverage(rows)
    repeat_coverage = _repeat_coverage(rows)
    eligible_repeat_groups = [
        row for row in repeat_coverage if int(row["repeat_count"]) >= MINIMUM_REPEATS
    ]
    repeated_requests = sorted({str(row["request_id"]) for row in eligible_repeat_groups})
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "dataset_column_audit": output_dir / "dataset_column_audit.csv",
        "metric_viability": output_dir / "metric_viability.csv",
        "group_coverage": output_dir / "group_coverage.csv",
        "repeat_coverage": output_dir / "repeat_coverage.csv",
        "statistical_design_summary": output_dir / "statistical_design_summary.json",
    }
    _write_csv(paths["dataset_column_audit"], column_audit)
    _write_csv(paths["metric_viability"], metric_viability)
    _write_csv(paths["group_coverage"], group_coverage)
    _write_csv(paths["repeat_coverage"], repeat_coverage)

    descriptive = [row["metric"] for row in metric_viability if row["descriptive"] == "yes"]
    repeat_metrics = [
        row["metric"]
        for row in metric_viability
        if row["repeat_reliability"] == "exploratory_pairwise_only"
    ]
    ineligible = {
        row["metric"]: row["ineligibility_reasons"].split(";")
        for row in metric_viability
        if row["ineligibility_reasons"]
    }
    concepts = sorted({str(row["concept_id"]) for row in rows})
    episodes = sorted({str(row["episode_id"]) for row in rows})
    geographies = sorted({str(row["geography"]) for row in rows})
    requests = sorted({str(row["request_id"]) for row in rows})
    repeats = sorted({str(row["repeat_id"]) for row in rows})
    terminology = _terminology_summary(terms_payload, concepts)
    summary = {
        "audit_version": "1.0B-design-audit-v2",
        "dataset": {
            "path": str(dataset_path),
            "sha256": _sha256(dataset_path),
            "row_count": table.num_rows,
            "column_count": table.num_columns,
            "analytical_grain": list(GRAIN),
            "duplicate_grain_count": 0,
            "identifier_uniqueness": {
                field: len({str(row[field]) for row in rows}) for field in IDENTIFIERS
            },
        },
        "coverage": {
            "request_ids": requests,
            "repeat_ids": repeats,
            "concept_ids": concepts,
            "episode_ids": episodes,
            "geographies": geographies,
            "terminology_versions": sorted({str(row["terminology_version"]) for row in rows}),
            "eligible_reliability_group_count": len(eligible_repeat_groups),
            "repeat_eligible_request_concept_group_count": len(eligible_repeat_groups),
            "repeated_request_count": len(repeated_requests),
            "repeated_request_ids": repeated_requests,
            "repeat_coverage_interpretation": (
                f"{len(eligible_repeat_groups)} concept groups nested within "
                f"{len(repeated_requests)} repeated request(s)"
            ),
            "independent_request_count": len(requests),
            "independent_episode_count": len(episodes),
            "independent_geography_count": len(geographies),
        },
        "lineage": {
            "inputs": provenance.get("inputs", {}),
            "join_summary": join_summary,
            "unmatched_record_count": len(unmatched),
            "unmatched_by_source_and_direction": _unmatched_counts(unmatched),
        },
        "terminology": terminology,
        "recommendations": {
            "overall_implementation_recommendation": (
                "conditional_go_descriptive_repeat_only_no_go_formal_inference"
            ),
            "current_batch_inference_status": "no_go_formal_inference_exploratory_only",
            "viable_metrics": {
                "descriptive": descriptive,
                "repeat_agreement_exploratory": repeat_metrics,
                "bootstrap_current_batch": [],
                "permutation_current_batch": [],
                "bootstrap_after_independent_expansion": [
                    "absolute_peak_lift",
                    "peak_lead_lag_days",
                    "zero_fraction",
                    "anticipatory_peak_lift",
                    "immediate_peak_lift",
                    "early_recovery_peak_lift",
                    "extended_recovery_peak_lift",
                ],
                "permutation_after_matched_control_expansion": [
                    "anticipatory_standardized_lift",
                    "immediate_standardized_lift",
                    "early_recovery_standardized_lift",
                    "extended_recovery_standardized_lift",
                ],
            },
            "ineligible_metrics": ineligible,
            "independent_resampling_unit": (
                "request_id (cluster); episode_id above request when multiple requests "
                "per episode exist"
            ),
            "eligible_reliability_groups": [
                {"request_id": row["request_id"], "concept_id": row["concept_id"]}
                for row in eligible_repeat_groups
            ],
            "eligible_reliability_repeated_request_count": len(repeated_requests),
            "recommended_statistical_tests": [
                {
                    "name": "paired_repeat_agreement_descriptive",
                    "status": "available_for_one_repeated_request_exploratory_only",
                    "general_repeat_reliability": "not_established",
                    "statistics": [
                        "absolute_difference",
                        "relative_difference_when_denominator_nonzero",
                        "within_group_standard_deviation",
                        "rank_consistency_across_concepts",
                    ],
                },
                {
                    "name": "cluster_bootstrap_request_level_estimand",
                    "status": "not_available_current_batch",
                },
                {
                    "name": "paired_role_or_control_permutation",
                    "status": "not_available_current_batch",
                },
            ],
            "minimum_sample_thresholds": {
                "repeat_agreement_repeats_per_request_concept": 2,
                "icc_independent_request_concept_groups": 20,
                "population_inference_independent_requests_per_metric": (
                    MINIMUM_INDEPENDENT_REQUESTS
                ),
                "bootstrap_independent_requests_per_estimand": MINIMUM_INDEPENDENT_REQUESTS,
                "permutation_paired_independent_units": 10,
                "warn_sparse_independent_units_below": 30,
            },
            "deterministic_seed": SEED,
            "iteration_counts": {
                "bootstrap": BOOTSTRAP_ITERATIONS,
                "permutation": PERMUTATION_ITERATIONS,
            },
            "required_upstream_additions": _required_upstream_additions(
                join_summary=join_summary,
                unmatched_record_count=len(unmatched),
                independent_request_count=len(requests),
            ),
        },
        "exclusions": {
            row["metric"]: {
                "missing": int(row["missing_count"]),
                "non_finite": int(row["non_finite_count"]),
            }
            for row in metric_viability
        },
    }
    _write_json(paths["statistical_design_summary"], summary)
    return paths


def _column_audit(table: pa.Table) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for field in table.schema:
        values = table[field.name].to_pylist()
        non_null = [value for value in values if value is not None]
        numeric = pa.types.is_integer(field.type) or pa.types.is_floating(field.type)
        finite = sum(_finite(value) for value in non_null) if numeric else None
        output.append(
            {
                "column": field.name,
                "data_type": str(field.type),
                "row_count": len(values),
                "non_null_count": len(non_null),
                "null_count": len(values) - len(non_null),
                "null_percentage": _format_number(
                    100.0 * (len(values) - len(non_null)) / len(values) if values else 0.0
                ),
                "unique_count": len({_stable_value(value) for value in non_null}),
                "minimum": _stable_value(min(non_null)) if non_null else "",
                "maximum": _stable_value(max(non_null)) if non_null else "",
                "finite_count": finite if finite is not None else "",
                "non_finite_count": len(non_null) - finite if finite is not None else "",
            }
        )
    return output


def _metric_viability(table: pa.Table, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for field in table.schema:
        if not (
            pa.types.is_integer(field.type)
            or pa.types.is_floating(field.type)
            or pa.types.is_boolean(field.type)
        ):
            continue
        values = [row.get(field.name) for row in rows]
        numeric_values = [float(value) for value in values if value is not None and _finite(value)]
        represented = [
            row for row in rows if row.get(field.name) is not None and _finite(row[field.name])
        ]
        repeat_groups: dict[tuple[str, str], set[str]] = defaultdict(set)
        for row in represented:
            repeat_groups[(str(row["request_id"]), str(row["concept_id"]))].add(
                str(row["repeat_id"])
            )
        independent_requests = {str(row["request_id"]) for row in represented}
        independent_episodes = {str(row["episode_id"]) for row in represented}
        independent_geographies = {str(row["geography"]) for row in represented}
        metric_type = _metric_type(field.name, field.type)
        reasons: list[str] = []
        if not numeric_values:
            reasons.append("no_finite_observations")
        if len(set(numeric_values)) <= 1:
            reasons.append("constant_or_single_value")
        if sum(value is None for value in values) > len(values) / 2:
            reasons.append("more_than_50_percent_missing")
        if metric_type in {"count_like", "flag_derived"}:
            reasons.append(f"{metric_type}_not_primary_response")
        repeat_eligible = sum(
            len(repeat_ids) >= MINIMUM_REPEATS for repeat_ids in repeat_groups.values()
        )
        repeated_requests = {
            request_id
            for (request_id, _concept_id), repeat_ids in repeat_groups.items()
            if len(repeat_ids) >= MINIMUM_REPEATS
        }
        inference_reason = _population_inference_reason(len(independent_requests))
        if inference_reason:
            reasons.append(inference_reason)
        exploratory_repeat = bool(
            numeric_values
            and repeat_eligible
            and len(set(numeric_values)) > 1
            and metric_type not in {"count_like", "flag_derived"}
        )
        output.append(
            {
                "metric": field.name,
                "data_type": str(field.type),
                "metric_character": metric_type,
                "valid_observation_count": len(numeric_values),
                "missing_count": sum(value is None for value in values),
                "finite_count": len(numeric_values),
                "non_finite_count": sum(
                    value is not None and not _finite(value) for value in values
                ),
                "independent_request_count": len(independent_requests),
                "independent_episode_count": len(independent_episodes),
                "independent_geography_count": len(independent_geographies),
                "concept_count": len({str(row["concept_id"]) for row in represented}),
                "repeated_request_count": len(repeated_requests),
                "repeat_eligible_request_concept_group_count": repeat_eligible,
                "minimum": _stat(numeric_values, "min"),
                "maximum": _stat(numeric_values, "max"),
                "mean": _stat(numeric_values, "mean"),
                "median": _stat(numeric_values, "median"),
                "standard_deviation": _stat(numeric_values, "stdev"),
                "interquartile_range": _stat(numeric_values, "iqr"),
                "zero_fraction": _format_number(
                    sum(value == 0 for value in numeric_values) / len(numeric_values)
                    if numeric_values
                    else None
                ),
                "positive_count": sum(value > 0 for value in numeric_values),
                "negative_count": sum(value < 0 for value in numeric_values),
                "descriptive": "yes" if numeric_values else "no",
                "repeat_reliability": (
                    "exploratory_pairwise_only" if exploratory_repeat else "ineligible"
                ),
                "population_inference": (
                    "cluster_count_threshold_met" if not inference_reason else "ineligible"
                ),
                "bootstrap": "ineligible_current_batch_design_not_established",
                "permutation": "ineligible_current_batch_no_exchangeable_units",
                "ineligibility_reasons": ";".join(reasons),
            }
        )
    return output


def _population_inference_reason(independent_request_count: int) -> str:
    if independent_request_count >= MINIMUM_INDEPENDENT_REQUESTS:
        return ""
    return (
        f"insufficient_independent_requests_{independent_request_count}_below_"
        f"{MINIMUM_INDEPENDENT_REQUESTS}"
    )


def _required_upstream_additions(
    *,
    join_summary: Mapping[str, Any],
    unmatched_record_count: int,
    independent_request_count: int,
) -> list[str]:
    additions: list[str] = []
    if independent_request_count < MINIMUM_INDEPENDENT_REQUESTS:
        additions.append(
            f"at least {MINIMUM_INDEPENDENT_REQUESTS} independent request clusters per "
            f"estimand (currently {independent_request_count})"
        )
    additions.extend(
        [
            "explicit matched treated/comparison request pairs",
            "pre-specified negative-control concepts or event windows",
        ]
    )
    if unmatched_record_count or join_summary.get("all_required_joins_matched") is not True:
        additions.append("completed request-plan and metric joins for analyzed rows")
    return additions


def _group_coverage(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for field in GROUP_FIELDS:
        for value in sorted({str(row.get(field) or "") for row in rows}):
            selected = [row for row in rows if str(row.get(field) or "") == value]
            selected_repeat_groups: dict[tuple[str, str], set[str]] = defaultdict(set)
            for row in selected:
                selected_repeat_groups[(str(row["request_id"]), str(row["concept_id"]))].add(
                    str(row["repeat_id"])
                )
            eligible_groups = {
                key
                for key, repeat_ids in selected_repeat_groups.items()
                if len(repeat_ids) >= MINIMUM_REPEATS
            }
            output.append(
                {
                    "grouping_dimension": field,
                    "group_value": value,
                    "row_count": len(selected),
                    "request_count": len({str(row["request_id"]) for row in selected}),
                    "episode_count": len({str(row["episode_id"]) for row in selected}),
                    "geography_count": len({str(row["geography"]) for row in selected}),
                    "concept_count": len({str(row["concept_id"]) for row in selected}),
                    "repeat_count": len({str(row["repeat_id"]) for row in selected}),
                    "request_concept_group_count": len(
                        {(str(row["request_id"]), str(row["concept_id"])) for row in selected}
                    ),
                    "repeated_request_count": len(
                        {request_id for request_id, _concept_id in eligible_groups}
                    ),
                    "repeat_eligible_request_concept_group_count": len(eligible_groups),
                }
            )
    return output


def _repeat_coverage(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].append(row)
    return [
        {
            "request_id": request_id,
            "concept_id": concept_id,
            "episode_id": str(group[0]["episode_id"]),
            "geography": str(group[0]["geography"]),
            "repeat_count": len({str(row["repeat_id"]) for row in group}),
            "repeat_ids": ";".join(sorted({str(row["repeat_id"]) for row in group})),
            "row_count": len(group),
            "repeated_request": (
                "yes" if len({str(row["repeat_id"]) for row in group}) >= MINIMUM_REPEATS else "no"
            ),
            "nesting_interpretation": (
                "concept_nested_within_repeated_request"
                if len({str(row["repeat_id"]) for row in group}) >= MINIMUM_REPEATS
                else "single_export_request_concept"
            ),
            "eligible_for_pairwise_repeat_agreement": (
                "yes" if len({str(row["repeat_id"]) for row in group}) >= MINIMUM_REPEATS else "no"
            ),
        }
        for (request_id, concept_id), group in sorted(grouped.items())
    ]


def _terminology_summary(payload: Any, present_concepts: Sequence[str]) -> dict[str, Any]:
    concepts = payload.get("concepts", []) if isinstance(payload, dict) else []
    rows = [row for row in concepts if isinstance(row, dict)]
    configured = {str(row.get("concept_id")): row for row in rows}
    return {
        "configured_concept_count": len(configured),
        "configured_semantic_family_counts": dict(
            sorted(Counter(str(row.get("semantic_family")) for row in rows).items())
        ),
        "configured_proxy_type_counts": dict(
            sorted(Counter(str(row.get("proxy_type")) for row in rows).items())
        ),
        "dataset_concepts": [
            {
                "concept_id": concept,
                "semantic_family": configured.get(concept, {}).get("semantic_family"),
                "proxy_type": configured.get(concept, {}).get("proxy_type"),
                "query_type": configured.get(concept, {}).get("query_type"),
            }
            for concept in present_concepts
        ],
    }


def _unmatched_counts(rows: Sequence[Mapping[str, str]]) -> list[dict[str, Any]]:
    counts = Counter((row.get("source", ""), row.get("direction", "")) for row in rows)
    return [
        {"source": source, "direction": direction, "count": count}
        for (source, direction), count in sorted(counts.items())
    ]


def _metric_type(name: str, data_type: pa.DataType) -> str:
    if pa.types.is_boolean(data_type):
        return "flag_derived"
    if name == "zero_fraction":
        return "bounded_0_1"
    if pa.types.is_integer(data_type) and name != "peak_lead_lag_days":
        return "count_like"
    return "continuous"


def _stat(values: Sequence[float], operation: str) -> str:
    if not values:
        return ""
    result: float | None
    if operation == "min":
        result = min(values)
    elif operation == "max":
        result = max(values)
    elif operation == "mean":
        result = statistics.fmean(values)
    elif operation == "median":
        result = statistics.median(values)
    elif operation == "stdev":
        result = statistics.stdev(values) if len(values) >= MINIMUM_REPEATS else None
    else:
        quartiles = (
            statistics.quantiles(values, n=4, method="inclusive")
            if len(values) >= MINIMUM_REPEATS
            else []
        )
        result = quartiles[2] - quartiles[0] if quartiles else None
    return _format_number(result)


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _format_number(value: float | int | None) -> str:
    if value is None:
        return ""
    return format(float(value), ".12g")


def _stable_value(value: Any) -> str:
    if isinstance(value, float):
        return _format_number(value)
    return str(value)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise AuditError(f"audit_empty_output: path={path}")
    fields = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
