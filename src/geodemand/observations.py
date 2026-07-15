from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_SEED = 20260715
WET_HOUR_MM = 1.0
MIN_CORRELATION_PAIRS = 3
RULE_VERSION = "0.7B-v1"
TYPICAL_MIN_MEMBERS = 2
TYPICAL_MAX_MEMBERS = 10
ZERO_DENOMINATOR_EPSILON = 1e-9


class ObservationsError(ValueError):
    """Raised when multi-source observations cannot be processed."""


def pilot_sample(
    verification_sample: Path,
    output_dir: Path,
    seed: int = DEFAULT_SEED,
    target_count: int = 30,
) -> dict[str, Path]:
    rows = _read_rows(verification_sample)
    if target_count < 1:
        raise ObservationsError("target_count must be at least 1")
    if not rows:
        raise ObservationsError(f"Verification sample contains no episodes: {verification_sample}")
    selected: dict[str, dict[str, Any]] = {}

    def add(candidates: list[dict[str, Any]], label: str, limit: int) -> None:
        count = 0
        for row in candidates:
            episode_id = str(row["episode_id"])
            if episode_id in selected:
                continue
            selected[episode_id] = {**row, "pilot_selection_reason": label}
            count += 1
            if count >= limit or len(selected) >= target_count:
                break

    add(
        sorted(rows, key=lambda row: (-int(row["member_count"]), str(row["episode_id"]))),
        "largest",
        5,
    )
    add(
        _stable_shuffle(
            [
                row
                for row in rows
                if int(row.get("additional_balanced_members", 0) or 0) > 0
                or row.get("selection_stratum") == "balanced_merge_candidate"
            ],
            seed,
        ),
        "balanced_merge",
        5,
    )
    add(
        _stable_shuffle(
            [
                row
                for row in rows
                if TYPICAL_MIN_MEMBERS <= int(row["member_count"]) <= TYPICAL_MAX_MEMBERS
            ],
            seed,
        ),
        "typical_multi_member",
        5,
    )
    add(
        _stable_shuffle([row for row in rows if int(row["member_count"]) == 1], seed),
        "singleton",
        5,
    )
    add(
        _stable_shuffle(
            [row for row in rows if row.get("selection_stratum") == "split_boundary"], seed
        ),
        "split_boundary",
        5,
    )
    add(_coverage_fill_order(rows, selected, seed), "coverage_fill", target_count)
    pilot_rows = sorted(selected.values(), key=lambda row: str(row["episode_id"]))[:target_count]
    output_dir.mkdir(parents=True, exist_ok=True)
    sample_path = output_dir / "pilot_verification_sample.parquet"
    summary_path = output_dir / "pilot_sample_summary.json"
    manifest_path = output_dir / "pilot_sample_manifest.json"
    _write_parquet(sample_path, pilot_rows)
    _write_json(
        summary_path,
        {
            "pilot_episode_count": len(pilot_rows),
            "selection_counts": dict(Counter(row["pilot_selection_reason"] for row in pilot_rows)),
            "year_counts": dict(Counter(str(row["episode_start_date"])[:4] for row in pilot_rows)),
            "season_counts": dict(Counter(_season(row) for row in pilot_rows)),
            "state_counts": dict(
                sorted(Counter(state for row in pilot_rows for state in _state_values(row)).items())
            ),
            "region_counts": dict(sorted(Counter(_region(row) for row in pilot_rows).items())),
        },
    )
    _write_json(
        manifest_path,
        {
            "seed": seed,
            "target_count": target_count,
            "input_sample_sha256": _sha256(verification_sample),
            "pilot_sample_sha256": _sha256(sample_path),
            "selection_algorithm": "stratified-quota-then-coverage-v1",
        },
    )
    return {
        "pilot_verification_sample": sample_path,
        "pilot_sample_summary": summary_path,
        "pilot_sample_manifest": manifest_path,
    }


def compare_precipitation(
    mrms_metrics: Path,
    mrms_timeseries: Path,
    imerg_metrics: Path,
    imerg_timeseries: Path,
    verification_sample: Path,
    output_dir: Path,
    wet_hour_mm: float = WET_HOUR_MM,
) -> dict[str, Path]:
    del mrms_metrics, imerg_metrics, verification_sample
    mrms_rows = _read_rows(mrms_timeseries)
    imerg_rows = _read_rows(imerg_timeseries)
    mrms_by_episode = _series_by_episode(mrms_rows, "area_mean_qpe_mm")
    imerg_by_episode = _series_by_episode(imerg_rows, "area_mean_imerg_mm")
    comparison_rows = [
        _comparison_row(
            episode_id,
            mrms_by_episode.get(episode_id, {}),
            imerg_by_episode.get(episode_id, {}),
            wet_hour_mm,
        )
        for episode_id in sorted(set(mrms_by_episode) | set(imerg_by_episode))
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    comparison_path = output_dir / "mrms_imerg_episode_comparison.parquet"
    summary_path = output_dir / "mrms_imerg_comparison_summary.json"
    _write_parquet(comparison_path, comparison_rows)
    adequate = [row for row in comparison_rows if not row["insufficient_overlap_flag"]]
    _write_json(
        summary_path,
        {
            "episode_count_compared": len(comparison_rows),
            "adequate_comparison_count": len(adequate),
            "median_bias_mm": _median([row["mean_bias_mm"] for row in adequate]),
            "median_absolute_error_mm": _median(
                [row["mean_absolute_error_mm"] for row in adequate]
            ),
            "wet_hour_threshold_mm": wet_hour_mm,
        },
    )
    return {
        "mrms_imerg_episode_comparison": comparison_path,
        "mrms_imerg_comparison_summary": summary_path,
    }


def assess_observations(
    sample_path: Path,
    mrms_metrics: Path,
    imerg_metrics: Path,
    precipitation_comparison: Path,
    usgs_summary: Path,
    output_dir: Path,
    rules: Mapping[str, Any] | None = None,
) -> dict[str, Path]:
    active_rules = dict(rules or _default_rules())
    sample = {str(row["episode_id"]): row for row in _read_rows(sample_path)}
    mrms = {str(row["episode_id"]): row for row in _read_rows(mrms_metrics)}
    imerg = {str(row["episode_id"]): row for row in _read_rows(imerg_metrics)}
    comparison = {str(row["episode_id"]): row for row in _read_rows(precipitation_comparison)}
    usgs = {str(row["episode_id"]): row for row in _read_rows(usgs_summary)}
    rows = [
        _assessment_row(
            episode_id,
            sample.get(episode_id, {}),
            mrms.get(episode_id),
            imerg.get(episode_id),
            comparison.get(episode_id),
            usgs.get(episode_id),
            active_rules,
        )
        for episode_id in sorted(set(sample) | set(mrms) | set(imerg) | set(usgs))
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    assessment_path = output_dir / "multisource_episode_assessment.parquet"
    rules_path = output_dir / "assessment_rules.json"
    _write_parquet(assessment_path, rows)
    _write_json(rules_path, active_rules)
    return {"multisource_episode_assessment": assessment_path, "assessment_rules": rules_path}


def write_quicklooks(
    assessment_path: Path, output_dir: Path, max_quicklooks: int = 7
) -> dict[str, Path]:
    rows = _read_rows(assessment_path)[:max_quicklooks]
    quicklook_dir = output_dir / "quicklooks"
    quicklook_dir.mkdir(parents=True, exist_ok=True)
    manifest_rows = []
    for row in rows:
        filename = f"{row['episode_id']}_{row['integrated_evidence_category']}.txt"
        path = quicklook_dir / filename
        path.write_text(
            "\n".join(
                [
                    f"episode_id={row['episode_id']}",
                    f"category={row['integrated_evidence_category']}",
                    f"review={row['integrated_review_flag']}",
                    "deterministic_quicklook_placeholder=true",
                ]
            ),
            encoding="utf-8",
        )
        manifest_rows.append({"episode_id": row["episode_id"], "quicklook_path": str(path)})
    manifest_path = quicklook_dir / "quicklook_manifest.csv"
    _write_csv(manifest_path, manifest_rows, ["episode_id", "quicklook_path"])
    return {"quicklook_manifest": manifest_path}


def _comparison_row(
    episode_id: str,
    mrms: Mapping[str, float],
    imerg: Mapping[str, float],
    wet_hour_mm: float,
) -> dict[str, Any]:
    common_hours = sorted(set(mrms) & set(imerg))
    paired = [(mrms[hour], imerg[hour]) for hour in common_hours]
    if not paired:
        return {
            "episode_id": episode_id,
            "common_hour_count": 0,
            "common_coverage_fraction": 0.0,
            "insufficient_overlap_flag": True,
            "comparison_review_reason": "no_common_hours",
        }
    mrms_values = [pair[0] for pair in paired]
    imerg_values = [pair[1] for pair in paired]
    diffs = [left - right for left, right in paired]
    wet_counts = _wet_counts(mrms_values, imerg_values, wet_hour_mm)
    return {
        "episode_id": episode_id,
        "common_hour_count": len(paired),
        "common_coverage_fraction": len(paired) / max(len(set(mrms) | set(imerg)), 1),
        "mrms_total_area_mean_mm": sum(mrms_values),
        "imerg_total_area_mean_mm": sum(imerg_values),
        "total_difference_mm": sum(mrms_values) - sum(imerg_values),
        "total_ratio": _stable_ratio(sum(mrms_values), sum(imerg_values)),
        "mean_bias_mm": sum(diffs) / len(diffs),
        "mean_absolute_error_mm": sum(abs(value) for value in diffs) / len(diffs),
        "root_mean_square_error_mm": math.sqrt(sum(value**2 for value in diffs) / len(diffs)),
        "pearson_correlation": _pearson(mrms_values, imerg_values),
        "spearman_correlation": _pearson(_ranks(mrms_values), _ranks(imerg_values)),
        "mrms_peak_hour_utc": common_hours[mrms_values.index(max(mrms_values))],
        "imerg_peak_hour_utc": common_hours[imerg_values.index(max(imerg_values))],
        "peak_time_difference_hours": imerg_values.index(max(imerg_values))
        - mrms_values.index(max(mrms_values)),
        "mrms_max_hourly_mm": max(mrms_values),
        "imerg_max_hourly_mm": max(imerg_values),
        "maximum_hourly_difference_mm": max(abs(value) for value in diffs),
        "spatial_pattern_correlation": None,
        "comparison_spatial_method": "episode_area_summary",
        "insufficient_overlap_flag": len(paired) < MIN_CORRELATION_PAIRS,
        "comparison_review_reason": ""
        if len(paired) >= MIN_CORRELATION_PAIRS
        else "too_few_common_hours",
        **wet_counts,
    }


def _assessment_row(
    episode_id: str,
    sample: Mapping[str, Any],
    mrms: Mapping[str, Any] | None,
    imerg: Mapping[str, Any] | None,
    comparison: Mapping[str, Any] | None,
    usgs: Mapping[str, Any] | None,
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    mrms_available = mrms is not None
    imerg_available = imerg is not None
    usgs_available = usgs is not None and usgs.get("no_gauge_reason", "") == ""
    mrms_signal = (
        mrms is not None
        and float(mrms.get("coverage_fraction", 0.0)) >= rules["min_precip_coverage"]
    )
    imerg_signal = (
        imerg is not None
        and float(imerg.get("coverage_fraction", 0.0)) >= rules["min_precip_coverage"]
    )
    timing = comparison is not None and not bool(comparison.get("insufficient_overlap_flag", True))
    usgs_support = str(usgs.get("hydrologic_response_supported") if usgs else "unknown").lower()
    category = "insufficient_data"
    reasons: list[str] = []
    if mrms_signal and imerg_signal and timing and usgs_support == "true":
        category = "strong_multisource_support"
    elif (mrms_signal or imerg_signal) and usgs_support == "true":
        category = "precipitation_support_with_gauge_response"
    elif (mrms_signal or imerg_signal) and usgs_support == "unknown":
        category = "precipitation_support_no_suitable_gauge"
    elif (mrms_signal or imerg_signal) and usgs_available and usgs_support == "false":
        category = "precipitation_support_without_detected_gauge_response"
    elif usgs_support == "true":
        category = "usgs_response_with_weak_precipitation_support"
    elif comparison and abs(float(comparison.get("mean_bias_mm", 0.0))) > rules["conflict_bias_mm"]:
        category = "conflicting_precipitation_products"
    else:
        reasons.append("coverage_or_signal_insufficient")
    return {
        "episode_id": episode_id,
        "policy": "conservative",
        "selection_strata": sample.get("selection_stratum", ""),
        "member_count": sample.get("member_count"),
        "duration_days": sample.get("episode_duration_days"),
        "states": sample.get("state_codes", ""),
        "mrms_available": mrms_available,
        "imerg_available": imerg_available,
        "usgs_gauge_available": usgs_available,
        "mrms_precipitation_supported": bool(mrms_signal),
        "imerg_precipitation_supported": bool(imerg_signal),
        "mrms_imerg_timing_consistent": bool(timing),
        "mrms_imerg_amount_consistent": category != "conflicting_precipitation_products",
        "usgs_hydrologic_response_supported": usgs_support,
        "best_usgs_association_quality": usgs.get("best_usgs_association_quality", "")
        if usgs
        else "",
        "mrms_quality_adequate": bool(mrms_signal),
        "imerg_quality_adequate": bool(imerg_signal),
        "usgs_coverage_adequate": bool(usgs_available),
        "precipitation_evidence_strength": int(bool(mrms_signal)) + int(bool(imerg_signal)),
        "hydrologic_response_evidence_strength": 1 if usgs_support == "true" else 0,
        "integrated_evidence_category": category,
        "integrated_review_flag": category
        in {"insufficient_data", "conflicting_precipitation_products"},
        "integrated_review_reasons": ";".join(reasons),
    }


def _series_by_episode(rows: list[dict[str, Any]], value_field: str) -> dict[str, dict[str, float]]:
    grouped: dict[str, dict[str, float]] = {}
    for row in rows:
        grouped.setdefault(str(row["episode_id"]), {})[str(row["valid_time_utc"])] = float(
            row[value_field]
        )
    return grouped


def _wet_counts(left: Sequence[float], right: Sequence[float], threshold: float) -> dict[str, Any]:
    both_wet = sum(1 for a, b in zip(left, right, strict=True) if a >= threshold and b >= threshold)
    mrms_only = sum(1 for a, b in zip(left, right, strict=True) if a >= threshold and b < threshold)
    imerg_only = sum(
        1 for a, b in zip(left, right, strict=True) if a < threshold and b >= threshold
    )
    both_dry = sum(1 for a, b in zip(left, right, strict=True) if a < threshold and b < threshold)
    total = max(len(left), 1)
    return {
        "wet_hour_agreement": both_wet / total,
        "dry_hour_agreement": both_dry / total,
        "both_wet_hour_count": both_wet,
        "mrms_only_wet_hour_count": mrms_only,
        "imerg_only_wet_hour_count": imerg_only,
    }


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) < MIN_CORRELATION_PAIRS:
        return None
    mean_left = sum(left) / len(left)
    mean_right = sum(right) / len(right)
    numerator = sum((a - mean_left) * (b - mean_right) for a, b in zip(left, right, strict=True))
    denom_left = math.sqrt(sum((a - mean_left) ** 2 for a in left))
    denom_right = math.sqrt(sum((b - mean_right) ** 2 for b in right))
    if denom_left == 0 or denom_right == 0:
        return None
    return numerator / (denom_left * denom_right)


def _ranks(values: Sequence[float]) -> list[float]:
    order = {value: rank for rank, value in enumerate(sorted(values), start=1)}
    return [float(order[value]) for value in values]


def _stable_ratio(left: float, right: float) -> float | None:
    return None if abs(right) < ZERO_DENOMINATOR_EPSILON else left / right


def _median(values: Sequence[float | None]) -> float | None:
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return None
    return clean[len(clean) // 2]


def _stable_shuffle(rows: list[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: hashlib.sha256(f"{seed}:{row['episode_id']}".encode()).hexdigest(),
    )


def _coverage_fill_order(
    rows: list[dict[str, Any]], selected: Mapping[str, Mapping[str, Any]], seed: int
) -> list[dict[str, Any]]:
    remaining = [row for row in rows if str(row["episode_id"]) not in selected]
    covered_years = {str(row["episode_start_date"])[:4] for row in selected.values()}
    covered_seasons = {_season(row) for row in selected.values()}
    covered_states = {state for row in selected.values() for state in _state_values(row)}
    covered_regions = {_region(row) for row in selected.values()}
    ordered: list[dict[str, Any]] = []
    while remaining:
        ranked = sorted(
            remaining,
            key=lambda row: (
                -int(str(row["episode_start_date"])[:4] not in covered_years),
                -int(_season(row) not in covered_seasons),
                -sum(state not in covered_states for state in _state_values(row)),
                -int(_region(row) not in covered_regions),
                hashlib.sha256(f"{seed}:{row['episode_id']}".encode()).hexdigest(),
            ),
        )
        chosen = ranked[0]
        ordered.append(chosen)
        remaining.remove(chosen)
        covered_years.add(str(chosen["episode_start_date"])[:4])
        covered_seasons.add(_season(chosen))
        covered_states.update(_state_values(chosen))
        covered_regions.add(_region(chosen))
    return ordered


def _season(row: Mapping[str, Any]) -> str:
    month = int(str(row["episode_start_date"])[5:7])
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "fall"


def _state_values(row: Mapping[str, Any]) -> list[str]:
    return sorted(value for value in str(row.get("state_codes", "")).split(";") if value)


def _region(row: Mapping[str, Any]) -> str:
    for field in ("region", "primary_region", "census_region"):
        value = str(row.get(field, "")).strip()
        if value:
            return value
    return "unassigned"


def _default_rules() -> dict[str, Any]:
    return {
        "assessment_rule_version": RULE_VERSION,
        "min_precip_coverage": 0.7,
        "conflict_bias_mm": 25.0,
        "wet_hour_threshold_mm": WET_HOUR_MM,
    }


def _read_rows(path: Path) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


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
