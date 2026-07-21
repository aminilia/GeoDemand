from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from geodemand.trends import REGIONS, VALID_STATES, TrendsError, read_request_plan_rows

PHASES = ("anticipatory", "immediate", "early_recovery", "extended_recovery")
FLOOD_AWARENESS = {"flood", "flooding", "flash_flood", "context_heavy_rain"}
WEATHER_CONTEXT = {
    "context_weather",
    "context_storm",
    "context_rain",
    "context_heavy_rain",
}
NONFLOOD_CONTEXT = {
    "anchor_temperature",
    "context_heat",
    "context_heat_wave",
    "context_snow",
    "context_tornado",
    "context_hurricane",
}
CONTROL_CONCORDANCE_FRACTION = 0.75
STANDARDIZED_COMPARISON_SCALE = "within_series_baseline_standardized_lift"
TWO_REPEAT_COUNT = 2
EVENT_STUDY_PLAN_FIELDS = {
    "request_id",
    "episode_id",
    "geography",
    "geography_level",
    "batch_id",
    "event_start_date",
    "event_end_date",
    "baseline_start_date",
    "baseline_end_date",
    "post_start_date",
    "post_end_date",
    "terminology_version",
}


def response_phase_windows(
    event_start: date, event_end: date, rules: Mapping[str, Any]
) -> dict[str, tuple[date, date]]:
    event_rules = cast(Mapping[str, Any], rules["event_study"])
    return {
        "anticipatory": (
            event_start - timedelta(days=int(event_rules["anticipatory_days"])),
            event_start - timedelta(days=1),
        ),
        "immediate": (
            event_start,
            event_end + timedelta(days=int(event_rules["immediate_post_end_days"])),
        ),
        "early_recovery": (
            event_end + timedelta(days=int(event_rules["early_recovery_start_days"])),
            event_end + timedelta(days=int(event_rules["early_recovery_end_days"])),
        ),
        "extended_recovery": (
            event_end + timedelta(days=int(event_rules["extended_recovery_start_days"])),
            event_end + timedelta(days=int(event_rules["extended_recovery_end_days"])),
        ),
    }


def select_controls(
    pilot_path: Path,
    catalog_path: Path,
    rules_path: Path,
    output_root: Path,
    metadata_path: Path | None = None,
) -> dict[str, Path]:
    rules = _load_yaml(rules_path)
    control_rules = cast(Mapping[str, Any], rules["controls"])
    buffer_days = int(control_rules["event_buffer_days"])
    maximum = int(control_rules["maximum_controls"])
    pilot = _read_rows(pilot_path)
    catalog = _read_rows(catalog_path)
    intervals_by_state = _catalog_intervals_by_state(catalog)
    metadata = _state_metadata(metadata_path)
    selected: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for episode in sorted(pilot, key=lambda row: str(row["provisional_episode_id"])):
        episode_id = str(episode["provisional_episode_id"])
        treated = str(episode["primary_state"])
        excluded_episode_states = _states(episode)
        start = _as_date(episode["start_date"]) - timedelta(days=buffer_days)
        end = _as_date(episode["end_date"]) + timedelta(days=buffer_days)
        candidates: list[dict[str, Any]] = []
        for state in sorted(VALID_STATES - {treated}):
            reasons = []
            if state in excluded_episode_states:
                reasons.append("same_multistate_episode")
            concurrent = _state_has_episode(state, start, end, intervals_by_state, episode_id)
            if concurrent:
                reasons.append("concurrent_provisional_flood_episode")
            state_meta = metadata.get(state, {})
            trends_available = state_meta.get(
                "observed_trends_availability", state_meta.get("trends_available")
            )
            if trends_available is False:
                reasons.append("trends_data_unavailable")
            score, criteria = _control_score(treated, state, metadata, control_rules)
            metadata_complete = all(
                state_meta.get(field) not in {None, "", "unknown"}
                for field in ("census_region", "population_tier", "climate_class")
            )
            matching_status = (
                "metadata_backed_match_candidate"
                if metadata_complete and trends_available is True
                else "metadata_backed_availability_unknown"
                if metadata_complete
                else "unmatched_fallback_control"
            )
            diagnostic = {
                "treated_episode": episode_id,
                "treated_state": treated,
                "control_state": state,
                "selection_eligible": not reasons,
                "selection_criteria": ";".join(criteria),
                "exclusion_reasons": ";".join(sorted(reasons)),
                "population_tier": state_meta.get("population_tier", "unknown"),
                "census_region": state_meta.get("census_region", REGIONS.get(state, "unknown")),
                "climate_class": state_meta.get("climate_class", "unknown"),
                "temporal_flood_free_check": not concurrent,
                "flood_contamination_score": _state_episode_overlap_count(
                    state, start, end, intervals_by_state, episode_id
                ),
                "trends_availability": trends_available
                if trends_available is not None
                else "unknown",
                "selection_score": score,
                "same_region_score": float(control_rules["same_region_score"])
                if "same_census_region" in criteria
                else 0.0,
                "population_tier_score": float(control_rules["population_tier_score"])
                if "same_population_tier" in criteria
                else 0.0,
                "climate_class_score": float(control_rules["climate_class_score"])
                if "same_climate_class" in criteria
                else 0.0,
                "trends_availability_score": float(control_rules["trends_availability_score"])
                if "trends_data_available" in criteria
                else 0.0,
                "matching_quality_status": matching_status,
                "control_rank": None,
                "selected": False,
            }
            diagnostics.append(diagnostic)
            if not reasons:
                candidates.append(diagnostic)
        ranked = sorted(
            candidates, key=lambda row: (-float(row["selection_score"]), row["control_state"])
        )
        for rank, row in enumerate(ranked, 1):
            row["control_rank"] = rank
            row["candidate_control_count"] = len(ranked)
            if rank <= maximum:
                row["selected"] = True
                selected.append(dict(row))
    controls_dir = output_root / "controls"
    controls_path = controls_dir / "episode_control_states.parquet"
    diagnostics_path = controls_dir / "control_selection_diagnostics.csv"
    summary_path = controls_dir / "control_selection_summary.json"
    _write_parquet(controls_path, selected)
    _write_csv(diagnostics_path, diagnostics)
    _write_json(
        summary_path,
        {
            "pilot_episode_count": len(pilot),
            "selected_control_count": len(selected),
            "selected_controls_with_unknown_trends_availability": sum(
                row.get("trends_availability") == "unknown" for row in selected
            ),
            "candidate_controls_with_unknown_trends_availability": sum(
                row.get("selection_eligible") and row.get("trends_availability") == "unknown"
                for row in diagnostics
            ),
            "trends_availability_scoring": "not_awarded_when_unknown",
            "trends_availability_limitation": (
                "All reference values are currently unknown; no availability score is awarded."
            ),
            "episodes_with_controls": len({row["treated_episode"] for row in selected}),
            "maximum_controls_per_episode": maximum,
            "event_buffer_days": buffer_days,
            "deterministic": True,
        },
    )
    return {
        "controls": controls_path,
        "diagnostics": diagnostics_path,
        "summary": summary_path,
    }


def calculate_phase_metrics(
    observations_path: Path,
    plan_path: Path,
    terms_path: Path,
    rules_path: Path,
    output_root: Path,
) -> dict[str, Path]:
    rules = _load_yaml(rules_path)
    plans = {
        str(row["request_id"]): row
        for row in read_request_plan_rows(plan_path, required_fields=EVENT_STUDY_PLAN_FIELDS)
    }
    terms = _load_yaml(terms_path)
    term_by_id = {str(row["concept_id"]): row for row in terms["concepts"]}
    observation_rows = _read_rows(observations_path)
    grouped = _request_series(observation_rows)
    lineage = {
        (str(row["request_id"]), str(row.get("repeat_id") or "initial")): str(
            row.get("export_attempt_id") or ""
        )
        for row in observation_rows
    }
    rows = []
    for (request_id, repeat_id, concept_id), series in sorted(grouped.items()):
        plan = plans.get(request_id)
        if plan is None:
            raise TrendsError(f"Imported request is absent from plan: {request_id}")
        rows.append(
            _phase_metric_row(
                request_id,
                repeat_id,
                lineage.get((request_id, repeat_id), ""),
                concept_id,
                series,
                plan,
                term_by_id.get(concept_id, {}),
                rules,
            )
        )
    path = output_root / "processed" / "trends_phase_response_metrics.parquet"
    stability_path = output_root / "diagnostics" / "phase_repeat_stability.parquet"
    aggregate_path = output_root / "processed" / "trends_phase_response_aggregated.parquet"
    stability, aggregated = _phase_repeat_outputs(rows, rules)
    _write_parquet(path, rows)
    _write_parquet(stability_path, stability)
    _write_parquet(aggregate_path, aggregated)
    return {
        "phase_metrics": path,
        "phase_repeat_stability": stability_path,
        "phase_metrics_aggregated": aggregate_path,
    }


def calculate_concurrence(
    observations_path: Path,
    plan_path: Path,
    terms_path: Path,
    rules_path: Path,
    output_root: Path,
    phase_metrics_path: Path | None = None,
) -> dict[str, Path]:
    rules = _load_yaml(rules_path)
    event_rules = cast(Mapping[str, Any], rules["event_study"])
    plans = {
        str(row["request_id"]): row
        for row in read_request_plan_rows(plan_path, required_fields=EVENT_STUDY_PLAN_FIELDS)
    }
    observations = _read_rows(observations_path)
    series = _request_series(observations)
    terms = _load_yaml(terms_path)
    term_by_id = {str(row["concept_id"]): row for row in terms["concepts"]}
    output = []
    for (request_id, repeat_id, concept_id), demand_series in sorted(series.items()):
        term = term_by_id.get(concept_id, {})
        if term.get("semantic_family") not in {"behavioral_demand_proxy", "context"}:
            continue
        request_concepts = {
            other_id: other_series
            for (other_request, other_repeat, other_id), other_series in series.items()
            if other_request == request_id and other_repeat == repeat_id
        }
        awareness_id = _best_concurrent_series(request_concepts, FLOOD_AWARENESS)
        weather_id = _best_concurrent_series(request_concepts, WEATHER_CONTEXT - {concept_id})
        nonflood_id = _best_concurrent_series(request_concepts, NONFLOOD_CONTEXT)
        awareness = request_concepts.get(awareness_id, {}) if awareness_id else {}
        weather = request_concepts.get(weather_id, {}) if weather_id else {}
        nonflood = request_concepts.get(nonflood_id, {}) if nonflood_id else {}
        demand_peak = _peak_date(demand_series)
        awareness_peak = _peak_date(awareness)
        weather_peak = _peak_date(weather)
        nonflood_peak = _peak_date(nonflood)
        flood_difference = _day_difference(demand_peak, awareness_peak)
        weather_difference = _day_difference(demand_peak, weather_peak)
        nonflood_difference = _day_difference(demand_peak, nonflood_peak)
        output.append(
            {
                "provisional_episode_id": plans[request_id]["episode_id"],
                "episode_id": plans[request_id]["episode_id"],
                "request_id": request_id,
                "repeat_id": repeat_id,
                "request_role": plans[request_id].get("request_role", "treated_state"),
                "geography": plans[request_id]["geography"],
                "geography_level": plans[request_id]["geography_level"],
                "batch_id": plans[request_id]["batch_id"],
                "comparison_batch_id": plans[request_id].get(
                    "comparison_batch_id", plans[request_id]["batch_id"]
                ),
                "concept_id": concept_id,
                "terminology_version": plans[request_id]["terminology_version"],
                "proxy_type": term.get("proxy_type"),
                "flood_awareness_concept_id": awareness_id,
                "weather_context_concept_id": weather_id,
                "nonflood_weather_concept_id": nonflood_id,
                "flood_awareness_peak_date": _iso(awareness_peak),
                "demand_proxy_peak_date": _iso(demand_peak),
                "demand_flood_peak_difference_days": flood_difference,
                "simultaneous_flood_awareness_flag": flood_difference is not None
                and flood_difference <= int(event_rules["strong_concurrence_days"]),
                "moderate_flood_awareness_flag": flood_difference is not None
                and flood_difference <= int(event_rules["moderate_concurrence_days"]),
                "simultaneous_weather_attention_flag": weather_difference is not None
                and weather_difference <= int(event_rules["strong_concurrence_days"]),
                "simultaneous_nonflood_weather_flag": nonflood_difference is not None
                and nonflood_difference <= int(event_rules["strong_concurrence_days"]),
                "flood_awareness_correlation": _aligned_correlation(
                    demand_series,
                    awareness,
                    int(event_rules["minimum_correlation_observations"]),
                ),
                "weather_context_correlation": _aligned_correlation(
                    demand_series,
                    weather,
                    int(event_rules["minimum_correlation_observations"]),
                ),
            }
        )
    concurrence_path = output_root / "processed" / "trends_event_concurrence_metrics.parquet"
    aggregate_concurrence_path = (
        output_root / "processed" / "trends_event_concurrence_aggregated.parquet"
    )
    _write_parquet(concurrence_path, output)
    _write_parquet(aggregate_concurrence_path, _logical_concurrence_rows(output))
    phase_path = phase_metrics_path or (
        output_root / "processed" / "trends_phase_response_metrics.parquet"
    )
    phase_rows = _read_rows(phase_path)
    _, logical_phase_rows = _phase_repeat_outputs([dict(row) for row in phase_rows], rules)
    national_rows = _national_comparisons(logical_phase_rows, event_rules)
    national_path = output_root / "diagnostics" / "national_spike_diagnostics.csv"
    retailer_path = output_root / "diagnostics" / "retailer_promotion_diagnostics.csv"
    _write_csv(national_path, national_rows)
    retailer_ids = {
        str(row["concept_id"])
        for row in terms["concepts"]
        if row.get("proxy_type") == "retailer_brand"
    }
    _write_csv(
        retailer_path,
        [row for row in national_rows if str(row["concept_id"]) in retailer_ids],
    )
    return {
        "concurrence": concurrence_path,
        "concurrence_aggregated": aggregate_concurrence_path,
        "national_spikes": national_path,
        "retailer_promotions": retailer_path,
    }


def calculate_control_adjusted_metrics(
    phase_metrics_path: Path,
    controls_path: Path,
    output_root: Path,
) -> dict[str, Path]:
    metrics = _logical_phase_rows(_read_rows(phase_metrics_path))
    controls = _read_rows(controls_path)
    allowed = {(str(row["treated_episode"]), f"US-{row['control_state']}") for row in controls}
    treated = [
        row
        for row in metrics
        if row.get("request_role", "treated_state") in {"treated_state", "treated_state_comparison"}
    ]
    control_rows = [row for row in metrics if row.get("request_role") == "control_state"]
    output = []
    for row in treated:
        episode_id = str(row["provisional_episode_id"])
        matching = [
            control
            for control in control_rows
            if str(control["provisional_episode_id"]) == episode_id
            and str(control["concept_id"]) == str(row["concept_id"])
            and str(control.get("comparison_batch_id")) == str(row.get("comparison_batch_id"))
            and (episode_id, str(control["geography"])) in allowed
        ]
        for phase in PHASES:
            treated_lift = _optional_float(row.get(f"{phase}_standardized_lift"))
            values = [
                value
                for control in matching
                if (value := _optional_float(control.get(f"{phase}_standardized_lift"))) is not None
            ]
            median_control = statistics.median(values) if values else None
            adjusted = subtract_standardized_lifts(
                treated_lift,
                median_control,
                STANDARDIZED_COMPARISON_SCALE,
            )
            output.append(
                {
                    "provisional_episode_id": episode_id,
                    "episode_id": episode_id,
                    "request_id": row["request_id"],
                    "repeat_id": "aggregated_after_variability",
                    "export_attempt_id": row.get("export_attempt_id", ""),
                    "request_role": row.get("request_role", "treated_state"),
                    "concept_id": row["concept_id"],
                    "phase": phase,
                    "treated_geography": row["geography"],
                    "geography_level": row["geography_level"],
                    "batch_id": row["batch_id"],
                    "terminology_version": row["terminology_version"],
                    "comparison_batch_id": row.get("comparison_batch_id"),
                    "treated_baseline_mean": row.get("baseline_mean"),
                    "treated_event_lift": treated_lift,
                    "treated_repeat_count": int(row.get("repeat_count") or 1),
                    "treated_maximum_repeat_variability": row.get(
                        "maximum_repeat_phase_lift_stddev"
                    ),
                    "treated_repeat_stability_status": row.get("repeat_stability_status"),
                    "control_count": len(values),
                    "control_request_ids": ";".join(
                        sorted(str(control["request_id"]) for control in matching)
                    ),
                    "control_geographies": ";".join(
                        sorted(str(control["geography"]) for control in matching)
                    ),
                    "control_repeat_counts": ";".join(
                        sorted(
                            f"{control['request_id']}:{int(control.get('repeat_count') or 1)}"
                            for control in matching
                        )
                    ),
                    "maximum_control_repeat_variability": max(
                        (
                            float(control["maximum_repeat_phase_lift_stddev"])
                            for control in matching
                            if control.get("maximum_repeat_phase_lift_stddev") is not None
                        ),
                        default=None,
                    ),
                    "control_repeat_stability_statuses": ";".join(
                        sorted(
                            f"{control['request_id']}:{control.get('repeat_stability_status')}"
                            for control in matching
                        )
                    ),
                    "median_control_event_lift": median_control,
                    "control_event_lift_iqr": _iqr(values),
                    "adjusted_event_lift": adjusted,
                    "controls_concordant": _controls_concordant(values),
                    "control_quality_status": "usable"
                    if values and treated_lift is not None
                    else "insufficient_controls",
                    "comparison_scale": STANDARDIZED_COMPARISON_SCALE,
                    "aggregation_method": "logical_request_mean_after_repeat_variability",
                    "raw_index_subtraction_performed": False,
                }
            )
    path = output_root / "processed" / "trends_control_adjusted_metrics.parquet"
    _write_parquet(path, output)
    return {"control_adjusted_metrics": path}


def attribute_peaks(  # noqa: PLR0913
    phase_metrics_path: Path,
    concurrence_path: Path,
    output_root: Path,
    rules_path: Path,
    control_adjusted_path: Path | None = None,
    national_diagnostics_path: Path | None = None,
) -> dict[str, Path]:
    rules = _load_yaml(rules_path)
    attribution_rules = cast(Mapping[str, Any], rules["attribution"])
    phase_rows = _logical_phase_rows(_read_rows(phase_metrics_path))
    concurrence = {
        (str(row["request_id"]), str(row["concept_id"])): row
        for row in _logical_concurrence_rows(_read_rows(concurrence_path))
    }
    controls = _control_lookup(control_adjusted_path)
    national = _national_lookup(national_diagnostics_path)
    output = []
    retailer_diagnostics = []
    nonflood_diagnostics = []
    for phase in phase_rows:
        key = (str(phase["request_id"]), str(phase["concept_id"]))
        evidence = concurrence.get(key)
        if evidence is None:
            continue
        control = controls.get(
            (
                str(phase["provisional_episode_id"]),
                str(phase["concept_id"]),
                str(phase["dominant_event_response_phase"]),
            )
        )
        national_row = national.get(
            (
                str(phase["provisional_episode_id"]),
                str(phase["concept_id"]),
                str(phase.get("comparison_batch_id")),
            )
        )
        category, reasons = classify_peak_attribution(
            phase, evidence, control, national_row, attribution_rules
        )
        row = {
            "provisional_episode_id": phase["provisional_episode_id"],
            "episode_id": phase["provisional_episode_id"],
            "request_id": phase["request_id"],
            "repeat_id": "aggregated_after_variability",
            "export_attempt_id": phase.get("export_attempt_id", ""),
            "request_role": phase["request_role"],
            "geography": phase["geography"],
            "geography_level": phase["geography_level"],
            "batch_id": phase["batch_id"],
            "concept_id": phase["concept_id"],
            "terminology_version": phase["terminology_version"],
            "global_peak_date": phase.get("global_peak_date"),
            "global_peak_phase": phase.get("global_peak_phase"),
            "global_peak_value": phase.get("global_peak_value"),
            "dominant_event_response_phase": phase.get("dominant_event_response_phase"),
            "dominant_standardized_phase_lift": phase.get("dominant_standardized_phase_lift"),
            "dominant_robust_phase_lift": phase.get("dominant_robust_phase_lift"),
            "peak_phase": phase["peak_phase"],
            "peak_lead_lag_days": phase["peak_lead_lag_days"],
            "attribution_category": category,
            "attribution_reasons": ";".join(reasons),
            "simultaneous_flood_awareness_flag": evidence.get("simultaneous_flood_awareness_flag"),
            "simultaneous_weather_attention_flag": evidence.get(
                "simultaneous_weather_attention_flag"
            ),
            "concurrence_consensus_status": evidence.get("concurrence_consensus_status"),
            "strong_flood_concurrent_repeat_count": evidence.get(
                "strong_flood_concurrent_repeat_count"
            ),
            "strong_flood_concurrent_repeat_fraction": evidence.get(
                "strong_flood_concurrent_repeat_fraction"
            ),
            "weather_concurrent_repeat_count": evidence.get("weather_concurrent_repeat_count"),
            "weather_concurrent_repeat_fraction": evidence.get(
                "weather_concurrent_repeat_fraction"
            ),
            "any_repeat_strong_flood_concurrence": evidence.get(
                "any_repeat_strong_flood_concurrence"
            ),
            "any_repeat_weather_concurrence": evidence.get("any_repeat_weather_concurrence"),
            "any_repeat_nonflood_weather_concurrence": evidence.get(
                "any_repeat_nonflood_weather_concurrence"
            ),
            "adjusted_event_lift": control.get("adjusted_event_lift") if control else None,
            "control_quality_status": control.get("control_quality_status")
            if control
            else "missing",
            "national_concurrent_spike_flag": national_row.get("national_concurrent_spike_flag")
            if national_row
            else None,
            "national_concurrence_status": national_row.get("national_concurrence_status")
            if national_row
            else "insufficient_data",
            "metric_quality_status": phase["metric_quality_status"],
            "repeat_stability_status": phase.get("repeat_stability_status"),
            "treated_repeat_count": int(phase.get("repeat_count") or 1),
            "maximum_repeat_variability": phase.get("maximum_repeat_phase_lift_stddev"),
            "aggregation_method": phase.get("aggregation_method"),
            "national_repeat_count": national_row.get("national_repeat_count")
            if national_row
            else None,
            "national_maximum_repeat_variability": national_row.get(
                "national_maximum_repeat_variability"
            )
            if national_row
            else None,
            "control_repeat_counts": control.get("control_repeat_counts") if control else None,
            "maximum_control_repeat_variability": control.get("maximum_control_repeat_variability")
            if control
            else None,
            "noncausal_classification": True,
            "component_evidence_complete": bool(control and national_row),
        }
        output.append(row)
        if phase.get("proxy_type") == "retailer_brand":
            retailer_diagnostics.append(row)
        if category == "nonflood_weather_consistent":
            nonflood_diagnostics.append(row)
    processed_path = output_root / "processed" / "trends_peak_attribution.parquet"
    retailer_path = output_root / "diagnostics" / "retailer_promotion_diagnostics.csv"
    nonflood_path = output_root / "diagnostics" / "nonflood_weather_context.csv"
    _write_parquet(processed_path, output)
    _write_csv(retailer_path, retailer_diagnostics)
    _write_csv(nonflood_path, nonflood_diagnostics)
    return {
        "attribution": processed_path,
        "retailer_promotions": retailer_path,
        "nonflood_weather": nonflood_path,
    }


def _phase_metric_row(  # noqa: PLR0915
    request_id: str,
    repeat_id: str,
    export_attempt_id: str,
    concept_id: str,
    series: Mapping[date, float],
    plan: Mapping[str, Any],
    term: Mapping[str, Any],
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    normalization = cast(Mapping[str, Any], rules["normalization"])
    event_start = _as_date(plan["event_start_date"])
    event_end = _as_date(plan["event_end_date"])
    windows = response_phase_windows(event_start, event_end, rules)
    baseline_end = min(
        _as_date(plan["baseline_end_date"]), windows["anticipatory"][0] - timedelta(days=1)
    )
    baseline_values = _period_values(series, _as_date(plan["baseline_start_date"]), baseline_end)
    baseline_mean = statistics.fmean(baseline_values) if baseline_values else None
    baseline_sd = statistics.pstdev(baseline_values) if len(baseline_values) > 1 else None
    baseline_median = statistics.median(baseline_values) if baseline_values else None
    baseline_mad = (
        statistics.median(abs(value - baseline_median) for value in baseline_values)
        if baseline_median is not None
        else None
    )
    result: dict[str, Any] = {
        "provisional_episode_id": plan["episode_id"],
        "episode_id": plan["episode_id"],
        "request_id": request_id,
        "repeat_id": repeat_id,
        "export_attempt_id": export_attempt_id,
        "request_role": plan.get("request_role", "treated_state"),
        "treated_geography": plan.get("treated_geography", plan["geography"]),
        "geography": plan["geography"],
        "geography_level": plan["geography_level"],
        "batch_id": plan["batch_id"],
        "comparison_batch_id": plan.get("comparison_batch_id", plan.get("batch_id")),
        "concept_id": concept_id,
        "terminology_version": plan["terminology_version"],
        "semantic_family": term.get("semantic_family"),
        "proxy_type": term.get("proxy_type"),
        "baseline_mean": baseline_mean,
        "baseline_standard_deviation": baseline_sd,
        "baseline_median": baseline_median,
        "baseline_mad": baseline_mad,
        "baseline_valid_day_count": len(baseline_values),
    }
    phase_maxima: dict[str, float] = {}
    standardized_lifts: dict[str, float] = {}
    robust_lifts: dict[str, float] = {}
    for phase, (start, end) in windows.items():
        values = _period_values(series, start, end)
        mean = statistics.fmean(values) if values else None
        maximum = max(values) if values else None
        result[f"{phase}_valid_day_count"] = len(values)
        result[f"{phase}_mean"] = mean
        result[f"{phase}_maximum"] = maximum
        result[f"{phase}_peak_lift"] = (
            maximum - baseline_mean if maximum is not None and baseline_mean is not None else None
        )
        standardized = _standardized_lift(mean, baseline_mean, baseline_sd, normalization)
        robust = _robust_lift(mean, baseline_median, baseline_mad, normalization)
        result[f"{phase}_standardized_lift"] = standardized
        result[f"{phase}_robust_lift"] = robust
        if standardized is not None:
            standardized_lifts[phase] = standardized
        if robust is not None:
            robust_lifts[phase] = robust
        if maximum is not None:
            phase_maxima[phase] = maximum
    overall_peak_date = _peak_date(series)
    peak_phase = "outside_event_phases"
    if overall_peak_date is not None:
        for phase, (start, end) in windows.items():
            if start <= overall_peak_date <= end:
                peak_phase = phase
                break
    global_peak_value = series.get(overall_peak_date) if overall_peak_date is not None else None
    result["global_peak_phase"] = peak_phase
    result["global_peak_date"] = _iso(overall_peak_date)
    result["global_peak_value"] = global_peak_value
    result["peak_phase"] = peak_phase
    result["peak_date"] = _iso(overall_peak_date)
    result["peak_lead_lag_days"] = (
        (overall_peak_date - event_start).days if overall_peak_date is not None else None
    )
    result["standardized_peak_lift"] = _standardized_lift(
        max(phase_maxima.values()) if phase_maxima else None,
        baseline_mean,
        baseline_sd,
        normalization,
    )
    dominant_phase = (
        max(standardized_lifts, key=lambda phase: (standardized_lifts[phase], phase))
        if standardized_lifts
        else None
    )
    result["dominant_event_response_phase"] = dominant_phase
    result["dominant_standardized_phase_lift"] = (
        standardized_lifts.get(dominant_phase) if dominant_phase else None
    )
    result["dominant_robust_phase_lift"] = (
        robust_lifts.get(dominant_phase) if dominant_phase else None
    )
    minimum_duration_lift = float(
        cast(Mapping[str, Any], rules["attribution"])["minimum_standardized_phase_lift"]
    )
    result["longest_qualifying_response_run_days"] = _longest_response_run(
        series,
        baseline_mean,
        baseline_sd,
        windows,
        normalization,
        minimum_duration_lift,
    )
    if not series:
        quality = "missing"
    elif len(baseline_values) < int(normalization["minimum_baseline_days"]):
        quality = "insufficient_baseline"
    elif all(value == 0 for value in series.values()):
        quality = "all_zero"
    else:
        quality = "usable"
    result["metric_quality_status"] = quality
    return result


def _national_comparisons(
    phase_rows: Sequence[Mapping[str, Any]], rules: Mapping[str, Any]
) -> list[dict[str, Any]]:
    national = {
        (
            str(row["provisional_episode_id"]),
            str(row["concept_id"]),
            str(row.get("comparison_batch_id")),
        ): row
        for row in phase_rows
        if row.get("request_role") == "national_comparison"
    }
    output = []
    for state in phase_rows:
        if state.get("request_role") != "treated_state_comparison":
            continue
        key = (
            str(state["provisional_episode_id"]),
            str(state["concept_id"]),
            str(state.get("comparison_batch_id")),
        )
        comparison = national.get(key)
        if comparison is None:
            continue
        comparison_phase = str(state.get("dominant_event_response_phase") or "")
        state_lift = _optional_float(state.get(f"{comparison_phase}_standardized_lift"))
        national_lift = _optional_float(comparison.get(f"{comparison_phase}_standardized_lift"))
        difference = subtract_standardized_lifts(
            state_lift,
            national_lift,
            STANDARDIZED_COMPARISON_SCALE,
        )
        peak_difference = _day_difference(
            _agreed_repeat_peak_date(state),
            _agreed_repeat_peak_date(comparison),
        )
        state_agreement = bool(state.get("repeat_peak_date_agreement"))
        national_agreement = bool(comparison.get("repeat_peak_date_agreement"))
        state_has_peak = bool(state.get("repeat_peak_dates"))
        national_has_peak = bool(comparison.get("repeat_peak_dates"))
        if not state_has_peak or not national_has_peak:
            concurrence_status = "insufficient_data"
        elif not state_agreement and not national_agreement:
            concurrence_status = "indeterminate_both_repeat_disagreement"
        elif not state_agreement:
            concurrence_status = "indeterminate_state_repeat_disagreement"
        elif not national_agreement:
            concurrence_status = "indeterminate_national_repeat_disagreement"
        elif peak_difference is None:
            concurrence_status = "insufficient_data"
        elif peak_difference <= int(rules["national_concurrence_days"]):
            concurrence_status = "concurrent"
        else:
            concurrence_status = "not_concurrent"
        concurrent_flag = (
            concurrence_status == "concurrent"
            if concurrence_status in {"concurrent", "not_concurrent"}
            else None
        )
        output.append(
            {
                "provisional_episode_id": state["provisional_episode_id"],
                "episode_id": state["provisional_episode_id"],
                "state_request_id": state["request_id"],
                "national_request_id": comparison["request_id"],
                "concept_id": state["concept_id"],
                "state_request_role": state["request_role"],
                "national_request_role": comparison["request_role"],
                "geography_level": state["geography_level"],
                "batch_id": state["batch_id"],
                "terminology_version": state["terminology_version"],
                "comparison_batch_id": state.get("comparison_batch_id"),
                "state_geography": state["geography"],
                "national_geography": comparison["geography"],
                "comparison_phase": comparison_phase,
                "state_baseline_lift": state_lift,
                "national_baseline_lift": national_lift,
                "state_minus_national_lift": difference,
                "state_peak_date": _iso(_agreed_repeat_peak_date(state)),
                "national_peak_date": _iso(_agreed_repeat_peak_date(comparison)),
                "state_national_peak_difference_days": peak_difference,
                "national_concurrence_status": concurrence_status,
                "national_concurrent_spike_flag": concurrent_flag,
                "state_repeat_peak_date_agreement": state_agreement,
                "national_repeat_peak_date_agreement": national_agreement,
                "state_repeat_peak_date_range_days": state.get("repeat_peak_date_range_days"),
                "national_repeat_peak_date_range_days": comparison.get(
                    "repeat_peak_date_range_days"
                ),
                "state_repeat_count": int(state.get("repeat_count") or 1),
                "comparison_scale": STANDARDIZED_COMPARISON_SCALE,
                "treated_repeat_count": int(state.get("repeat_count") or 1),
                "national_repeat_count": int(comparison.get("repeat_count") or 1),
                "treated_maximum_repeat_variability": state.get("maximum_repeat_phase_lift_stddev"),
                "national_maximum_repeat_variability": comparison.get(
                    "maximum_repeat_phase_lift_stddev"
                ),
                "treated_repeat_stability_status": state.get("repeat_stability_status"),
                "national_repeat_stability_status": comparison.get("repeat_stability_status"),
                "aggregation_method": "logical_request_mean_after_repeat_variability",
                "raw_index_subtraction_performed": False,
            }
        )
    return output


def classify_peak_attribution(
    phase: Mapping[str, Any],
    concurrence: Mapping[str, Any],
    control: Mapping[str, Any] | None,
    national: Mapping[str, Any] | None,
    rules: Mapping[str, Any],
) -> tuple[str, list[str]]:
    if phase["metric_quality_status"] != "usable":
        return "insufficient_evidence", ["unusable_or_insufficient_series"]
    dominant_phase = str(phase.get("dominant_event_response_phase") or "")
    event_phase = dominant_phase in PHASES
    standardized = _optional_float(phase.get("dominant_standardized_phase_lift"))
    robust = _optional_float(phase.get("dominant_robust_phase_lift"))
    effect_size_support = bool(
        standardized is not None
        and standardized >= float(rules["minimum_standardized_phase_lift"])
        and (robust is None or robust >= float(rules["minimum_robust_phase_lift"]))
    )
    flood = _consensus_support(
        concurrence,
        "strong_flood_concurrent_repeat_fraction",
        float(rules["minimum_strong_concurrence_repeat_fraction"]),
        bool(rules["require_unanimous_concurrence_when_two_repeats"]),
    )
    weather = _consensus_support(
        concurrence,
        "weather_concurrent_repeat_fraction",
        float(rules["minimum_weather_concurrence_repeat_fraction"]),
        bool(rules["require_unanimous_concurrence_when_two_repeats"]),
    )
    nonflood_weather = _consensus_support(
        concurrence,
        "nonflood_weather_concurrent_repeat_fraction",
        float(rules["minimum_weather_concurrence_repeat_fraction"]),
        bool(rules["require_unanimous_concurrence_when_two_repeats"]),
    )
    adjusted = _optional_float(control.get("adjusted_event_lift")) if control else None
    control_support = bool(
        adjusted is not None
        and adjusted >= float(rules["minimum_positive_adjusted_lift"])
        and int(control.get("control_count", 0) if control else 0)
        >= int(rules["minimum_control_count"])
        and control is not None
        and control.get("control_quality_status") == "usable"
    )
    repeat_count = int(phase.get("repeat_count") or 1)
    repeat_stable = not (
        bool(rules["require_stable_repeats_when_repeated"])
        and repeat_count > 1
        and phase.get("repeat_stability_status") != "stable"
    )
    national_status = (
        str(national.get("national_concurrence_status")) if national else "insufficient_data"
    )
    national_spike = national_status == "concurrent"
    national_absent = national_status == "not_concurrent"
    retailer = phase.get("proxy_type") == "retailer_brand"
    if (
        event_phase
        and effect_size_support
        and flood
        and control_support
        and repeat_stable
        and national_absent
    ):
        return "event_consistent_signal", [
            "phase_specific_effect_size",
            "strong_flood_concurrence",
            "adequate_control_adjusted_lift",
            "repeat_stability_acceptable",
            "no_national_concurrent_spike",
        ]
    if retailer and national_spike and not flood and not control_support:
        return "likely_national_or_promotional", [
            "retailer_proxy",
            "national_concurrent_spike",
            "no_flood_concurrence",
            "weak_state_specific_lift",
        ]
    if (weather or nonflood_weather) and not flood:
        return "nonflood_weather_consistent", [
            "nonflood_weather_context_concurrence"
            if nonflood_weather
            else "weather_attention_concurrence",
            "no_flood_awareness_concurrence",
        ]
    if event_phase and effect_size_support and (flood or weather):
        reasons = [
            "phase_specific_effect_size",
            "partial_or_ambiguous_support",
        ]
        if not repeat_stable:
            reasons.append("unstable_repeats")
        if national_status not in {"concurrent", "not_concurrent"}:
            reasons.append("national_concurrence_indeterminate_or_unavailable")
        return "possibly_event_consistent", reasons
    return "unrelated_or_ambiguous", [
        "no_supported_event_phase_effect"
        if not event_phase or not effect_size_support
        else "missing_contextual_or_comparison_support"
    ]


def _request_series(
    observations: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str], dict[date, float]]:
    values: dict[tuple[str, str, str], dict[date, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in observations:
        if row.get("interest") is None or bool(row.get("is_partial", False)):
            continue
        key = (
            str(row["request_id"]),
            str(row.get("repeat_id") or "initial"),
            str(row["concept_id"]),
        )
        values[key][_as_date(row["date"])].append(float(row["interest"]))
    return {
        key: {day: statistics.fmean(points) for day, points in sorted(days.items())}
        for key, days in values.items()
    }


def _best_concurrent_series(
    series: Mapping[str, Mapping[date, float]], candidates: set[str]
) -> str | None:
    available = [item for item in candidates if item in series and series[item]]
    if not available:
        return None
    return max(
        available,
        key=lambda item: (max(series[item].values()), len(series[item]), item),
    )


def _aligned_correlation(
    left: Mapping[date, float], right: Mapping[date, float], minimum: int
) -> float | None:
    common = sorted(set(left) & set(right))
    if len(common) < minimum:
        return None
    left_values = [left[day] for day in common]
    right_values = [right[day] for day in common]
    if statistics.pstdev(left_values) == 0 or statistics.pstdev(right_values) == 0:
        return None
    left_mean = statistics.fmean(left_values)
    right_mean = statistics.fmean(right_values)
    covariance = statistics.fmean(
        (a - left_mean) * (b - right_mean) for a, b in zip(left_values, right_values, strict=True)
    )
    return covariance / (statistics.pstdev(left_values) * statistics.pstdev(right_values))


def _control_score(
    treated: str,
    candidate: str,
    metadata: Mapping[str, Mapping[str, Any]],
    rules: Mapping[str, Any],
) -> tuple[float, list[str]]:
    criteria = []
    score = 0.0
    treated_meta = metadata.get(treated, {})
    candidate_meta = metadata.get(candidate, {})
    treated_region = treated_meta.get("census_region", REGIONS.get(treated))
    candidate_region = candidate_meta.get("census_region", REGIONS.get(candidate))
    if treated_region == candidate_region:
        score += float(rules["same_region_score"])
        criteria.append("same_census_region")
    if treated_meta.get("population_tier") not in {None, "unknown"} and treated_meta.get(
        "population_tier"
    ) == candidate_meta.get("population_tier"):
        score += float(rules["population_tier_score"])
        criteria.append("same_population_tier")
    if treated_meta.get("climate_class") not in {None, "unknown"} and treated_meta.get(
        "climate_class"
    ) == candidate_meta.get("climate_class"):
        score += float(rules["climate_class_score"])
        criteria.append("same_climate_class")
    if (
        candidate_meta.get("observed_trends_availability", candidate_meta.get("trends_available"))
        is True
    ):
        score += float(rules["trends_availability_score"])
        criteria.append("trends_data_available")
    if not criteria:
        criteria.append("deterministic_fallback")
    return score, criteria


def _state_has_episode(
    state: str,
    start: date,
    end: date,
    intervals_by_state: Mapping[str, Sequence[tuple[date, date, str]]],
    treated_episode_id: str,
) -> bool:
    return any(
        episode_id != treated_episode_id and episode_start <= end and episode_end >= start
        for episode_start, episode_end, episode_id in intervals_by_state.get(state, [])
    )


def _state_episode_overlap_count(
    state: str,
    start: date,
    end: date,
    intervals_by_state: Mapping[str, Sequence[tuple[date, date, str]]],
    treated_episode_id: str,
) -> int:
    return sum(
        episode_id != treated_episode_id and episode_start <= end and episode_end >= start
        for episode_start, episode_end, episode_id in intervals_by_state.get(state, [])
    )


def _catalog_intervals_by_state(
    catalog: Sequence[Mapping[str, Any]],
) -> dict[str, list[tuple[date, date, str]]]:
    output: dict[str, list[tuple[date, date, str]]] = defaultdict(list)
    for row in catalog:
        interval = (
            _as_date(row["start_date"]),
            _as_date(row["end_date"]),
            str(row.get("provisional_episode_id")),
        )
        for state in _states(row):
            output[state].append(interval)
    return {state: sorted(intervals) for state, intervals in output.items()}


def _state_metadata(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    rows = _read_rows(path)
    return {
        str(row.get("state_code") or row.get("state")): row
        for row in rows
        if row.get("state_code") or row.get("state")
    }


def _states(row: Mapping[str, Any]) -> set[str]:
    states = {
        item.strip()
        for item in str(row.get("states", row.get("primary_state", ""))).split(";")
        if item.strip()
    }
    if row.get("primary_state"):
        states.add(str(row["primary_state"]))
    return states


def _standardized_lift(
    value: float | None,
    baseline_mean: float | None,
    baseline_sd: float | None,
    normalization: Mapping[str, Any],
) -> float | None:
    if value is None or baseline_mean is None or baseline_sd is None:
        return None
    if baseline_sd <= float(normalization["near_zero_standard_deviation"]):
        return None
    return (value - baseline_mean) / baseline_sd


def _robust_lift(
    value: float | None,
    baseline_median: float | None,
    baseline_mad: float | None,
    normalization: Mapping[str, Any],
) -> float | None:
    if value is None or baseline_median is None or baseline_mad is None:
        return None
    if baseline_mad <= float(normalization["near_zero_standard_deviation"]):
        return None
    return (value - baseline_median) / (1.4826 * baseline_mad)


def _longest_response_run(
    series: Mapping[date, float],
    baseline_mean: float | None,
    baseline_sd: float | None,
    windows: Mapping[str, tuple[date, date]],
    normalization: Mapping[str, Any],
    minimum_lift: float,
) -> int:
    if baseline_mean is None or baseline_sd is None:
        return 0
    allowed_days = {
        day
        for start, end in windows.values()
        for day in (start + timedelta(days=offset) for offset in range((end - start).days + 1))
    }
    qualifying = sorted(
        day
        for day, value in series.items()
        if day in allowed_days
        and (_standardized_lift(value, baseline_mean, baseline_sd, normalization) or 0.0)
        >= minimum_lift
    )
    best = current = 0
    previous: date | None = None
    for day in qualifying:
        current = current + 1 if previous is not None and day == previous + timedelta(days=1) else 1
        best = max(best, current)
        previous = day
    return best


def _phase_repeat_outputs(
    rows: list[dict[str, Any]], rules: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].append(row)
    threshold = float(cast(Mapping[str, Any], rules["attribution"])["maximum_repeat_lift_stddev"])
    diagnostics: list[dict[str, Any]] = []
    aggregated: list[dict[str, Any]] = []
    for (request_id, concept_id), repeats in sorted(grouped.items()):
        phase_variability: dict[str, float | None] = {}
        for phase in PHASES:
            values = [
                float(row[f"{phase}_standardized_lift"])
                for row in repeats
                if row.get(f"{phase}_standardized_lift") is not None
            ]
            phase_variability[phase] = statistics.pstdev(values) if len(values) > 1 else None
        dominant_phases = [str(row.get("dominant_event_response_phase")) for row in repeats]
        maximum_stddev = max(
            (value for value in phase_variability.values() if value is not None),
            default=None,
        )
        stable = bool(
            len(repeats) > 1
            and len(set(dominant_phases)) == 1
            and maximum_stddev is not None
            and maximum_stddev <= threshold
        )
        status = "stable" if stable else "unstable" if len(repeats) > 1 else "not_repeated"
        for row in repeats:
            row["repeat_count"] = len(repeats)
            row["repeat_stability_status"] = status
            row["maximum_repeat_phase_lift_stddev"] = maximum_stddev
        diagnostics.append(
            {
                "request_id": request_id,
                "concept_id": concept_id,
                "repeat_count": len(repeats),
                "repeat_ids": ";".join(sorted(str(row["repeat_id"]) for row in repeats)),
                "export_attempt_ids": ";".join(
                    sorted(str(row.get("export_attempt_id") or "") for row in repeats)
                ),
                "dominant_phase_agreement": len(set(dominant_phases)) == 1,
                "maximum_phase_lift_stddev": maximum_stddev,
                "repeat_stability_status": status,
                **{
                    f"{phase}_standardized_lift_stddev": phase_variability[phase]
                    for phase in PHASES
                },
            }
        )
        aggregated.append(_aggregate_phase_repeats(repeats, status, maximum_stddev))
    return diagnostics, aggregated


def _aggregate_phase_repeats(
    repeats: Sequence[Mapping[str, Any]],
    stability_status: str,
    maximum_stddev: float | None,
) -> dict[str, Any]:
    ordered = sorted(repeats, key=lambda row: str(row.get("repeat_id") or "initial"))
    aggregate = dict(ordered[0])
    aggregate["repeat_id"] = "aggregated_after_variability"
    aggregate["repeat_count"] = len(ordered)
    aggregate["repeat_ids"] = ";".join(str(row.get("repeat_id") or "initial") for row in ordered)
    aggregate["export_attempt_id"] = ";".join(
        sorted(str(row.get("export_attempt_id") or "") for row in ordered)
    )
    aggregate["repeat_stability_status"] = stability_status
    aggregate["maximum_repeat_phase_lift_stddev"] = maximum_stddev
    aggregate["aggregation_method"] = "arithmetic_mean_after_repeat_variability"
    numeric_fields = {
        "baseline_mean",
        "baseline_standard_deviation",
        "baseline_median",
        "baseline_mad",
        "standardized_peak_lift",
        *{
            f"{phase}_{suffix}"
            for phase in PHASES
            for suffix in ("mean", "maximum", "peak_lift", "standardized_lift", "robust_lift")
        },
    }
    for field in sorted(numeric_fields):
        values = [float(row[field]) for row in ordered if row.get(field) is not None]
        aggregate[field] = statistics.fmean(values) if values else None
    standardized = {
        phase: float(aggregate[f"{phase}_standardized_lift"])
        for phase in PHASES
        if aggregate.get(f"{phase}_standardized_lift") is not None
    }
    dominant = (
        max(standardized, key=lambda phase: (standardized[phase], phase)) if standardized else None
    )
    aggregate["dominant_event_response_phase"] = dominant
    aggregate["dominant_standardized_phase_lift"] = (
        aggregate.get(f"{dominant}_standardized_lift") if dominant else None
    )
    aggregate["dominant_robust_phase_lift"] = (
        aggregate.get(f"{dominant}_robust_lift") if dominant else None
    )
    peak_dates = sorted(
        date_value
        for row in ordered
        if (date_value := _optional_date(row.get("global_peak_date"))) is not None
    )
    aggregate["repeat_peak_dates"] = ";".join(item.isoformat() for item in peak_dates)
    aggregate["repeat_peak_date_agreement"] = bool(
        peak_dates and len(peak_dates) == len(ordered) and len(set(peak_dates)) == 1
    )
    aggregate["repeat_peak_date_range_days"] = (
        (peak_dates[-1] - peak_dates[0]).days if peak_dates else None
    )
    repeated_phases = [row.get("dominant_event_response_phase") for row in ordered]
    aggregate["repeat_dominant_phase_agreement"] = len(set(repeated_phases)) == 1
    for field in (
        "global_peak_date",
        "global_peak_phase",
        "global_peak_value",
        "peak_date",
        "peak_phase",
        "peak_lead_lag_days",
    ):
        aggregate[field] = None
    quality = {str(row.get("metric_quality_status") or "missing") for row in ordered}
    aggregate["metric_quality_status"] = (
        next(iter(quality)) if len(quality) == 1 else "mixed_repeat_quality"
    )
    return aggregate


def _logical_phase_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].append(row)
    output = []
    for repeats in grouped.values():
        if len(repeats) == 1 and repeats[0].get("repeat_id") == "aggregated_after_variability":
            output.append(dict(repeats[0]))
            continue
        variability = max(
            (
                float(row["maximum_repeat_phase_lift_stddev"])
                for row in repeats
                if row.get("maximum_repeat_phase_lift_stddev") is not None
            ),
            default=None,
        )
        statuses = {str(row.get("repeat_stability_status") or "not_repeated") for row in repeats}
        status = next(iter(statuses)) if len(statuses) == 1 else "mixed_repeat_stability"
        output.append(_aggregate_phase_repeats(repeats, status, variability))
    return sorted(output, key=lambda row: (str(row["request_id"]), str(row["concept_id"])))


def _logical_concurrence_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].append(row)
    output = []
    for repeats in grouped.values():
        ordered = sorted(repeats, key=lambda row: str(row.get("repeat_id")))
        aggregate = dict(ordered[0])
        repeat_count = len(ordered)
        aggregate["repeat_id"] = "aggregated_after_variability"
        aggregate["export_attempt_id"] = "aggregated_after_variability"
        aggregate["repeat_export_attempt_ids"] = ";".join(
            sorted(
                str(row["export_attempt_id"])
                for row in ordered
                if row.get("export_attempt_id") not in {None, ""}
            )
        )
        aggregate["repeat_count"] = repeat_count
        aggregate["aggregation_method"] = "logical_request_repeat_consensus_v1"
        count_fields = {
            "strong_flood_concurrent_repeat_count": "simultaneous_flood_awareness_flag",
            "moderate_flood_concurrent_repeat_count": "moderate_flood_awareness_flag",
            "weather_concurrent_repeat_count": "simultaneous_weather_attention_flag",
            "nonflood_weather_concurrent_repeat_count": "simultaneous_nonflood_weather_flag",
        }
        for count_field, source_field in count_fields.items():
            count = sum(bool(row.get(source_field)) for row in ordered)
            fraction_field = count_field.replace("_count", "_fraction")
            aggregate[count_field] = count
            aggregate[fraction_field] = count / repeat_count
        aggregate["any_repeat_strong_flood_concurrence"] = bool(
            aggregate["strong_flood_concurrent_repeat_count"]
        )
        aggregate["any_repeat_weather_concurrence"] = bool(
            aggregate["weather_concurrent_repeat_count"]
        )
        aggregate["any_repeat_nonflood_weather_concurrence"] = bool(
            aggregate["nonflood_weather_concurrent_repeat_count"]
        )
        valid_differences = [
            value
            for row in ordered
            if (value := _optional_float(row.get("demand_flood_peak_difference_days"))) is not None
        ]
        aggregate["flood_peak_difference_median_days"] = (
            statistics.median(valid_differences) if valid_differences else None
        )
        aggregate["flood_peak_difference_range_days"] = (
            max(valid_differences) - min(valid_differences) if valid_differences else None
        )
        aggregate["concurrence_consensus_status"] = _concurrence_consensus_status(
            repeat_count,
            int(aggregate["strong_flood_concurrent_repeat_count"]),
            len(valid_differences),
        )
        _set_repeat_agreement_fields(aggregate, ordered)
        _set_correlation_summaries(aggregate, ordered)
        for field in (
            "simultaneous_flood_awareness_flag",
            "moderate_flood_awareness_flag",
            "simultaneous_weather_attention_flag",
            "simultaneous_nonflood_weather_flag",
        ):
            aggregate[field] = ordered[0].get(field) if repeat_count == 1 else None
        output.append(aggregate)
    return sorted(output, key=lambda row: (str(row["request_id"]), str(row["concept_id"])))


def _concurrence_consensus_status(
    repeat_count: int, concurrent_count: int, valid_evidence_count: int
) -> str:
    if valid_evidence_count == 0:
        return "insufficient_repeat_evidence"
    if repeat_count == 1:
        return "not_repeated"
    if concurrent_count == repeat_count:
        return "unanimous"
    if concurrent_count > repeat_count / 2:
        return "majority"
    if concurrent_count > 0:
        return "minority_only"
    return "none"


def _set_repeat_agreement_fields(
    aggregate: dict[str, Any], repeats: Sequence[Mapping[str, Any]]
) -> None:
    concept_fields = {
        "flood_awareness_concept_id": "repeat_flood_awareness_concept_agreement",
        "weather_context_concept_id": "repeat_weather_context_concept_agreement",
        "nonflood_weather_concept_id": "repeat_nonflood_context_concept_agreement",
    }
    for field, agreement_field in concept_fields.items():
        values = [str(row[field]) for row in repeats if row.get(field) not in {None, ""}]
        agreement = len(values) == len(repeats) and len(set(values)) == 1
        aggregate[agreement_field] = agreement
        aggregate[field] = values[0] if agreement else None
        aggregate[f"repeat_{field}s"] = ";".join(sorted(set(values)))
    for field in ("flood_awareness_peak_date", "demand_proxy_peak_date"):
        values = [str(row[field]) for row in repeats if row.get(field) not in {None, ""}]
        agreement = len(values) == len(repeats) and len(set(values)) == 1
        aggregate[f"repeat_{field}_agreement"] = agreement
        aggregate[f"repeat_{field}s"] = ";".join(sorted(values))
        aggregate[field] = values[0] if agreement else None
    differences = [
        value
        for row in repeats
        if (value := _optional_float(row.get("demand_flood_peak_difference_days"))) is not None
    ]
    aggregate["demand_flood_peak_difference_days"] = (
        differences[0] if len(differences) == len(repeats) and len(set(differences)) == 1 else None
    )


def _set_correlation_summaries(
    aggregate: dict[str, Any], repeats: Sequence[Mapping[str, Any]]
) -> None:
    for source, prefix in (
        ("flood_awareness_correlation", "flood_awareness_correlation"),
        ("weather_context_correlation", "weather_context_correlation"),
    ):
        values = [
            value for row in repeats if (value := _optional_float(row.get(source))) is not None
        ]
        aggregate[f"{prefix}_median"] = statistics.median(values) if values else None
        aggregate[f"{prefix}_minimum"] = min(values) if values else None
        aggregate[f"{prefix}_maximum"] = max(values) if values else None
        aggregate[f"{prefix}_valid_repeat_count"] = len(values)
        aggregate[source] = values[0] if len(repeats) == 1 and values else None


def _consensus_support(
    concurrence: Mapping[str, Any],
    fraction_field: str,
    minimum_fraction: float,
    require_unanimous_for_two: bool,
) -> bool:
    repeat_count = int(concurrence.get("repeat_count") or 1)
    fraction = _optional_float(concurrence.get(fraction_field))
    if fraction is None:
        return False
    if repeat_count == TWO_REPEAT_COUNT and require_unanimous_for_two:
        return fraction == 1.0
    return fraction >= minimum_fraction


def subtract_standardized_lifts(
    left: float | None, right: float | None, comparison_scale: str
) -> float | None:
    if comparison_scale != STANDARDIZED_COMPARISON_SCALE:
        raise TrendsError(
            "Cross-geography subtraction requires within-series baseline-standardized lifts."
        )
    if left is None or right is None:
        return None
    return left - right


def _control_lookup(path: Path | None) -> dict[tuple[str, str, str], dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    return {
        (
            str(row["provisional_episode_id"]),
            str(row["concept_id"]),
            str(row["phase"]),
        ): row
        for row in _read_rows(path)
    }


def _national_lookup(path: Path | None) -> dict[tuple[str, str, str], dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    return {
        (
            str(row["provisional_episode_id"]),
            str(row["concept_id"]),
            str(row["comparison_batch_id"]),
        ): row
        for row in _read_rows(path)
    }


def _period_values(series: Mapping[date, float], start: date, end: date) -> list[float]:
    return [value for day, value in series.items() if start <= day <= end]


def _peak_date(series: Mapping[date, float]) -> date | None:
    if not series:
        return None
    maximum = max(series.values())
    return min(day for day, value in series.items() if value == maximum)


def _day_difference(left: date | None, right: date | None) -> int | None:
    return abs((left - right).days) if left is not None and right is not None else None


def _agreed_repeat_peak_date(row: Mapping[str, Any]) -> date | None:
    if not bool(row.get("repeat_peak_date_agreement")):
        return None
    values = str(row.get("repeat_peak_dates") or "").split(";")
    return _optional_date(values[0]) if values and values[0] else None


def _iqr(values: Sequence[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    quartiles = statistics.quantiles(values, n=4, method="inclusive")
    return quartiles[2] - quartiles[0]


def _controls_concordant(values: Sequence[float]) -> bool | None:
    if not values:
        return None
    positive = sum(value > 0 for value in values)
    negative = sum(value < 0 for value in values)
    return max(positive, negative) / len(values) >= CONTROL_CONCORDANCE_FRACTION


def _optional_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def _optional_date(value: Any) -> date | None:
    return _as_date(value) if value is not None else None


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _iso(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TrendsError(f"Expected a YAML mapping in {path}.")
    return payload


def _read_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(_deterministic_rows(rows))
    pq.write_table(table, path, compression="zstd")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    ordered = _deterministic_rows(rows)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        if fieldnames:
            writer.writeheader()
            writer.writerows(ordered)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _deterministic_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = [dict(row) for row in rows]
    identity_fields = (
        "provisional_episode_id",
        "treated_episode",
        "episode_id",
        "request_role",
        "geography",
        "request_id",
        "batch_id",
        "concept_id",
        "phase",
        "control_state",
    )
    return sorted(
        output,
        key=lambda row: (
            *(str(row.get(field, "")) for field in identity_fields),
            json.dumps(row, sort_keys=True, default=str),
        ),
    )
