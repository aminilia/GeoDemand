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

from geodemand.trends import REGIONS, VALID_STATES, TrendsError

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
            if state_meta.get("trends_available") is False:
                reasons.append("trends_data_unavailable")
            score, criteria = _control_score(treated, state, metadata, control_rules)
            diagnostic = {
                "treated_episode": episode_id,
                "treated_state": treated,
                "control_state": state,
                "selection_eligible": not reasons,
                "selection_criteria": ";".join(criteria),
                "exclusion_reasons": ";".join(sorted(reasons)),
                "population_tier": state_meta.get("population_tier", "unknown"),
                "census_region": REGIONS.get(state, "unknown"),
                "climate_class": state_meta.get("climate_class", "unknown"),
                "temporal_flood_free_check": not concurrent,
                "trends_availability": state_meta.get("trends_available", "unknown"),
                "selection_score": score,
                "control_rank": None,
            }
            diagnostics.append(diagnostic)
            if not reasons:
                candidates.append(diagnostic)
        ranked = sorted(
            candidates, key=lambda row: (-float(row["selection_score"]), row["control_state"])
        )
        for rank, row in enumerate(ranked[:maximum], 1):
            row["control_rank"] = rank
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
    plans = {str(row["request_id"]): row for row in _read_rows(plan_path)}
    terms = _load_yaml(terms_path)
    term_by_id = {str(row["concept_id"]): row for row in terms["concepts"]}
    grouped = _request_series(_read_rows(observations_path))
    rows = []
    for (request_id, concept_id), series in sorted(grouped.items()):
        plan = plans.get(request_id)
        if plan is None:
            raise TrendsError(f"Imported request is absent from plan: {request_id}")
        rows.append(
            _phase_metric_row(
                request_id, concept_id, series, plan, term_by_id.get(concept_id, {}), rules
            )
        )
    path = output_root / "processed" / "trends_phase_response_metrics.parquet"
    _write_parquet(path, rows)
    return {"phase_metrics": path}


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
    plans = {str(row["request_id"]): row for row in _read_rows(plan_path)}
    observations = _read_rows(observations_path)
    series = _request_series(observations)
    terms = _load_yaml(terms_path)
    term_by_id = {str(row["concept_id"]): row for row in terms["concepts"]}
    output = []
    for (request_id, concept_id), demand_series in sorted(series.items()):
        term = term_by_id.get(concept_id, {})
        if term.get("semantic_family") not in {"behavioral_demand_proxy", "context"}:
            continue
        request_concepts = {
            other_id: other_series
            for (other_request, other_id), other_series in series.items()
            if other_request == request_id
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
    _write_parquet(concurrence_path, output)
    phase_path = phase_metrics_path or (
        output_root / "processed" / "trends_phase_response_metrics.parquet"
    )
    national_rows = _national_comparisons(_read_rows(phase_path), event_rules)
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
        "national_spikes": national_path,
        "retailer_promotions": retailer_path,
    }


def calculate_control_adjusted_metrics(
    phase_metrics_path: Path,
    controls_path: Path,
    output_root: Path,
) -> dict[str, Path]:
    metrics = _read_rows(phase_metrics_path)
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
                    "control_count": len(values),
                    "control_request_ids": ";".join(
                        sorted(str(control["request_id"]) for control in matching)
                    ),
                    "control_geographies": ";".join(
                        sorted(str(control["geography"]) for control in matching)
                    ),
                    "median_control_event_lift": median_control,
                    "control_event_lift_iqr": _iqr(values),
                    "adjusted_event_lift": adjusted,
                    "controls_concordant": _controls_concordant(values),
                    "control_quality_status": "usable"
                    if values and treated_lift is not None
                    else "insufficient_controls",
                    "comparison_scale": STANDARDIZED_COMPARISON_SCALE,
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
    phase_rows = _read_rows(phase_metrics_path)
    concurrence = {
        (str(row["request_id"]), str(row["concept_id"])): row
        for row in _read_rows(concurrence_path)
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
                str(phase["peak_phase"]),
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
            "request_role": phase["request_role"],
            "geography": phase["geography"],
            "geography_level": phase["geography_level"],
            "batch_id": phase["batch_id"],
            "concept_id": phase["concept_id"],
            "terminology_version": phase["terminology_version"],
            "peak_phase": phase["peak_phase"],
            "peak_lead_lag_days": phase["peak_lead_lag_days"],
            "attribution_category": category,
            "attribution_reasons": ";".join(reasons),
            "simultaneous_flood_awareness_flag": evidence.get("simultaneous_flood_awareness_flag"),
            "simultaneous_weather_attention_flag": evidence.get(
                "simultaneous_weather_attention_flag"
            ),
            "adjusted_event_lift": control.get("adjusted_event_lift") if control else None,
            "control_quality_status": control.get("control_quality_status")
            if control
            else "missing",
            "national_concurrent_spike_flag": national_row.get("national_concurrent_spike_flag")
            if national_row
            else None,
            "metric_quality_status": phase["metric_quality_status"],
            "component_evidence_complete": bool(control and national_row),
        }
        output.append(row)
        if phase.get("proxy_type") == "retailer_brand":
            retailer_diagnostics.append(row)
        if category == "weather_related_not_flood_specific":
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
    result: dict[str, Any] = {
        "provisional_episode_id": plan["episode_id"],
        "episode_id": plan["episode_id"],
        "request_id": request_id,
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
        "baseline_valid_day_count": len(baseline_values),
    }
    phase_maxima: dict[str, float] = {}
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
        result[f"{phase}_standardized_lift"] = _standardized_lift(
            mean, baseline_mean, baseline_sd, normalization
        )
        if maximum is not None:
            phase_maxima[phase] = maximum
    overall_peak_date = _peak_date(series)
    peak_phase = "outside_event_phases"
    if overall_peak_date is not None:
        for phase, (start, end) in windows.items():
            if start <= overall_peak_date <= end:
                peak_phase = phase
                break
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
        state_lift = _optional_float(state.get("standardized_peak_lift"))
        national_lift = _optional_float(comparison.get("standardized_peak_lift"))
        difference = subtract_standardized_lifts(
            state_lift,
            national_lift,
            STANDARDIZED_COMPARISON_SCALE,
        )
        peak_difference = _day_difference(
            _optional_date(state.get("peak_date")),
            _optional_date(comparison.get("peak_date")),
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
                "state_baseline_lift": state_lift,
                "national_baseline_lift": national_lift,
                "state_minus_national_lift": difference,
                "state_peak_date": state.get("peak_date"),
                "national_peak_date": comparison.get("peak_date"),
                "state_national_peak_difference_days": peak_difference,
                "national_concurrent_spike_flag": peak_difference is not None
                and peak_difference <= int(rules["national_concurrence_days"]),
                "comparison_scale": STANDARDIZED_COMPARISON_SCALE,
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
    peak_phase = str(phase["peak_phase"])
    event_phase = peak_phase in {"anticipatory", "immediate", "early_recovery"}
    flood = bool(concurrence.get("simultaneous_flood_awareness_flag"))
    weather = bool(concurrence.get("simultaneous_weather_attention_flag"))
    nonflood_weather = bool(concurrence.get("simultaneous_nonflood_weather_flag"))
    adjusted = _optional_float(control.get("adjusted_event_lift")) if control else None
    control_support = adjusted is not None and adjusted > float(
        rules["minimum_positive_adjusted_lift"]
    )
    national_spike = bool(national and national.get("national_concurrent_spike_flag"))
    retailer = phase.get("proxy_type") == "retailer_brand"
    if event_phase and flood and control_support and not national_spike:
        return "likely_event_associated", [
            "event_phase_peak",
            "strong_flood_concurrence",
            "positive_control_adjusted_lift",
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
        return "weather_related_not_flood_specific", [
            "nonflood_weather_context_concurrence"
            if nonflood_weather
            else "weather_attention_concurrence",
            "no_flood_awareness_concurrence",
        ]
    if event_phase and (flood or weather):
        return "possibly_event_associated", [
            "event_phase_peak",
            "partial_or_ambiguous_support",
        ]
    return "unrelated_or_ambiguous", [
        "peak_outside_supported_event_pattern"
        if peak_phase == "outside_event_phases"
        else "missing_contextual_or_comparison_support"
    ]


def _request_series(
    observations: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], dict[date, float]]:
    values: dict[tuple[str, str], dict[date, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in observations:
        if row.get("interest") is None or bool(row.get("is_partial", False)):
            continue
        values[(str(row["request_id"]), str(row["concept_id"]))][_as_date(row["date"])].append(
            float(row["interest"])
        )
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
    if REGIONS.get(treated) == REGIONS.get(candidate):
        score += float(rules["same_region_score"])
        criteria.append("same_census_region")
    treated_meta = metadata.get(treated, {})
    candidate_meta = metadata.get(candidate, {})
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
    if candidate_meta.get("trends_available") is True:
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
    return {str(row["state"]): row for row in _read_rows(path)}


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
        (str(row["provisional_episode_id"]), str(row["concept_id"]), str(row["phase"])): row
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
