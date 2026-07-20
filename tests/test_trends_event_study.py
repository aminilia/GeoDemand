from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from geodemand.trends import VALID_STATES, TrendsError, map_geographies, plan_requests
from geodemand.trends_event_study import (
    STANDARDIZED_COMPARISON_SCALE,
    calculate_concurrence,
    calculate_control_adjusted_metrics,
    calculate_phase_metrics,
    classify_peak_attribution,
    response_phase_windows,
    select_controls,
    subtract_standardized_lifts,
)

ROOT = Path(__file__).parents[1]
RULES = ROOT / "config" / "trends_rules.yaml"
TERMS = ROOT / "config" / "trends_terms.yaml"


@pytest.mark.parametrize(
    ("peak_date", "expected_phase"),
    [
        (date(2024, 6, 4), "anticipatory"),
        (date(2024, 6, 11), "immediate"),
        (date(2024, 6, 15), "early_recovery"),
        (date(2024, 6, 25), "extended_recovery"),
        (date(2024, 5, 15), "outside_event_phases"),
    ],
)
def test_phase_metrics_preserve_each_peak_phase(
    tmp_path: Path, peak_date: date, expected_phase: str
) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    _write(plan, [_plan("r1")])
    _write(observations, _observations("r1", {"walmart": peak_date}))
    paths = calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "out")
    row = _rows(paths["phase_metrics"])[0]
    assert row["peak_phase"] == expected_phase
    assert row["peak_lead_lag_days"] == (peak_date - date(2024, 6, 10)).days
    assert row["baseline_valid_day_count"] == 28
    if expected_phase in {
        "anticipatory",
        "immediate",
        "early_recovery",
        "extended_recovery",
    }:
        assert row[f"{expected_phase}_maximum"] == 80.0
        assert row[f"{expected_phase}_peak_lift"] is not None


def test_response_phase_boundaries() -> None:
    rules = _rules()
    windows = response_phase_windows(date(2024, 6, 10), date(2024, 6, 12), rules)
    assert windows == {
        "anticipatory": (date(2024, 6, 3), date(2024, 6, 9)),
        "immediate": (date(2024, 6, 10), date(2024, 6, 14)),
        "early_recovery": (date(2024, 6, 15), date(2024, 6, 19)),
        "extended_recovery": (date(2024, 6, 20), date(2024, 7, 10)),
    }


def test_behavioral_proxy_and_optional_context_contract() -> None:
    terms = yaml.safe_load(TERMS.read_text(encoding="utf-8"))["concepts"]
    proxies = {
        row["query_text"]: row
        for row in terms
        if row["semantic_family"] == "behavioral_demand_proxy"
    }
    assert set(proxies) == {
        "Walmart",
        "Home Depot",
        "Lowe's",
        "generator",
        "bottled water",
        "batteries",
        "sandbags",
        "sump pump",
        "wet vacuum",
        "dehumidifier",
    }
    assert {row["proxy_type"] for row in proxies.values()} == {
        "retailer_brand",
        "preparation_product",
        "damage_mitigation_product",
    }
    assert all(row["interpretation_scope"] for row in proxies.values())
    optional_context = {
        row["query_text"]
        for row in terms
        if row.get("exclusion_reason") == "selected_ambiguous_cases_only"
    }
    assert optional_context == {"heat", "heat wave", "snow", "tornado", "hurricane"}


@pytest.mark.parametrize(
    ("difference", "strong", "moderate"),
    [(2, True, True), (5, False, True)],
)
def test_flood_awareness_concurrence_windows(
    tmp_path: Path, difference: int, strong: bool, moderate: bool
) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    demand_peak = date(2024, 6, 9)
    _write(plan, [_plan("r1", concepts="context_weather;flood;walmart")])
    _write(
        observations,
        _observations(
            "r1",
            {
                "walmart": demand_peak,
                "flood": demand_peak + timedelta(days=difference),
                "context_weather": demand_peak,
            },
        ),
    )
    phase = calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "out")
    paths = calculate_concurrence(
        observations, plan, TERMS, RULES, tmp_path / "out", phase["phase_metrics"]
    )
    row = next(item for item in _rows(paths["concurrence"]) if item["concept_id"] == "walmart")
    assert row["demand_flood_peak_difference_days"] == difference
    assert row["simultaneous_flood_awareness_flag"] is strong
    assert row["moderate_flood_awareness_flag"] is moderate


def test_insufficient_observations_skip_correlation(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    _write(plan, [_plan("r1", concepts="context_weather;flood;walmart")])
    rows = _observations(
        "r1",
        {
            "walmart": date(2024, 6, 9),
            "flood": date(2024, 6, 10),
            "context_weather": date(2024, 6, 9),
        },
    )[:30]
    _write(observations, rows)
    empty_phase = tmp_path / "phase.parquet"
    _write(empty_phase, [])
    paths = calculate_concurrence(observations, plan, TERMS, RULES, tmp_path / "out", empty_phase)
    walmart = next(item for item in _rows(paths["concurrence"]) if item["concept_id"] == "walmart")
    assert walmart["flood_awareness_correlation"] is None
    assert walmart["weather_context_correlation"] is None


def test_national_retailer_spike_uses_standardized_lifts(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    rows = [
        _plan("state", role="treated_state_comparison", geography="US-FL"),
        _plan("national", role="national_comparison", geography="US", geography_level="country"),
    ]
    _write(plan, rows)
    points = []
    for request_id in ("state", "national"):
        points.extend(_observations(request_id, {"walmart": date(2024, 6, 9)}))
    _write(observations, points)
    phase = calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "out")
    paths = calculate_concurrence(
        observations, plan, TERMS, RULES, tmp_path / "out", phase["phase_metrics"]
    )
    row = _csv_rows(paths["national_spikes"])[0]
    assert row["national_concurrent_spike_flag"] == "True"
    assert row["comparison_scale"] == STANDARDIZED_COMPARISON_SCALE
    assert row["raw_index_subtraction_performed"] == "False"


def test_heat_context_supports_nonflood_weather_explanation(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    _write(plan, [_plan("r1", concepts="context_weather;context_heat;walmart")])
    peak = date(2024, 6, 11)
    _write(
        observations,
        _observations("r1", {"walmart": peak, "context_heat": peak, "context_weather": peak}),
    )
    phase = calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "out")
    paths = calculate_concurrence(
        observations, plan, TERMS, RULES, tmp_path / "out", phase["phase_metrics"]
    )
    row = next(item for item in _rows(paths["concurrence"]) if item["concept_id"] == "walmart")
    assert row["nonflood_weather_concept_id"] == "context_heat"
    assert row["simultaneous_nonflood_weather_flag"] is True


def test_control_selection_is_deterministic_and_excludes_concurrent_flood(
    tmp_path: Path,
) -> None:
    pilot = tmp_path / "pilot.parquet"
    catalog = tmp_path / "catalog.parquet"
    metadata = tmp_path / "metadata.parquet"
    treated = _episode("e1", "TX", date(2024, 6, 10))
    concurrent = _episode("e2", "OK", date(2024, 6, 12))
    _write(pilot, [treated])
    _write(catalog, [treated, concurrent])
    _write(metadata, _metadata({"OK", "LA", "CA"}))
    first = select_controls(pilot, catalog, RULES, tmp_path / "first", metadata)
    second = select_controls(pilot, catalog, RULES, tmp_path / "second", metadata)
    assert _sha(first["controls"]) == _sha(second["controls"])
    assert _sha(first["diagnostics"]) == _sha(second["diagnostics"])
    selected = _rows(first["controls"])
    assert "OK" not in {row["control_state"] for row in selected}
    diagnostic = next(
        row for row in _csv_rows(first["diagnostics"]) if row["control_state"] == "OK"
    )
    assert "concurrent_provisional_flood_episode" in diagnostic["exclusion_reasons"]


def test_control_selection_regional_fallback_and_no_valid_state(tmp_path: Path) -> None:
    pilot = tmp_path / "pilot.parquet"
    catalog = tmp_path / "catalog.parquet"
    treated = _episode("e1", "TX", date(2024, 6, 10))
    _write(pilot, [treated])
    _write(catalog, [treated])
    fallback_metadata = tmp_path / "fallback.parquet"
    _write(fallback_metadata, _metadata({"CA"}))
    fallback = select_controls(pilot, catalog, RULES, tmp_path / "fallback-out", fallback_metadata)
    assert [row["control_state"] for row in _rows(fallback["controls"])] == ["CA"]
    assert _rows(fallback["controls"])[0]["matching_quality_status"] == (
        "metadata_backed_match_candidate"
    )

    unmatched = select_controls(pilot, catalog, RULES, tmp_path / "unmatched-out")
    unmatched_rows = _rows(unmatched["controls"])
    assert unmatched_rows
    assert all(
        row["matching_quality_status"] == "unmatched_fallback_control" for row in unmatched_rows
    )

    none_metadata = tmp_path / "none.parquet"
    _write(none_metadata, _metadata(set()))
    no_controls = select_controls(pilot, catalog, RULES, tmp_path / "none-out", none_metadata)
    summary = json.loads(no_controls["summary"].read_text(encoding="utf-8"))
    assert summary["selected_control_count"] == 0
    assert summary["episodes_with_controls"] == 0


def test_control_adjusted_lift_and_raw_subtraction_guard(tmp_path: Path) -> None:
    phase = tmp_path / "phase.parquet"
    controls = tmp_path / "controls.parquet"
    treated = _phase_row("treated", "treated_state_comparison", "US-FL", 3.0)
    control_one = _phase_row("c1", "control_state", "US-GA", 1.0)
    control_two = _phase_row("c2", "control_state", "US-SC", 2.0)
    _write(phase, [control_two, treated, control_one])
    _write(
        controls,
        [
            {"treated_episode": "e1", "control_state": "GA"},
            {"treated_episode": "e1", "control_state": "SC"},
        ],
    )
    paths = calculate_control_adjusted_metrics(phase, controls, tmp_path / "out")
    immediate = next(
        row for row in _rows(paths["control_adjusted_metrics"]) if row["phase"] == "immediate"
    )
    assert immediate["treated_event_lift"] == 3.0
    assert immediate["median_control_event_lift"] == 1.5
    assert immediate["adjusted_event_lift"] == 1.5
    assert immediate["raw_index_subtraction_performed"] is False
    assert subtract_standardized_lifts(3.0, 1.5, STANDARDIZED_COMPARISON_SCALE) == 1.5
    with pytest.raises(TrendsError, match="baseline-standardized"):
        subtract_standardized_lifts(80.0, 20.0, "raw_google_trends_index")


@pytest.mark.parametrize(
    (
        "quality",
        "peak_phase",
        "proxy_type",
        "flood",
        "weather",
        "nonflood",
        "control",
        "national",
        "expected",
    ),
    [
        (
            "usable",
            "immediate",
            "preparation_product",
            True,
            True,
            False,
            1.0,
            False,
            "event_consistent_signal",
        ),
        (
            "usable",
            "anticipatory",
            "preparation_product",
            True,
            False,
            False,
            None,
            None,
            "possibly_event_consistent",
        ),
        (
            "usable",
            "immediate",
            "retailer_brand",
            False,
            False,
            False,
            -0.5,
            True,
            "likely_national_or_promotional",
        ),
        (
            "usable",
            "immediate",
            "retailer_brand",
            False,
            True,
            False,
            None,
            None,
            "nonflood_weather_consistent",
        ),
        (
            "usable",
            "immediate",
            "retailer_brand",
            False,
            False,
            True,
            None,
            None,
            "nonflood_weather_consistent",
        ),
        (
            "usable",
            "outside_event_phases",
            "retailer_brand",
            False,
            False,
            False,
            None,
            None,
            "unrelated_or_ambiguous",
        ),
        (
            "missing",
            "immediate",
            "retailer_brand",
            True,
            True,
            False,
            1.0,
            False,
            "insufficient_evidence",
        ),
    ],
)
def test_peak_attribution_categories(
    quality: str,
    peak_phase: str,
    proxy_type: str,
    flood: bool,
    weather: bool,
    nonflood: bool,
    control: float | None,
    national: bool | None,
    expected: str,
) -> None:
    phase = {
        "metric_quality_status": quality,
        "peak_phase": peak_phase,
        "dominant_event_response_phase": (
            peak_phase
            if peak_phase in {"anticipatory", "immediate", "early_recovery", "extended_recovery"}
            else None
        ),
        "dominant_standardized_phase_lift": 1.0,
        "dominant_robust_phase_lift": 1.0,
        "repeat_count": 1,
        "repeat_stability_status": "not_repeated",
        "proxy_type": proxy_type,
    }
    concurrence = {
        "simultaneous_flood_awareness_flag": flood,
        "simultaneous_weather_attention_flag": weather,
        "simultaneous_nonflood_weather_flag": nonflood,
    }
    control_row = (
        {
            "adjusted_event_lift": control,
            "control_count": 3,
            "control_quality_status": "usable",
        }
        if control is not None
        else None
    )
    national_row = {"national_concurrent_spike_flag": national} if national is not None else None
    category, reasons = classify_peak_attribution(
        phase, concurrence, control_row, national_row, _rules()["attribution"]
    )
    assert category == expected
    assert reasons


def test_optional_request_plans_are_bounded_and_deterministic(tmp_path: Path) -> None:
    pilot = tmp_path / "pilot.parquet"
    episodes = [
        _episode("e1", "FL", date(2022, 6, 10), 1),
        _episode("e2", "TX", date(2023, 6, 10), 2),
        _episode("e3", "CA", date(2024, 6, 10), 1),
        _episode("e4", "NY", date(2025, 6, 10), 2),
        _episode("e5", "WA", date(2025, 8, 10), 1),
    ]
    _write(pilot, episodes)
    geography = map_geographies(pilot, tmp_path / "geography")["mapping"]
    controls = tmp_path / "controls.parquet"
    _write(
        controls,
        [
            {
                "treated_episode": episode["provisional_episode_id"],
                "control_state": state,
                "control_rank": rank,
            }
            for episode in episodes
            for rank, state in enumerate(("GA", "SC", "NC"), 1)
        ],
    )
    first = plan_requests(
        pilot,
        geography,
        TERMS,
        RULES,
        tmp_path / "first",
        include_behavioral_state=True,
        include_national=True,
        controls_path=controls,
    )
    second = plan_requests(
        pilot,
        geography,
        TERMS,
        RULES,
        tmp_path / "second",
        include_behavioral_state=True,
        include_national=True,
        controls_path=controls,
    )
    summary = json.loads(first["event_study_summary"].read_text(encoding="utf-8"))
    assert summary == {
        "control_request_count": 30,
        "deduplicated_total_request_count": 75,
        "mini_pilot_episode_count": 5,
        "national_request_count": 10,
        "optional_generation": True,
        "standard_mini_pilot_unique_request_count": 25,
        "treated_request_count": 10,
    }
    assert _sha(first["treated_state_comparison"]) == _sha(second["treated_state_comparison"])
    assert _sha(first["national_comparison"]) == _sha(second["national_comparison"])
    assert _sha(first["control_state_requests"]) == _sha(second["control_state_requests"])
    assert _sha(first["event_study_plan"]) == _sha(second["event_study_plan"])
    assert len(_rows(first["event_study_plan"])) == 50
    national = _rows(first["national_comparison"])
    assert all(row["request_role"] == "national_comparison" for row in national)
    assert all(row["geography"] == "US" for row in national)


def test_phase_output_order_and_hash_are_deterministic(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    _write(plan, [_plan("r2"), _plan("r1")])
    _write(
        observations,
        [
            *_observations("r2", {"walmart": date(2024, 6, 11)}),
            *_observations("r1", {"walmart": date(2024, 6, 9)}),
        ],
    )
    first = calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "one")
    second = calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "two")
    assert _sha(first["phase_metrics"]) == _sha(second["phase_metrics"])
    request_ids = [row["request_id"] for row in _rows(first["phase_metrics"])]
    assert request_ids == sorted(request_ids)


def test_repeats_remain_separate_and_report_instability(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    _write(plan, [_plan("r1")])
    _write(
        observations,
        [
            *_observations(
                "r1",
                {"walmart": date(2024, 6, 4)},
                repeat_id="repeat-1",
                export_attempt_id="attempt-1",
            ),
            *_observations(
                "r1",
                {"walmart": date(2024, 6, 11)},
                repeat_id="repeat-2",
                export_attempt_id="attempt-2",
            ),
        ],
    )
    paths = calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "out")
    rows = _rows(paths["phase_metrics"])
    assert len(rows) == 2
    assert {row["repeat_id"] for row in rows} == {"repeat-1", "repeat-2"}
    assert {row["export_attempt_id"] for row in rows} == {"attempt-1", "attempt-2"}
    assert all(row["repeat_stability_status"] == "unstable" for row in rows)
    diagnostics = _rows(paths["phase_repeat_stability"])[0]
    assert diagnostics["dominant_phase_agreement"] is False


def test_global_baseline_peak_does_not_erase_event_phase_response(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    _write(plan, [_plan("r1")])
    rows = _observations("r1", {"walmart": date(2024, 5, 10)})
    for row in rows:
        day = date.fromisoformat(str(row["date"]))
        if date(2024, 6, 10) <= day <= date(2024, 6, 12):
            row["interest"] = 35.0
    _write(observations, rows)
    metric = _rows(
        calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "out")["phase_metrics"]
    )[0]
    assert metric["global_peak_phase"] == "outside_event_phases"
    assert metric["dominant_event_response_phase"] == "immediate"
    assert metric["dominant_standardized_phase_lift"] > 0.5


def test_response_duration_is_longest_consecutive_qualifying_run(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    observations = tmp_path / "observations.parquet"
    _write(plan, [_plan("r1")])
    rows = _observations("r1", {"walmart": date(2024, 6, 10)})
    for row in rows:
        day = date.fromisoformat(str(row["date"]))
        if date(2024, 6, 10) <= day <= date(2024, 6, 12):
            row["interest"] = 40.0
        elif day == date(2024, 6, 13):
            row["interest"] = 9.0
    _write(observations, rows)
    metric = _rows(
        calculate_phase_metrics(observations, plan, TERMS, RULES, tmp_path / "out")["phase_metrics"]
    )[0]
    assert metric["longest_qualifying_response_run_days"] == 3


def _plan(
    request_id: str,
    role: str = "treated_state_comparison",
    geography: str = "US-FL",
    geography_level: str = "state",
    concepts: str = "walmart",
) -> dict[str, object]:
    return {
        "request_id": request_id,
        "request_role": role,
        "treated_geography": "US-FL",
        "episode_id": "e1",
        "geography": geography,
        "geography_level": geography_level,
        "batch_id": "national_1_retailer_context",
        "comparison_batch_id": "national_1_retailer_context",
        "concept_ids": concepts,
        "terminology_version": "0.8A-v3",
        "event_start_date": "2024-06-10",
        "event_end_date": "2024-06-12",
        "baseline_start_date": "2024-05-06",
        "baseline_end_date": "2024-06-09",
        "post_start_date": "2024-06-13",
        "post_end_date": "2024-07-10",
    }


def _observations(
    request_id: str,
    peaks: dict[str, date],
    repeat_id: str = "initial",
    export_attempt_id: str = "",
) -> list[dict[str, object]]:
    output = []
    start = date(2024, 5, 6)
    for concept_id, peak in peaks.items():
        for offset in range(66):
            day = start + timedelta(days=offset)
            output.append(
                {
                    "request_id": request_id,
                    "repeat_id": repeat_id,
                    "export_attempt_id": export_attempt_id,
                    "concept_id": concept_id,
                    "date": day.isoformat(),
                    "interest": 80.0 if day == peak else 9.0 + offset % 3,
                    "is_partial": False,
                }
            )
    return output


def _episode(episode_id: str, state: str, start: date, member_count: int = 1) -> dict[str, object]:
    return {
        "provisional_episode_id": episode_id,
        "primary_state": state,
        "states": state,
        "start_date": start.isoformat(),
        "end_date": start.isoformat(),
        "member_count": member_count,
        "candidate_split": "development",
        "representative_latitude": 30.0,
        "representative_longitude": -90.0,
        "duration_days": 0,
        "union_area_km2": 20.0,
        "provisional_catalog_status": "retained_provisionally",
        "manual_review_flag": False,
        "split_boundary_flag": False,
        "selection_rank": int(episode_id.removeprefix("e")),
    }


def _metadata(available: set[str]) -> list[dict[str, object]]:
    return [
        {
            "state_code": state,
            "census_region": "South" if state in {"TX", "OK", "LA"} else "West",
            "population_tier": "medium",
            "climate_class": "humid",
            "coastal_state_flag": state in {"CA", "LA"},
            "observed_trends_availability": state in available,
            "metadata_source": "synthetic test metadata",
            "metadata_version": "test-v1",
        }
        for state in sorted(VALID_STATES)
    ]


def _phase_row(request_id: str, role: str, geography: str, lift: float) -> dict[str, object]:
    return {
        "provisional_episode_id": "e1",
        "request_id": request_id,
        "request_role": role,
        "geography": geography,
        "geography_level": "state",
        "batch_id": "national_1_retailer_context",
        "comparison_batch_id": "national_1_retailer_context",
        "concept_id": "walmart",
        "terminology_version": "0.8A-v3",
        "baseline_mean": 10.0,
        **{
            f"{phase}_standardized_lift": lift
            for phase in ("anticipatory", "immediate", "early_recovery", "extended_recovery")
        },
    }


def _rules() -> dict[str, object]:
    return yaml.safe_load(RULES.read_text(encoding="utf-8"))


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
