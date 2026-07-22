from __future__ import annotations

from typing import Any

import pyarrow as pa
from scripts.audit_analysis_dataset import (
    MINIMUM_REPEATS,
    _audit_recommendations,
    _matched_controls_available,
    _metric_viability,
    _repeat_coverage,
    _required_upstream_additions,
)

from geodemand.analysis_descriptive import PAIR_SIZE


def _metric_rows(request_count: int) -> list[dict[str, Any]]:
    return [
        {
            "request_id": f"request_{index:02d}",
            "repeat_id": "repeat_1",
            "concept_id": "flood",
            "episode_id": f"episode_{index:02d}",
            "geography": f"US-{index:02d}",
            "response": float(index),
        }
        for index in range(request_count)
    ]


def _response_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    table = pa.Table.from_pylist(rows)
    return next(row for row in _metric_viability(table, rows) if row["metric"] == "response")


def test_population_inference_reason_distinguishes_one_five_and_twenty_requests() -> None:
    one = _response_audit(_metric_rows(1))
    five = _response_audit(_metric_rows(5))
    twenty = _response_audit(_metric_rows(20))

    assert one["independent_request_count"] == 1
    assert "insufficient_independent_requests_1_below_20" in one["ineligibility_reasons"]
    assert five["independent_request_count"] == 5
    assert "insufficient_independent_requests_5_below_20" in five["ineligibility_reasons"]
    assert twenty["independent_request_count"] == 20
    assert twenty["population_inference"] == "cluster_count_threshold_met"
    assert "insufficient_independent_requests" not in twenty["ineligibility_reasons"]


def test_metric_cluster_counts_use_only_valid_finite_rows() -> None:
    rows = _metric_rows(5)
    rows[3]["response"] = None
    rows[4]["response"] = float("inf")

    audit = _response_audit(rows)

    assert audit["independent_request_count"] == 3
    assert audit["independent_episode_count"] == 3
    assert audit["independent_geography_count"] == 3
    assert "insufficient_independent_requests_3_below_20" in audit["ineligibility_reasons"]


def test_five_repeat_groups_are_nested_in_one_repeated_request() -> None:
    rows = [
        {
            "request_id": "florida",
            "repeat_id": repeat_id,
            "concept_id": f"concept_{concept_index}",
            "episode_id": "florida_episode",
            "geography": "US-FL",
            "response": float(concept_index + repeat_index),
        }
        for concept_index in range(5)
        for repeat_index, repeat_id in enumerate(("repeat_1", "repeat_2"))
    ]

    audit = _response_audit(rows)
    coverage = _repeat_coverage(rows)

    assert audit["repeated_request_count"] == 1
    assert audit["repeat_eligible_request_concept_group_count"] == 5
    assert audit["repeat_reliability"] == "exploratory_pairwise_only"
    assert sum(row["eligible_for_pairwise_repeat_agreement"] == "yes" for row in coverage) == 5
    assert {row["nesting_interpretation"] for row in coverage} == {
        "concept_nested_within_pairwise_repeated_request"
    }


def test_required_upstream_additions_respect_completed_lineage() -> None:
    complete = _required_upstream_additions(
        join_summary={"all_required_joins_matched": True},
        unmatched_record_count=0,
        independent_request_count=5,
    )
    incomplete = _required_upstream_additions(
        join_summary={"all_required_joins_matched": False},
        unmatched_record_count=1,
        independent_request_count=5,
    )

    assert not any("completed request-plan" in item for item in complete)
    assert any("completed request-plan" in item for item in incomplete)


def test_audit_recommendations_are_dynamic_for_repeat_and_cluster_counts() -> None:
    metric_rows = [_response_audit(_metric_rows(20))]
    one_repeat = _audit_recommendations(
        independent_request_count=5,
        independent_episode_count=5,
        independent_geography_count=5,
        repeated_request_count=1,
        eligible_repeat_group_count=5,
        lineage_complete=True,
        matched_controls_available=False,
        metric_viability=metric_rows,
    )
    multiple_repeats = _audit_recommendations(
        independent_request_count=20,
        independent_episode_count=20,
        independent_geography_count=5,
        repeated_request_count=3,
        eligible_repeat_group_count=15,
        lineage_complete=True,
        matched_controls_available=True,
        metric_viability=metric_rows,
    )

    assert "5_independent_requests_below_20" in one_repeat["current_batch_inference_status"]
    assert "1_repeated_requests" in one_repeat["repeat_diagnostic_status"]
    assert "3_repeated_requests" in multiple_repeats["repeat_diagnostic_status"]
    assert "one repeated request" not in multiple_repeats["repeat_diagnostic_status"]
    assert multiple_repeats["cluster_count_threshold_met"] is True
    assert "conditional" in multiple_repeats["current_batch_inference_status"]


def test_audit_recommendations_track_lineage_and_matched_controls() -> None:
    metric_rows = [_response_audit(_metric_rows(20))]
    complete = _audit_recommendations(
        independent_request_count=20,
        independent_episode_count=20,
        independent_geography_count=5,
        repeated_request_count=0,
        eligible_repeat_group_count=0,
        lineage_complete=True,
        matched_controls_available=True,
        metric_viability=metric_rows,
    )
    incomplete = _audit_recommendations(
        independent_request_count=20,
        independent_episode_count=20,
        independent_geography_count=5,
        repeated_request_count=0,
        eligible_repeat_group_count=0,
        lineage_complete=False,
        matched_controls_available=False,
        metric_viability=metric_rows,
    )

    assert complete["design_requirements"]["lineage_complete"] is True
    assert complete["design_requirements"]["matched_controls_available"] is True
    assert incomplete["design_requirements"]["lineage_complete"] is False
    assert incomplete["design_requirements"]["matched_controls_available"] is False


def test_matched_control_availability_requires_shared_episode_and_concept() -> None:
    treated = {
        "episode_id": "episode",
        "concept_id": "flood",
        "request_role": "treated_state",
    }
    unmatched_control = {
        "episode_id": "other_episode",
        "concept_id": "flood",
        "request_role": "comparison_state",
    }
    matched_control = {
        "episode_id": "episode",
        "concept_id": "flood",
        "request_role": "comparison_state",
    }

    assert _matched_controls_available([treated, unmatched_control]) is False
    assert _matched_controls_available([treated, matched_control]) is True


def test_audit_pairwise_contract_excludes_three_repeat_groups() -> None:
    rows = [
        {
            "request_id": "request",
            "repeat_id": f"repeat_{index}",
            "concept_id": "flood",
            "episode_id": "episode",
            "geography": "US-FL",
            "response": float(index),
        }
        for index in range(1, 4)
    ]

    audit = _response_audit(rows)
    coverage = _repeat_coverage(rows)

    assert audit["repeat_eligible_request_concept_group_count"] == 0
    assert audit["multi_repeat_request_concept_group_count"] == 1
    assert audit["repeat_reliability"] == "multi_repeat_not_pairwise_eligible"
    assert coverage[0]["repeat_design_status"] == "multi_repeat_not_pairwise_eligible"
    assert coverage[0]["eligible_for_pairwise_repeat_agreement"] == "no"


def test_audit_and_production_pair_size_contracts_match() -> None:
    assert MINIMUM_REPEATS == PAIR_SIZE == 2
