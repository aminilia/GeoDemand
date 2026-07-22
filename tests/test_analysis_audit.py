from __future__ import annotations

from typing import Any

import pyarrow as pa
from scripts.audit_analysis_dataset import (
    _metric_viability,
    _repeat_coverage,
    _required_upstream_additions,
)


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
        "concept_nested_within_repeated_request"
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
