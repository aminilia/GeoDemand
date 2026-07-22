from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

import geodemand.analysis_descriptive as descriptive_module
from geodemand.analysis import ANALYSIS_DATASET_SCHEMA
from geodemand.analysis_descriptive import (
    DESCRIPTIVE_METRICS,
    EXCLUSION_FIELDS,
    GROUP_SUMMARY_SCHEMA,
    PAIR_SCHEMA,
    RANK_SCHEMA,
    REPEAT_DIAGNOSTIC_METRICS,
    REQUEST_SUMMARY_SCHEMA,
    DescriptiveAnalysisError,
    _average_ranks,
    describe_signal,
)
from geodemand.cli import app


def test_describe_signal_cli_help_and_registration() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["analysis", "describe-signal", "--help"])
    group = runner.invoke(app, ["analysis", "--help"])

    assert result.exit_code == 0
    assert (
        "Generate deterministic descriptive and paired-repeat signal diagnostics." in result.output
    )
    assert "--dataset" in result.output
    assert "--output-root" in result.output
    assert "describe-signal" in group.output
    assert "validate-signal" not in group.output


def test_describe_signal_summaries_pairs_ranks_and_determinism(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, _paired_rows())

    first = describe_signal(dataset_path=dataset, output_root=tmp_path / "first")
    second = describe_signal(dataset_path=dataset, output_root=tmp_path / "second")

    assert {name: _sha(path) for name, path in first.items()} == {
        name: _sha(path) for name, path in second.items()
    }
    summary = json.loads(first["descriptive_summary"].read_text(encoding="utf-8"))
    assert summary["independent_request_count"] == 1
    assert summary["repeated_request_count"] == 1
    assert summary["repeat_eligible_request_concept_group_count"] == 5
    assert summary["output_row_counts"]["paired_repeat_diagnostics"] == (
        5 * len(REPEAT_DIAGNOSTIC_METRICS)
    )
    assert len(pq.read_table(first["descriptive_signal_summary"])) == len(DESCRIPTIVE_METRICS)
    assert pq.read_table(first["request_concept_summary"]).schema == REQUEST_SUMMARY_SCHEMA
    assert pq.read_table(first["paired_repeat_diagnostics"]).schema == PAIR_SCHEMA
    ranks = pq.read_table(first["concept_rank_agreement"]).to_pylist()
    complete_rank = next(row for row in ranks if row["metric"] == "absolute_peak_lift")
    assert complete_rank["common_concept_count"] == 5
    assert complete_rank["eligible"] is True
    assert complete_rank["spearman_rank_correlation"] == pytest.approx(1.0)


def test_request_summary_distinguishes_one_and_two_repeats(tmp_path: Path) -> None:
    rows = _paired_rows()[:2] + [_row("other", "repeat_1", "flood", 4.0)]
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")
    summaries = pq.read_table(result["request_concept_summary"]).to_pylist()
    paired = next(
        row
        for row in summaries
        if row["request_id"] == "florida" and row["metric"] == "absolute_peak_lift"
    )
    single = next(
        row
        for row in summaries
        if row["request_id"] == "other" and row["metric"] == "absolute_peak_lift"
    )

    assert paired["repeat_count"] == 2
    assert paired["within_request_standard_deviation"] is not None
    assert single["repeat_count"] == 1
    assert single["within_request_standard_deviation"] is None


@pytest.mark.parametrize("bad_value", [None, "", "   "])
def test_describe_signal_rejects_invalid_keys(tmp_path: Path, bad_value: str | None) -> None:
    row = _row("request", "repeat_1", "flood", 1.0)
    row["request_id"] = bad_value
    dataset = _dataset(tmp_path, [row])

    with pytest.raises(DescriptiveAnalysisError, match="descriptive_invalid_key"):
        describe_signal(dataset_path=dataset, output_root=tmp_path / "out")


def test_describe_signal_rejects_duplicate_grain(tmp_path: Path) -> None:
    row = _row("request", "repeat_1", "flood", 1.0)
    dataset = _dataset(tmp_path, [row, dict(row)])

    with pytest.raises(DescriptiveAnalysisError, match="descriptive_duplicate_grain"):
        describe_signal(dataset_path=dataset, output_root=tmp_path / "out")


def test_describe_signal_rejects_missing_fields_and_suffix(tmp_path: Path) -> None:
    missing = tmp_path / "missing.parquet"
    pq.write_table(pa.table({"request_id": ["request"]}), missing)
    unsupported = tmp_path / "dataset.csv"
    unsupported.write_text("x\n", encoding="utf-8")

    with pytest.raises(DescriptiveAnalysisError, match="descriptive_input_missing_fields"):
        describe_signal(dataset_path=missing, output_root=tmp_path / "missing-out")
    with pytest.raises(DescriptiveAnalysisError, match="descriptive_input_read_error"):
        describe_signal(dataset_path=unsupported, output_root=tmp_path / "suffix-out")


def test_describe_signal_translates_malformed_parquet(tmp_path: Path) -> None:
    malformed = tmp_path / "malformed.parquet"
    malformed.write_bytes(b"not parquet")

    with pytest.raises(DescriptiveAnalysisError, match="descriptive_input_read_error"):
        describe_signal(dataset_path=malformed, output_root=tmp_path / "out")


def test_empty_outputs_keep_explicit_schemas_and_csv_header(tmp_path: Path) -> None:
    dataset = tmp_path / "empty.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=ANALYSIS_DATASET_SCHEMA), dataset)

    result = describe_signal(dataset_path=dataset, output_root=tmp_path / "out")

    assert pq.read_table(result["paired_repeat_diagnostics"]).schema == PAIR_SCHEMA
    assert pq.read_table(result["concept_rank_agreement"]).schema == RANK_SCHEMA
    assert pq.read_table(result["geography_concept_summary"]).schema == GROUP_SUMMARY_SCHEMA
    with result["descriptive_exclusions"].open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        assert next(reader) == list(EXCLUSION_FIELDS)


def test_relative_difference_zero_guard_negative_values_and_null_exclusions(
    tmp_path: Path,
) -> None:
    rows = [
        _row("florida", "repeat_1", "flood", 0.0),
        _row("florida", "repeat_2", "flood", -2.0),
    ]
    rows[0]["robust_peak_lift"] = None
    rows[1]["z_score_peak_lift"] = float("inf")
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")
    pairs = pq.read_table(result["paired_repeat_diagnostics"]).to_pylist()
    absolute = next(row for row in pairs if row["metric"] == "absolute_peak_lift")
    exclusions = list(
        csv.DictReader(result["descriptive_exclusions"].open(encoding="utf-8", newline=""))
    )

    assert absolute["signed_difference"] == -2.0
    assert absolute["relative_difference"] is None
    assert absolute["relative_difference_exclusion_reason"] == "zero_or_near_zero_denominator"
    assert any(row["reason"] == "missing_value" for row in exclusions)
    assert any(row["reason"] == "non_finite_value" for row in exclusions)
    assert any(row["reason"] == "fewer_than_two_valid_repeats" for row in exclusions)


def test_conflicting_group_lineage_is_rejected(tmp_path: Path) -> None:
    rows = [
        _row("request", "repeat_1", "flood", 1.0),
        _row("request", "repeat_2", "flood", 2.0),
    ]
    rows[1]["geography"] = "US-KS"

    with pytest.raises(DescriptiveAnalysisError, match="conflicting_lineage"):
        describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")


def test_tied_rank_behavior_is_deterministic() -> None:
    assert _average_ranks([1.0, 1.0, 3.0, 4.0]) == [1.5, 1.5, 3.0, 4.0]


def test_insufficient_common_concepts_are_reported(tmp_path: Path) -> None:
    rows = [
        _row("request", repeat, concept, float(index))
        for index, concept in enumerate(("flood", "weather"))
        for repeat in ("repeat_1", "repeat_2")
    ]
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")
    ranks = pq.read_table(result["concept_rank_agreement"]).to_pylist()

    assert ranks
    assert all(row["eligible"] is False for row in ranks)
    assert {row["exclusion_reason"] for row in ranks} == {
        "insufficient_common_concepts_for_rank_agreement"
    }


def test_multiple_repeated_requests_have_dynamic_warning(tmp_path: Path) -> None:
    rows = _paired_rows("first") + _paired_rows("second")
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")
    summary = json.loads(result["descriptive_summary"].read_text(encoding="utf-8"))
    warnings = " ".join(summary["warnings"])

    assert summary["repeated_request_count"] == 2
    assert "2 repeated requests" in warnings
    assert "one repeated request" not in warnings
    assert "p-values" in warnings


def test_twenty_independent_requests_do_not_trigger_stale_repeat_wording(
    tmp_path: Path,
) -> None:
    rows = [_row(f"request_{index}", "repeat_1", "flood", float(index)) for index in range(20)]
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")
    summary = json.loads(result["descriptive_summary"].read_text(encoding="utf-8"))
    warnings = " ".join(summary["warnings"])

    assert summary["independent_request_count"] == 20
    assert summary["repeated_request_count"] == 0
    assert "one repeated request" not in warnings
    assert not any(
        forbidden in result
        for forbidden in ("validate_signal", "bootstrap", "permutation", "icc", "p_value")
    )


def test_principal_summaries_weight_request_concept_units_equally(tmp_path: Path) -> None:
    rows = [
        _row("many", "repeat_1", "flood", 0.0),
        _row("many", "repeat_2", "flood", 0.0),
        _row("many", "repeat_3", "flood", 0.0),
        _row("single", "repeat_1", "flood", 100.0),
    ]
    for row in rows:
        row["episode_id"] = "shared_episode"
        row["geography"] = "US-FL"
        row["semantic_family"] = "shared_family"
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")

    dataset_summary = _metric_row(result["descriptive_signal_summary"], "absolute_peak_lift")
    geography_summary = _metric_row(result["geography_concept_summary"], "absolute_peak_lift")
    episode_summary = _metric_row(result["episode_concept_summary"], "absolute_peak_lift")
    family_summary = _metric_row(result["semantic_family_summary"], "absolute_peak_lift")

    for summary in (
        dataset_summary,
        geography_summary,
        episode_summary,
        family_summary,
    ):
        assert summary["mean"] == 50.0
        assert summary["raw_repeat_row_count"] == 4
        assert summary["valid_repeat_value_count"] == 4
        assert summary["request_concept_unit_count"] == 2
        assert summary["valid_request_concept_unit_count"] == 2
        assert summary["independent_request_count"] == 2


def test_semantic_family_summary_does_not_weight_repeated_concept_more(
    tmp_path: Path,
) -> None:
    rows = [
        _row("many", "repeat_1", "flood", 0.0),
        _row("many", "repeat_2", "flood", 0.0),
        _row("many", "repeat_3", "flood", 0.0),
        _row("single", "repeat_1", "weather", 100.0),
    ]
    for row in rows:
        row["semantic_family"] = "shared_family"
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")
    summary = _metric_row(result["semantic_family_summary"], "absolute_peak_lift")

    assert summary["mean"] == 50.0
    assert summary["concept_count"] == 2
    assert summary["request_concept_unit_count"] == 2


def test_three_repeat_rank_and_pair_contract_is_explicit(tmp_path: Path) -> None:
    rows = [
        _row("request", repeat_id, concept, float(index + repeat_index))
        for index, concept in enumerate(("rain", "weather", "flood"), start=1)
        for repeat_index, repeat_id in enumerate(("repeat_1", "repeat_2", "repeat_3"))
    ]
    result = describe_signal(dataset_path=_dataset(tmp_path, rows), output_root=tmp_path / "out")
    summary = json.loads(result["descriptive_summary"].read_text(encoding="utf-8"))
    ranks = pq.read_table(result["concept_rank_agreement"]).to_pylist()
    pairs = pq.read_table(result["paired_repeat_diagnostics"]).to_pylist()
    exclusions = list(
        csv.DictReader(result["descriptive_exclusions"].open(encoding="utf-8", newline=""))
    )

    assert pairs == []
    assert summary["repeat_eligible_request_concept_group_count"] == 0
    assert len(ranks) == len(REPEAT_DIAGNOSTIC_METRICS)
    assert {row["exclusion_reason"] for row in ranks} == {
        "more_than_two_repeats_requires_multi_repeat_rank_summary"
    }
    assert any(
        row["reason"] == "more_than_two_repeats_requires_nonpaired_summary" for row in exclusions
    )


def _metric_row(path: Path, metric: str) -> dict[str, Any]:
    return next(row for row in pq.read_table(path).to_pylist() if row["metric"] == metric)


def test_output_write_errors_are_translated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset = _dataset(tmp_path, [_row("request", "repeat_1", "flood", 1.0)])

    def fail_write(*_args: object, **_kwargs: object) -> None:
        raise OSError("denied")

    monkeypatch.setattr(descriptive_module.pq, "write_table", fail_write)
    with pytest.raises(DescriptiveAnalysisError, match="descriptive_output_write_error"):
        describe_signal(dataset_path=dataset, output_root=tmp_path / "out")


def _paired_rows(request_id: str = "florida") -> list[dict[str, Any]]:
    concepts = ("context_heavy_rain", "context_weather", "flash_flood", "flood", "flooding")
    return [
        _row(request_id, repeat_id, concept, float(index + repeat_index))
        for index, concept in enumerate(concepts, start=1)
        for repeat_index, repeat_id in enumerate(("repeat_1", "repeat_2"))
    ]


def _row(request_id: str, repeat_id: str, concept_id: str, value: float) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for field in ANALYSIS_DATASET_SCHEMA:
        if pa.types.is_string(field.type):
            row[field.name] = "value"
        elif pa.types.is_boolean(field.type):
            row[field.name] = False
        elif pa.types.is_integer(field.type):
            row[field.name] = 1
        else:
            row[field.name] = value
    row.update(
        {
            "schema_version": "1.0A-v1",
            "request_id": request_id,
            "repeat_id": repeat_id,
            "concept_id": concept_id,
            "episode_id": f"episode_{request_id}",
            "geography": "US-FL" if request_id in {"florida", "first"} else "US-WA",
            "semantic_family": "context" if concept_id.startswith("context") else "flood",
            "request_role": "treated_state",
            "plan_join_status": "matched",
            "repeat_metrics_join_status": "matched",
            "episode_metrics_join_status": "matched",
        }
    )
    for metric in set(DESCRIPTIVE_METRICS) | set(REPEAT_DIAGNOSTIC_METRICS):
        row[metric] = value
    row["peak_lead_lag_days"] = int(value)
    return row


def _dataset(tmp_path: Path, rows: list[dict[str, Any]]) -> Path:
    path = tmp_path / f"dataset-{len(list(tmp_path.glob('dataset-*.parquet')))}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=ANALYSIS_DATASET_SCHEMA), path)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
