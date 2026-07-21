from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

import geodemand.analysis as analysis_module
from geodemand.analysis import AnalysisError, build_analysis_dataset
from geodemand.cli import app
from geodemand.trends import TrendsError, read_request_plan_rows
from geodemand.trends_event_study import calculate_phase_metrics

ROOT = Path(__file__).parents[1]
RULES = ROOT / "config" / "trends_rules.yaml"
TERMS = ROOT / "config" / "trends_terms.yaml"


def test_build_analysis_dataset_is_deterministic_and_preserves_repeats(
    tmp_path: Path,
) -> None:
    inputs = _analysis_inputs(tmp_path)

    first = build_analysis_dataset(**inputs, output_root=tmp_path / "first")
    second = build_analysis_dataset(**inputs, output_root=tmp_path / "second")

    assert _sha(first["dataset"]) == _sha(second["dataset"])
    assert _sha(first["join_summary"]) == _sha(second["join_summary"])
    rows = _rows(first["dataset"])
    assert len(rows) == 2
    assert {row["request_id"] for row in rows} == {_LONG_REQUEST_ID}
    assert {row["repeat_id"] for row in rows} == {"repeat_1", "repeat_2"}
    assert {row["concept_id"] for row in rows} == {"flood"}
    assert all(row["plan_join_status"] == "matched" for row in rows)
    assert all(row["repeat_metrics_join_status"] == "matched" for row in rows)
    assert all(row["episode_metrics_join_status"] == "matched" for row in rows)


def test_build_analysis_dataset_accepts_csv_request_plan_with_bom(
    tmp_path: Path,
) -> None:
    inputs = _analysis_inputs(tmp_path, plan_format="csv-bom")

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    summary = json.loads(result["join_summary"].read_text(encoding="utf-8"))
    assert summary["all_required_joins_matched"] is True
    assert summary["row_counts"]["analysis_dataset_rows"] == 2


def test_build_analysis_dataset_rejects_duplicate_normalized_keys(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    duplicate = _repeat_metric_rows()
    duplicate.append(dict(duplicate[0]))
    _write_parquet(inputs["repeat_metrics_path"], duplicate)

    try:
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")
    except AnalysisError as exc:
        message = str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected duplicate normalized key failure.")

    assert "duplicate_normalized_keys" in message
    assert "repeat_response_metric_key" in message


def test_build_analysis_dataset_reports_unmatched_keys(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    rows = _repeat_metric_rows()
    rows.pop()
    _write_parquet(inputs["repeat_metrics_path"], rows)

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    summary = json.loads(result["join_summary"].read_text(encoding="utf-8"))
    unmatched = _csv_rows(result["unmatched_keys"])
    assert summary["all_required_joins_matched"] is False
    assert summary["unmatched_counts"]["repeat_metrics_join_status"] == 1
    assert len(unmatched) == 1
    assert unmatched[0]["repeat_id"] == "repeat_2"


def test_analysis_build_dataset_cli(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    output = tmp_path / "cli-out"

    result = CliRunner().invoke(
        app,
        [
            "analysis",
            "build-dataset",
            "--request-plan",
            str(inputs["request_plan_path"]),
            "--episode-metrics",
            str(inputs["episode_metrics_path"]),
            "--repeat-metrics",
            str(inputs["repeat_metrics_path"]),
            "--phase-metrics",
            str(inputs["phase_metrics_path"]),
            "--output-root",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert (output / "analysis_dataset.parquet").exists()
    assert "join_summary" in result.output


def test_analysis_build_dataset_cli_reports_duplicate_keys(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    duplicate = _phase_metric_rows()
    duplicate.append(dict(duplicate[0]))
    _write_parquet(inputs["phase_metrics_path"], duplicate)

    result = CliRunner().invoke(
        app,
        [
            "analysis",
            "build-dataset",
            "--request-plan",
            str(inputs["request_plan_path"]),
            "--episode-metrics",
            str(inputs["episode_metrics_path"]),
            "--repeat-metrics",
            str(inputs["repeat_metrics_path"]),
            "--phase-metrics",
            str(inputs["phase_metrics_path"]),
            "--output-root",
            str(tmp_path / "out"),
        ],
    )

    assert result.exit_code != 0
    assert "duplicate_normalized_keys" in result.output


def test_analysis_csv_parquet_and_pq_plans_are_equivalent(tmp_path: Path) -> None:
    parquet_inputs = _analysis_inputs(tmp_path / "parquet")
    csv_inputs = _analysis_inputs(tmp_path / "csv", plan_format="csv")
    pq_inputs = _analysis_inputs(tmp_path / "pq")
    pq_plan = pq_inputs["request_plan_path"].with_suffix(".pq")
    pq_inputs["request_plan_path"].replace(pq_plan)
    pq_inputs["request_plan_path"] = pq_plan

    outputs = [
        build_analysis_dataset(**inputs, output_root=tmp_path / f"out-{index}")["dataset"]
        for index, inputs in enumerate((parquet_inputs, csv_inputs, pq_inputs))
    ]

    assert [_rows(path) for path in outputs[1:]] == [_rows(outputs[0]), _rows(outputs[0])]


@pytest.mark.parametrize(
    ("input_name", "field"),
    [
        ("request_plan_path", "request_id"),
        ("episode_metrics_path", "concept_id"),
        ("repeat_metrics_path", "repeat_id"),
        ("phase_metrics_path", "request_id"),
    ],
)
def test_analysis_rejects_empty_normalized_keys(
    tmp_path: Path, input_name: str, field: str
) -> None:
    inputs = _analysis_inputs(tmp_path)
    path = inputs[input_name]
    rows = _rows(path)
    rows[0][field] = "   "
    _write_parquet(path, rows)

    with pytest.raises(AnalysisError, match="empty_key|empty_required_values"):
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")


def test_analysis_rejects_missing_minimum_schema(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    table = pq.read_table(inputs["repeat_metrics_path"]).drop(["baseline_mean"])
    pq.write_table(table, inputs["repeat_metrics_path"])

    with pytest.raises(AnalysisError, match="analysis_input_missing_fields.*baseline_mean"):
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")


def test_analysis_rejects_lineage_conflict(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    rows = _phase_metric_rows()
    rows[0]["geography"] = "US-GA"
    _write_parquet(inputs["phase_metrics_path"], rows)

    with pytest.raises(AnalysisError, match="analysis_lineage_conflict.*geography"):
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")


def test_analysis_rejects_unplanned_concept(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    rows = _rows(inputs["request_plan_path"])
    for row in rows:
        row["concept_ids"] = "context_weather"
    _write_parquet(inputs["request_plan_path"], rows)

    with pytest.raises(AnalysisError, match="analysis_unplanned_concept"):
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")


def test_analysis_reports_reverse_orphans(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    repeat_rows = _repeat_metric_rows()
    orphan = dict(repeat_rows[0], request_id="orphan-request")
    repeat_rows.append(orphan)
    _write_parquet(inputs["repeat_metrics_path"], repeat_rows)

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    unmatched = _csv_rows(result["unmatched_keys"])
    summary = json.loads(result["join_summary"].read_text(encoding="utf-8"))
    assert any(
        row["source"] == "repeat_response_metrics" and row["direction"] == "orphan_input"
        for row in unmatched
    )
    assert summary["orphan_counts"]["repeat_response_metrics"] == 1


def test_analysis_empty_phase_input_reports_orphans_and_keeps_schema(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    phase_table = pq.read_table(inputs["phase_metrics_path"]).slice(0, 0)
    pq.write_table(phase_table, inputs["phase_metrics_path"])

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    assert pq.read_table(result["dataset"]).num_rows == 0
    assert pq.read_table(result["dataset"]).column_names[:4] == [
        "schema_version",
        "request_id",
        "repeat_id",
        "concept_id",
    ]
    assert _csv_rows(result["unmatched_keys"])


def test_analysis_writes_fixed_empty_unmatched_header(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    assert result["unmatched_keys"].read_text(encoding="utf-8") == (
        "source,direction,request_id,repeat_id,concept_id,missing_join\n"
    )


@pytest.mark.parametrize("suffix", [".csv", ".txt"])
def test_analysis_rejects_unsupported_metric_suffix(tmp_path: Path, suffix: str) -> None:
    inputs = _analysis_inputs(tmp_path)
    original = inputs["repeat_metrics_path"]
    unsupported = original.with_suffix(suffix)
    original.replace(unsupported)
    inputs["repeat_metrics_path"] = unsupported

    with pytest.raises(AnalysisError, match="unsupported_analysis_metric_format"):
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")


def test_analysis_translates_malformed_parquet_for_api_and_cli(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    malformed = inputs["repeat_metrics_path"]
    malformed.write_bytes(b"not parquet")

    with pytest.raises(AnalysisError, match="analysis_input_read_error") as raised:
        build_analysis_dataset(**inputs, output_root=tmp_path / "api-out")
    assert type(raised.value) is AnalysisError
    result = CliRunner().invoke(
        app,
        [
            "analysis",
            "build-dataset",
            "--request-plan",
            str(inputs["request_plan_path"]),
            "--episode-metrics",
            str(inputs["episode_metrics_path"]),
            "--repeat-metrics",
            str(malformed),
            "--phase-metrics",
            str(inputs["phase_metrics_path"]),
            "--output-root",
            str(tmp_path / "cli-out"),
        ],
    )
    assert result.exit_code != 0
    assert "analysis_input_read_error" in result.output
    assert "ArrowInvalid" not in result.output and "ArrowIOError" not in result.output


def test_analysis_rejects_ambiguous_fallback_plan_rows(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    rows = _rows(inputs["request_plan_path"])
    rows[0]["planned_repeat_id"] = ""
    _write_parquet(inputs["request_plan_path"], rows)

    with pytest.raises(AnalysisError, match="ambiguous_request_plan_fallback"):
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")


def test_analysis_dataset_column_order_is_deterministic(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    first = build_analysis_dataset(**inputs, output_root=tmp_path / "one")["dataset"]
    second = build_analysis_dataset(**inputs, output_root=tmp_path / "two")["dataset"]

    assert pq.read_table(first).column_names == pq.read_table(second).column_names
    assert "treated_geography" in pq.read_table(first).column_names
    assert "immediate_valid_day_count" in pq.read_table(first).column_names


def test_analysis_reports_one_missing_concept_from_multi_concept_plan(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    plans = _rows(inputs["request_plan_path"])
    for plan in plans:
        plan["concept_ids"] = "flood;context_weather"
    _write_parquet(inputs["request_plan_path"], plans)
    phases = _phase_metric_rows()
    phases.append(dict(phases[0], concept_id="context_weather"))
    _write_parquet(inputs["phase_metrics_path"], phases)

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    missing = [
        row for row in _csv_rows(result["unmatched_keys"]) if row["source"] == "request_plan"
    ]
    assert [(row["repeat_id"], row["concept_id"]) for row in missing] == [
        ("repeat_2", "context_weather")
    ]


def test_analysis_reports_multiple_missing_planned_concepts(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    plans = _rows(inputs["request_plan_path"])
    for plan in plans:
        plan["concept_ids"] = "flood;context_weather;context_rain"
    _write_parquet(inputs["request_plan_path"], plans)

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    missing = [
        (row["repeat_id"], row["concept_id"])
        for row in _csv_rows(result["unmatched_keys"])
        if row["source"] == "request_plan"
    ]
    assert missing == [
        ("repeat_1", "context_rain"),
        ("repeat_1", "context_weather"),
        ("repeat_2", "context_rain"),
        ("repeat_2", "context_weather"),
    ]
    summary = json.loads(result["join_summary"].read_text(encoding="utf-8"))
    assert summary["orphan_counts"]["request_plan_missing_concepts"] == 4


def test_analysis_fallback_plan_requires_concepts_for_each_applicable_repeat(
    tmp_path: Path,
) -> None:
    inputs = _analysis_inputs(tmp_path)
    fallback = dict(_plan_rows()[0], planned_repeat_id="", concept_ids="flood;context_weather")
    _write_parquet(inputs["request_plan_path"], [fallback])
    phases = _phase_metric_rows()
    phases.append(dict(phases[0], concept_id="context_weather"))
    _write_parquet(inputs["phase_metrics_path"], phases)

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")

    missing = [
        row for row in _csv_rows(result["unmatched_keys"]) if row["source"] == "request_plan"
    ]
    assert [(row["repeat_id"], row["concept_id"]) for row in missing] == [
        ("repeat_2", "context_weather")
    ]


def test_upstream_empty_phase_output_can_feed_analysis(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    observations = tmp_path / "empty_observations.parquet"
    observation_schema = pa.schema(
        [
            pa.field("request_id", pa.string()),
            pa.field("repeat_id", pa.string()),
            pa.field("export_attempt_id", pa.string()),
            pa.field("concept_id", pa.string()),
            pa.field("date", pa.string()),
            pa.field("interest", pa.float64()),
            pa.field("is_partial", pa.bool_()),
        ]
    )
    pq.write_table(pa.Table.from_pylist([], schema=observation_schema), observations)
    upstream = calculate_phase_metrics(
        observations,
        inputs["request_plan_path"],
        TERMS,
        RULES,
        tmp_path / "phase-output",
    )
    inputs["phase_metrics_path"] = upstream["phase_metrics"]

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "analysis-output")

    assert pq.read_table(upstream["phase_metrics"]).num_rows == 0
    assert set(analysis_module.PHASE_METRIC_FIELDS) <= set(
        pq.read_table(upstream["phase_metrics"]).column_names
    )
    assert pq.read_table(result["dataset"]).num_rows == 0


def test_analysis_empty_and_nonempty_outputs_have_identical_explicit_schema(
    tmp_path: Path,
) -> None:
    populated_inputs = _analysis_inputs(tmp_path / "populated")
    empty_inputs = _analysis_inputs(tmp_path / "empty")
    empty_phase = pq.read_table(empty_inputs["phase_metrics_path"]).slice(0, 0)
    pq.write_table(empty_phase, empty_inputs["phase_metrics_path"])

    populated = build_analysis_dataset(
        **populated_inputs, output_root=tmp_path / "populated-output"
    )["dataset"]
    empty = build_analysis_dataset(**empty_inputs, output_root=tmp_path / "empty-output")["dataset"]

    assert pq.read_schema(populated) == analysis_module.ANALYSIS_DATASET_SCHEMA
    assert pq.read_schema(empty) == analysis_module.ANALYSIS_DATASET_SCHEMA


def test_analysis_schema_has_expected_arrow_types(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    dataset = build_analysis_dataset(**inputs, output_root=tmp_path / "out")["dataset"]
    schema = pq.read_schema(dataset)

    assert schema.field("request_id").type == pa.string()
    assert schema.field("baseline_mean").type == pa.float64()
    assert schema.field("repeat_count").type == pa.int64()
    assert schema.field("event_start_date").type == pa.string()
    assert schema.field("suppression_flag").type == pa.bool_()


@pytest.mark.parametrize(
    "field",
    ["repeat_count", "repeat_stability_status", "maximum_repeat_phase_lift_stddev"],
)
def test_analysis_requires_phase_repeat_stability_fields(tmp_path: Path, field: str) -> None:
    inputs = _analysis_inputs(tmp_path)
    table = pq.read_table(inputs["phase_metrics_path"]).drop([field])
    pq.write_table(table, inputs["phase_metrics_path"])

    with pytest.raises(AnalysisError, match=rf"analysis_input_missing_fields.*{field}"):
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")


def test_analysis_translates_output_directory_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = _analysis_inputs(tmp_path)
    output = tmp_path / "blocked-output"
    original_mkdir = Path.mkdir

    def fail_output_mkdir(path: Path, *args: object, **kwargs: object) -> None:
        if path == output:
            raise PermissionError("forced permission failure")
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_output_mkdir)
    with pytest.raises(AnalysisError, match="analysis_output_write_error"):
        build_analysis_dataset(**inputs, output_root=output)


def test_analysis_translates_parquet_output_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = _analysis_inputs(tmp_path)

    def fail_write(*args: object, **kwargs: object) -> None:
        raise pa.ArrowInvalid("forced parquet failure")

    monkeypatch.setattr(analysis_module.pq, "write_table", fail_write)
    with pytest.raises(AnalysisError, match="analysis_output_write_error") as raised:
        build_analysis_dataset(**inputs, output_root=tmp_path / "out")
    assert type(raised.value) is AnalysisError


def test_request_plan_csv_rejects_blank_header(tmp_path: Path) -> None:
    plan = tmp_path / "plan.csv"
    plan.write_text("request_id,   \nr1,value\n", encoding="utf-8")

    with pytest.raises(TrendsError, match="request_plan_blank_header"):
        read_request_plan_rows(plan, required_fields={"request_id"})


def test_request_plan_csv_rejects_duplicate_blank_headers(tmp_path: Path) -> None:
    plan = tmp_path / "plan.csv"
    plan.write_text("request_id, ,   \nr1,a,b\n", encoding="utf-8")

    with pytest.raises(TrendsError, match="request_plan_duplicate_headers"):
        read_request_plan_rows(plan, required_fields={"request_id"})


def test_request_plan_csv_rejects_extra_cells(tmp_path: Path) -> None:
    plan = tmp_path / "plan.csv"
    plan.write_text("request_id,concept_ids\nr1,flood,extra\n", encoding="utf-8")

    with pytest.raises(
        TrendsError,
        match=r"request_plan_row_width_error: row=2 expected=2 actual=3",
    ):
        read_request_plan_rows(plan, required_fields={"request_id"})


def test_unmatched_csv_order_is_deterministic(tmp_path: Path) -> None:
    inputs = _analysis_inputs(tmp_path)
    plans = _rows(inputs["request_plan_path"])
    for plan in plans:
        plan["concept_ids"] = "flood;zeta;alpha"
    _write_parquet(inputs["request_plan_path"], list(reversed(plans)))

    result = build_analysis_dataset(**inputs, output_root=tmp_path / "out")
    rows = _csv_rows(result["unmatched_keys"])

    assert list(rows[0]) == list(analysis_module.UNMATCHED_FIELDS)
    identities = [(row["request_id"], row["repeat_id"], row["concept_id"]) for row in rows]
    assert identities == sorted(identities)


def test_analysis_cli_scope_excludes_milestone_1_0b() -> None:
    result = CliRunner().invoke(app, ["analysis", "--help"])

    assert result.exit_code == 0
    assert "build-dataset" in result.output
    assert "validate-signal" not in result.output


_LONG_REQUEST_ID = "d7825d9b97515a3207c965a659da8883e2f3663ea0dbd5402565a7b9febf5351"


def _analysis_inputs(tmp_path: Path, plan_format: str = "parquet") -> dict[str, Path]:
    plan_name = "request_plan.csv" if plan_format.startswith("csv") else "request_plan.parquet"
    plan = tmp_path / plan_name
    episode = tmp_path / "episode_metrics.parquet"
    repeat = tmp_path / "repeat_metrics.parquet"
    phase = tmp_path / "phase_metrics.parquet"
    if plan_format.startswith("csv"):
        encoding = "utf-8-sig" if plan_format == "csv-bom" else "utf-8"
        _write_plan_csv(plan, _plan_rows(), encoding=encoding)
    else:
        _write_parquet(plan, _plan_rows())
    _write_parquet(episode, _episode_metric_rows())
    _write_parquet(repeat, _repeat_metric_rows())
    _write_parquet(phase, _phase_metric_rows())
    return {
        "request_plan_path": plan,
        "episode_metrics_path": episode,
        "repeat_metrics_path": repeat,
        "phase_metrics_path": phase,
    }


def _plan_rows() -> list[dict[str, object]]:
    return [
        {
            "request_id": _LONG_REQUEST_ID,
            "planned_repeat_id": repeat_id,
            "episode_id": "episode-1",
            "geography": "US-FL",
            "geography_level": "state",
            "batch_id": "batch_1_flood_awareness",
            "comparison_batch_id": "batch_1_flood_awareness",
            "request_role": "treated_state",
            "terminology_version": "0.8A-v3",
            "concept_ids": "flood",
            "expected_output_filename": f"{_LONG_REQUEST_ID}-{repeat_id}.csv",
            "event_start_date": "2024-06-10",
            "event_end_date": "2024-06-12",
            "baseline_start_date": "2024-05-06",
            "baseline_end_date": "2024-06-09",
            "post_start_date": "2024-06-13",
            "post_end_date": "2024-07-10",
        }
        for repeat_id in ("repeat_1", "repeat_2")
    ]


def _episode_metric_rows() -> list[dict[str, object]]:
    return [
        {
            **_lineage_fields(),
            **_scientific_fields(),
            "request_id": _LONG_REQUEST_ID,
            "repeat_id": "aggregated_after_variability",
            "repeat_ids": "repeat_1;repeat_2",
            "repeat_count": 2,
            "episode_id": "episode-1",
            "provisional_episode_id": "episode-1",
            "concept_id": "flood",
            "geography": "US-FL",
            "terminology_version": "0.8A-v3",
            "baseline_mean": 10.0,
            "event_maximum": 80.0,
            "absolute_peak_lift": 70.0,
            "repeat_stability_flag": True,
            "metric_quality_status": "usable",
            "zero_fraction": 0.0,
        }
    ]


def _repeat_metric_rows() -> list[dict[str, object]]:
    return [
        {
            **_lineage_fields(),
            **_scientific_fields(),
            "request_id": _LONG_REQUEST_ID,
            "repeat_id": repeat_id,
            "export_attempt_id": f"attempt-{repeat_id}",
            "episode_id": "episode-1",
            "provisional_episode_id": "episode-1",
            "concept_id": "flood",
            "geography": "US-FL",
            "terminology_version": "0.8A-v3",
            "baseline_mean": 10.0 + index,
            "event_maximum": 80.0 + index,
            "absolute_peak_lift": 70.0,
            "repeat_stability_flag": True,
            "metric_quality_status": "usable",
            "zero_fraction": 0.0,
        }
        for index, repeat_id in enumerate(("repeat_1", "repeat_2"))
    ]


def _phase_metric_rows() -> list[dict[str, object]]:
    return [
        {
            **_lineage_fields(),
            **_phase_required_fields(),
            "request_id": _LONG_REQUEST_ID,
            "repeat_id": repeat_id,
            "export_attempt_id": f"attempt-{repeat_id}",
            "episode_id": "episode-1",
            "provisional_episode_id": "episode-1",
            "request_role": "treated_state",
            "concept_id": "flood",
            "geography": "US-FL",
            "geography_level": "state",
            "batch_id": "batch_1_flood_awareness",
            "comparison_batch_id": "batch_1_flood_awareness",
            "terminology_version": "0.8A-v3",
            "baseline_mean": 10.0 + index,
            "baseline_valid_day_count": 28,
            "metric_quality_status": "usable",
            "repeat_count": 2,
            "repeat_stability_status": "stable",
            "maximum_repeat_phase_lift_stddev": 0.1,
            "peak_phase": "immediate",
            "dominant_event_response_phase": "immediate",
            "dominant_standardized_phase_lift": 3.2,
            "immediate_standardized_lift": 3.2,
            "immediate_robust_lift": 2.7,
        }
        for index, repeat_id in enumerate(("repeat_1", "repeat_2"))
    ]


def _lineage_fields() -> dict[str, object]:
    return {
        "episode_id": "episode-1",
        "geography": "US-FL",
        "geography_level": "state",
        "batch_id": "batch_1_flood_awareness",
        "comparison_batch_id": "batch_1_flood_awareness",
        "request_role": "treated_state",
        "terminology_version": "0.8A-v3",
        "event_start_date": "2024-06-10",
        "event_end_date": "2024-06-12",
        "baseline_start_date": "2024-05-06",
        "baseline_end_date": "2024-06-09",
        "post_start_date": "2024-06-13",
        "post_end_date": "2024-07-10",
    }


def _scientific_fields() -> dict[str, object]:
    return {
        "baseline_valid_day_count": 28,
        "event_valid_day_count": 3,
        "post_valid_day_count": 28,
        "baseline_mean": 10.0,
        "baseline_median": 10.0,
        "baseline_standard_deviation": 2.0,
        "baseline_mad": 1.0,
        "event_maximum": 80.0,
        "post_maximum": 40.0,
        "absolute_peak_lift": 70.0,
        "ratio_peak_lift": 8.0,
        "z_score_peak_lift": 35.0,
        "robust_peak_lift": 70.0,
        "zero_fraction": 0.0,
        "suppression_flag": False,
        "metric_quality_status": "usable",
        "metric_quality_reasons": "",
        "repeat_stability_flag": True,
        "peak_date": "2024-06-11",
    }


def _phase_required_fields() -> dict[str, object]:
    fields: dict[str, object] = {
        "treated_geography": "US-FL",
        "semantic_family": "flood_awareness",
        "proxy_type": "context",
        "standardized_peak_lift": 3.2,
        "peak_lead_lag_days": 1,
        "dominant_event_response_phase": "immediate",
        "dominant_standardized_phase_lift": 3.2,
        "dominant_robust_phase_lift": 2.7,
        "peak_phase": "immediate",
        "global_peak_date": "2024-06-11",
        "global_peak_phase": "immediate",
        "global_peak_value": 80.0,
        "baseline_valid_day_count": 28,
        "baseline_mean": 10.0,
        "baseline_median": 10.0,
        "baseline_standard_deviation": 2.0,
        "baseline_mad": 1.0,
        "metric_quality_status": "usable",
    }
    for phase in ("anticipatory", "immediate", "early_recovery", "extended_recovery"):
        fields[f"{phase}_valid_day_count"] = 7
        for suffix in ("mean", "maximum", "peak_lift", "standardized_lift", "robust_lift"):
            fields[f"{phase}_{suffix}"] = 1.0
    return fields


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _write_plan_csv(path: Path, rows: list[dict[str, object]], encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
