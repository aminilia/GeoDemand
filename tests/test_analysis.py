from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from geodemand.analysis import AnalysisError, build_analysis_dataset
from geodemand.cli import app


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
            "request_id": _LONG_REQUEST_ID,
            "repeat_id": repeat_id,
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
