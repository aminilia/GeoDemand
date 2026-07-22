from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from geodemand.acquisition_planning import (
    ACQUISITION_EPISODE_SELECTION_SCHEMA,
    ACQUISITION_EXECUTION_PLAN_SCHEMA,
    build_execution_plan,
    select_execution_episodes,
)
from geodemand.cli import app
from geodemand.trends import TrendsError, plan_requests
from geodemand.trends_browser import export_status

ROOT = Path(__file__).parents[1]
TERMS = ROOT / "config" / "trends_terms.yaml"
RULES = ROOT / "config" / "trends_rules.yaml"


def test_nested_selection_is_deterministic_and_order_independent(tmp_path: Path) -> None:
    rows = _episodes()
    first, first_paths = select_execution_episodes(rows, tmp_path / "first", 10, "v1")
    repeated, second_paths = select_execution_episodes(
        list(reversed(rows)), tmp_path / "second", 10, "v1"
    )
    second, _ = select_execution_episodes(list(reversed(rows)), tmp_path / "twenty", 20, "v1")
    first_ids = {str(row["provisional_episode_id"]) for row in first}
    repeated_ids = {str(row["provisional_episode_id"]) for row in repeated}
    second_ids = {str(row["provisional_episode_id"]) for row in second}

    assert len(first_ids) == 10
    assert first_ids == repeated_ids
    assert len(second_ids) == 20
    assert first_ids < second_ids
    assert _sha(first_paths["selection_csv"]) == _sha(second_paths["selection_csv"])
    assert _sha(first_paths["selection_parquet"]) == _sha(second_paths["selection_parquet"])
    assert all("execution_selection_rank" in row for row in first)
    assert sum(row["primary_state"] == "FL" for row in first) >= 1
    assert sum(row["primary_state"] == "FL" for row in second) >= 2
    twenty_audit = pq.read_table(
        tmp_path / "twenty" / "planning" / "acquisition_episode_selection.parquet"
    ).to_pylist()
    assert any(
        row["treated_state"] == "FL" and row["selection_reason"] == "required_state_continuity"
        for row in twenty_audit
    )

    audit = pq.read_table(first_paths["selection_parquet"])
    selected_audit = [row for row in audit.to_pylist() if row["included_in_10_episode_set"]]
    assert sorted(int(row["selection_step"]) for row in selected_audit) == list(range(1, 11))
    assert {str(row["selection_reason"]) for row in selected_audit} <= {
        "legacy_mini_pilot",
        "required_state_continuity",
        "underrepresented_year",
        "underrepresented_region",
        "underrepresented_state",
        "stable_source_rank_tiebreak",
    }


def test_execution_plan_counts_and_identity_contract(tmp_path: Path) -> None:
    selected, _ = select_execution_episodes(_episodes(), tmp_path / "selection", 10, "v1")
    geography = [_geography(row) for row in selected]
    controls = tmp_path / "controls.parquet"
    _write_controls(controls, selected)
    paths = build_execution_plan(
        selected,
        geography,
        controls,
        TERMS,
        RULES,
        tmp_path / "plan",
        "manual_csv",
        [
            "batch_1_flood_awareness",
            "batch_5_specific_recovery",
            "batch_6_specific_products",
        ],
        2,
        "1.0C-pilot-v1",
        1,
    )
    rows = pq.read_table(paths["parquet"]).to_pylist()
    summary = json.loads(paths["summary"].read_text(encoding="utf-8"))

    assert len(rows) == 120
    assert summary["logical_request_count"] == 60
    assert summary["matched_pair_count"] == 10
    assert summary["planned_repeat_count"] == 2
    assert paths["parquet"].name == "acquisition_execution_plan.parquet"
    assert paths["csv"].name == "acquisition_execution_plan.csv"
    assert paths["summary"].name == "acquisition_execution_summary.json"
    assert {str(row["planned_repeat_id"]) for row in rows} == {"repeat_1", "repeat_2"}
    by_request: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_request.setdefault(str(row["request_id"]), []).append(row)
    assert all(len(group) == 2 for group in by_request.values())
    assert all(
        len({str(row["matched_pair_id"]) for row in group}) == 1 for group in by_request.values()
    )
    by_episode = {str(row["episode_id"]): str(row["matched_pair_id"]) for row in rows}
    assert len(by_episode) == 10
    assert len(set(by_episode.values())) == 10
    sidecar = json.loads(next((paths["parquet"].parent / "sidecars").glob("*.json")).read_text())
    assert sidecar["execution_batch_id"] == "1.0C-pilot-v1"
    assert sidecar["matched_pair_id"].startswith("pair_")


def test_execution_batch_does_not_change_request_hash(tmp_path: Path) -> None:
    selected, _ = select_execution_episodes(_episodes(), tmp_path / "selection", 10, "v1")
    controls = tmp_path / "controls.parquet"
    _write_controls(controls, selected)
    args = (selected, [_geography(row) for row in selected], controls, TERMS, RULES)
    first = build_execution_plan(
        *args, tmp_path / "a", "manual_csv", ["batch_1_flood_awareness"], 2, "a", 1
    )
    second = build_execution_plan(
        *args, tmp_path / "b", "manual_csv", ["batch_1_flood_awareness"], 2, "b", 1
    )
    first_ids = {str(row["request_id"]) for row in pq.read_table(first["parquet"]).to_pylist()}
    second_ids = {str(row["request_id"]) for row in pq.read_table(second["parquet"]).to_pylist()}
    assert first_ids == second_ids


def test_missing_or_duplicate_rank_one_control_fails(tmp_path: Path) -> None:
    selected, _ = select_execution_episodes(_episodes(), tmp_path / "selection", 10, "v1")
    controls = tmp_path / "controls.parquet"
    _write_controls(controls, selected[:-1])
    with pytest.raises(TrendsError, match="rank_one_control_unavailable"):
        build_execution_plan(
            selected,
            [_geography(row) for row in selected],
            controls,
            TERMS,
            RULES,
            tmp_path / "plan",
            "manual_csv",
            ["batch_1_flood_awareness"],
            2,
            "v1",
            1,
        )


def test_invalid_and_malformed_selection_inputs_are_domain_errors(tmp_path: Path) -> None:
    with pytest.raises(TrendsError, match="invalid_episode_limit"):
        select_execution_episodes(_episodes(), tmp_path / "invalid", 11, "v1")
    malformed = _episodes()
    del malformed[0]["primary_state"]
    with pytest.raises(TrendsError, match="selection_output_build_error"):
        select_execution_episodes(malformed, tmp_path / "malformed", 10, "v1")
    insufficient = _episodes()
    florida = [row for row in insufficient if row["primary_state"] == "FL"]
    for row in florida[1:]:
        row["primary_state"] = "TX"
    with pytest.raises(TrendsError, match="selection_balance_unavailable"):
        select_execution_episodes(insufficient, tmp_path / "insufficient", 20, "v1")


def test_explicit_selection_and_execution_schemas(tmp_path: Path) -> None:
    selected, paths = select_execution_episodes(_episodes(), tmp_path / "selection", 10, "v1")
    assert pq.read_schema(paths["selection_parquet"]) == ACQUISITION_EPISODE_SELECTION_SCHEMA
    controls = tmp_path / "controls.parquet"
    _write_controls(controls, selected)
    plan = build_execution_plan(
        selected,
        [_geography(row) for row in selected],
        controls,
        TERMS,
        RULES,
        tmp_path / "plan",
        "manual_csv",
        ["batch_1_flood_awareness"],
        2,
        "v1",
        1,
    )
    assert pq.read_schema(plan["parquet"]) == ACQUISITION_EXECUTION_PLAN_SCHEMA


def test_legacy_master_execution_is_structured_error_for_api_and_cli(tmp_path: Path) -> None:
    rows = _episodes()
    pilot = tmp_path / "pilot.parquet"
    geography = tmp_path / "geography.parquet"
    controls = tmp_path / "controls.parquet"
    pq.write_table(pa.Table.from_pylist(rows), pilot)
    pq.write_table(pa.Table.from_pylist([_geography(row) for row in rows]), geography)
    _write_controls(controls, rows)
    with pytest.raises(TrendsError, match="execution_plan_conflicting_lineage"):
        build_execution_plan(
            rows,
            [_geography(row) for row in rows],
            controls,
            TERMS,
            RULES,
            tmp_path / "api",
            "manual_csv",
            ["batch_1_flood_awareness"],
            2,
            "v1",
            1,
        )
    result = CliRunner().invoke(
        app,
        [
            "trends",
            "plan",
            "--pilot",
            str(pilot),
            "--geography",
            str(geography),
            "--controls",
            str(controls),
            "--terms",
            str(TERMS),
            "--rules",
            str(RULES),
            "--output-root",
            str(tmp_path / "cli"),
            "--batch-id",
            "batch_1_flood_awareness",
            "--planned-repeats",
            "2",
            "--execution-batch-id",
            "v1",
            "--execution-only",
        ],
    )
    assert result.exit_code != 0
    assert "execution_plan_conflicting_lineage" in result.output


def test_execution_options_require_execution_only(tmp_path: Path) -> None:
    pilot = tmp_path / "pilot.parquet"
    geography = tmp_path / "geography.parquet"
    rows = _episodes()[:10]
    pq.write_table(pa.Table.from_pylist(rows), pilot)
    pq.write_table(pa.Table.from_pylist([_geography(row) for row in rows]), geography)
    with pytest.raises(TrendsError, match="execution_options_require_execution_only"):
        plan_requests(
            pilot,
            geography,
            TERMS,
            RULES,
            tmp_path / "plan",
            batch_ids=("batch_1_flood_awareness",),
        )


def test_acquisition_document_matches_implemented_contract() -> None:
    text = (ROOT / "docs" / "acquisition_design_1_0C.md").read_text(encoding="utf-8")
    assert "10 episodes × 2 roles × 3 panels × 2 repeats = 120 rows" in text
    assert "20 episodes × 2 roles × 2 panels × 2 repeats = 160 rows" in text
    assert "[future]" not in text
    assert "--episode-limit 10 --batch-id" not in text
    assert "40 unique execution rows" not in text
    assert "80 execution rows" not in text


def test_export_status_is_scoped_by_request_and_repeat(tmp_path: Path) -> None:
    plan = tmp_path / "plan.parquet"
    pq.write_table(pa.Table.from_pylist([_status_plan("request-a", "repeat_1")]), plan)
    manifest = tmp_path / "root" / "manifests" / "browser_export_manifest.parquet"
    manifest.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                _attempt("request-a", "repeat_1", "validated", "a"),
                _attempt("request-b", "repeat_1", "validation_failure", "b"),
            ]
        ),
        manifest,
    )
    status = export_status(plan, tmp_path / "root")
    assert status["planned"] == 1
    assert status["completed"] == 1
    assert status["failed"] == 0


def _episodes() -> list[dict[str, Any]]:
    states = ("FL", "TX", "CA", "NY", "WA", "GA", "IL", "CO", "PA", "NC")
    rows = []
    rank = 0
    for year in range(2022, 2026):
        for index in range(10):
            rank += 1
            start = date(year, 1, 10) + timedelta(days=index * 20)
            state = states[index]
            rows.append(
                {
                    "provisional_episode_id": f"e-{year}-{index:02d}",
                    "start_date": start.isoformat(),
                    "end_date": (start + timedelta(days=index % 3)).isoformat(),
                    "primary_state": state,
                    "selection_rank": rank,
                    "member_count": 1 if index % 2 == 0 else 4,
                }
            )
    return rows


def _geography(row: dict[str, Any]) -> dict[str, Any]:
    state = str(row["primary_state"])
    return {
        "provisional_episode_id": row["provisional_episode_id"],
        "trends_geography_identifier": f"US-{state}",
        "geography_level": "state",
    }


def _write_controls(path: Path, rows: list[dict[str, Any]]) -> None:
    controls = [
        {
            "treated_episode": row["provisional_episode_id"],
            "control_state": "AL" if row["primary_state"] != "AL" else "AK",
            "control_rank": 1,
        }
        for row in rows
    ]
    pq.write_table(pa.Table.from_pylist(controls), path)


def _status_plan(request_id: str, repeat_id: str) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "episode_id": "episode",
        "geography": "US-FL",
        "timeframe": "2024-01-01 2024-03-01",
        "target_concepts": "flood",
        "expected_output_filename": f"{request_id}.csv",
        "sidecar_metadata_filename": f"{request_id}.json",
        "explore_url": "https://trends.google.com/trends/explore",
        "terminology_version": "v1",
        "request_status": "planned",
        "planned_repeat_id": repeat_id,
        "batch_id": "batch",
    }


def _attempt(request_id: str, repeat_id: str, status: str, attempt_id: str) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "repeat_id": repeat_id,
        "status": status,
        "started_at": "2026-01-01T00:00:00+00:00",
        "export_attempt_id": attempt_id,
        "byte_count": 1,
    }


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
