from __future__ import annotations

import csv
import hashlib
import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import Polygon
from typer.testing import CliRunner

from geodemand.cli import app
from geodemand.episodes import build_episodes, compare_episode_policies


def test_episode_policies_and_outputs(episode_inputs: tuple[Path, Path], tmp_path: Path) -> None:
    events, states = episode_inputs

    conservative = build_episodes(events, states, tmp_path / "conservative", "conservative")
    balanced = build_episodes(events, states, tmp_path / "balanced", "balanced")
    broad = build_episodes(events, states, tmp_path / "broad", "broad")

    conservative_summary = _json(conservative["episode_summary"])
    balanced_summary = _json(balanced["episode_summary"])
    broad_summary = _json(broad["episode_summary"])

    assert conservative_summary["total_accepted_edges"] < balanced_summary["total_accepted_edges"]
    assert balanced_summary["total_accepted_edges"] <= broad_summary["total_accepted_edges"]
    assert balanced_summary["row_accounting"] == {
        "balanced": True,
        "eligible_candidate_records": 8,
        "episode_membership_rows": 8,
        "sum_episode_member_counts": 8,
    }

    edges = _rows(balanced["episode_edges"] / "part-00000.parquet")
    assert any(edge["shared_state_codes"] == "CA;NV" for edge in edges)
    unique_edge_pairs = {(edge["event_record_id_a"], edge["event_record_id_b"]) for edge in edges}
    assert len(unique_edge_pairs) == len(edges)

    episodes = _rows(balanced["episodes"] / "part-00000.parquet")
    assert any(episode["crosses_split_boundary"] for episode in episodes)
    assert any(episode["member_count"] == 1 for episode in episodes)
    assert all(len(str(episode["episode_id"])) == 64 for episode in episodes)


def test_episode_determinism_and_geoparquet(
    episode_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events, states = episode_inputs
    first = build_episodes(events, states, tmp_path / "first", "balanced")
    second = build_episodes(events, states, tmp_path / "second", "balanced")

    assert _sha256(first["episode_summary"]) == _sha256(second["episode_summary"])
    assert _rows(first["episode_membership"] / "part-00000.parquet") == _rows(
        second["episode_membership"] / "part-00000.parquet"
    )
    metadata = pq.read_metadata(first["episodes"] / "part-00000.parquet").metadata
    assert metadata is not None
    assert json.loads(metadata[b"geo"].decode("utf-8"))["primary_column"] == "geometry"


def test_episode_max_rows_bounds_membership(
    episode_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events, states = episode_inputs
    artifacts = build_episodes(events, states, tmp_path / "limited", "broad", max_rows=3)
    summary = _json(artifacts["episode_summary"])

    assert summary["row_accounting"]["eligible_candidate_records"] == 3
    assert summary["row_accounting"]["balanced"] is True


def test_episode_compare_outputs_sensitivity(
    episode_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events, states = episode_inputs
    artifacts = compare_episode_policies(events, states, tmp_path / "compare")
    sensitivity_rows = _csv_rows(artifacts["clustering_sensitivity"])
    agreement_rows = _csv_rows(artifacts["clustering_agreement"])

    assert {row["policy"] for row in sensitivity_rows} == {
        "conservative",
        "balanced",
        "broad",
    }
    assert all("pairwise_membership_agreement" not in row for row in sensitivity_rows)
    comparison = {row["comparison"]: row for row in agreement_rows}
    conservative_vs_balanced = comparison["conservative_vs_balanced"]
    balanced_vs_broad = comparison["balanced_vs_broad"]

    assert int(conservative_vs_balanced["episodes_split_relative_to_broader_policy"]) == 0
    assert int(balanced_vs_broad["episodes_split_relative_to_broader_policy"]) == 0
    assert int(conservative_vs_balanced["episodes_merged_by_broader_policy"]) > 0
    assert int(conservative_vs_balanced["narrower_episodes_preserved_exactly"]) > 0
    assert (
        int(conservative_vs_balanced["records_retaining_identical_complete_episode_membership"]) > 0
    )
    assert 0.0 <= float(conservative_vs_balanced["pairwise_membership_agreement"]) <= 1.0


def test_episode_cli_build(episode_inputs: tuple[Path, Path], tmp_path: Path) -> None:
    events, states = episode_inputs
    output_dir = tmp_path / "cli"

    result = CliRunner().invoke(
        app,
        [
            "episodes",
            "build",
            "--events",
            str(events),
            "--event-states",
            str(states),
            "--output-dir",
            str(output_dir),
            "--policy",
            "balanced",
            "--max-rows",
            "5",
        ],
    )

    assert result.exit_code == 0
    assert (output_dir / "episodes_balanced" / "episode_summary.json").exists()


@pytest.fixture()
def episode_inputs(tmp_path: Path) -> tuple[Path, Path]:
    events_dir = tmp_path / "eligible_event_records"
    states_dir = tmp_path / "event_state_records"
    events_dir.mkdir()
    states_dir.mkdir()
    events = [
        _event("e1", "2023-12-31", "2024-01-01", "CA", _box(0.00, 0.00)),
        _event("e2", "2023-12-31", "2024-01-01", "CA", _box(0.20, 0.00)),
        _event("e3", "2024-01-02", "2024-01-02", "CA", _box(0.42, 0.00)),
        _event("e4", "2024-01-10", "2024-01-10", "CA", _box(10.0, 0.00)),
        _event("e5", "2025-01-01", None, "TX", _box(20.0, 0.00)),
        _event("e6", "2025-01-02", "2025-01-02", "CA", _box(30.0, 0.00), review=True),
        _event("e7", "2025-01-03", "2025-01-03", "CA", _box(30.2, 0.00)),
        _event("e8", "2025-01-04", "2025-01-04", "CA", _box(30.4, 0.00)),
    ]
    state_rows = [
        _state("e1", "CA"),
        _state("e1", "NV"),
        _state("e2", "CA"),
        _state("e2", "NV"),
        _state("e3", "CA"),
        _state("e4", "CA"),
        _state("e5", "TX"),
        _state("e6", "CA"),
        _state("e7", "CA"),
        _state("e8", "CA"),
    ]
    _write_events(events_dir / "part-00000.parquet", events)
    _write_states(states_dir / "part-00000.parquet", state_rows)
    return events_dir, states_dir


def _box(x: float, y: float) -> Polygon:
    return Polygon([(x, y), (x + 0.3, y), (x + 0.3, y + 0.3), (x, y + 0.3)])


def _event(
    event_id: str,
    start: str,
    end: str | None,
    state: str,
    geometry: Polygon,
    review: bool = False,
) -> dict[str, object]:
    end_date = date.fromisoformat(end) if end is not None else None
    duration_days = (end_date - date.fromisoformat(start)).days if end_date is not None else 0
    return {
        "event_record_id": event_id,
        "source_uuid": event_id,
        "start_date": date.fromisoformat(start),
        "end_date": end_date,
        "duration_days": duration_days,
        "primary_state_code": state,
        "primary_state_name": state,
        "state_candidate_count": 1,
        "multi_state_event": False,
        "intersecting_state_codes": state,
        "event_area_km2": 1.0,
        "us_overlap_area_km2": 1.0,
        "representative_longitude": float(geometry.representative_point().x),
        "representative_latitude": float(geometry.representative_point().y),
        "study_domain": "conus",
        "spatial_review_required": review,
        "spatial_review_reason": "synthetic_review" if review else "",
        "candidate_split": _candidate_split(start),
        "geometry": geometry.wkb,
    }


def _candidate_split(start: str) -> str:
    if start < "2024-01-01":
        return "development"
    if start < "2025-01-01":
        return "validation"
    return "test"


def _state(event_id: str, state: str) -> dict[str, object]:
    return {
        "event_record_id": event_id,
        "state_code": state,
        "state_name": state,
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 1),
        "state_overlap_area_km2": 1.0,
        "event_area_fraction": 1.0,
        "us_overlap_fraction": 1.0,
        "is_primary_state": True,
        "study_domain": "conus",
    }


def _write_events(path: Path, rows: list[dict[str, object]]) -> None:
    schema = pa.schema(
        [
            pa.field("event_record_id", pa.string()),
            pa.field("source_uuid", pa.string()),
            pa.field("start_date", pa.date32()),
            pa.field("end_date", pa.date32()),
            pa.field("duration_days", pa.int64()),
            pa.field("primary_state_code", pa.string()),
            pa.field("primary_state_name", pa.string()),
            pa.field("state_candidate_count", pa.int64()),
            pa.field("multi_state_event", pa.bool_()),
            pa.field("intersecting_state_codes", pa.string()),
            pa.field("event_area_km2", pa.float64()),
            pa.field("us_overlap_area_km2", pa.float64()),
            pa.field("representative_longitude", pa.float64()),
            pa.field("representative_latitude", pa.float64()),
            pa.field("study_domain", pa.string()),
            pa.field("spatial_review_required", pa.bool_()),
            pa.field("spatial_review_reason", pa.string()),
            pa.field("candidate_split", pa.string()),
            pa.field("geometry", pa.binary()),
        ],
        metadata={
            b"geo": json.dumps(
                {
                    "version": "0.4.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB", "crs": "EPSG:4326"}},
                },
                sort_keys=True,
            ).encode()
        },
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _write_states(path: Path, rows: list[dict[str, object]]) -> None:
    schema = pa.schema(
        [
            pa.field("event_record_id", pa.string()),
            pa.field("state_code", pa.string()),
            pa.field("state_name", pa.string()),
            pa.field("start_date", pa.date32()),
            pa.field("end_date", pa.date32()),
            pa.field("state_overlap_area_km2", pa.float64()),
            pa.field("event_area_fraction", pa.float64()),
            pa.field("us_overlap_fraction", pa.float64()),
            pa.field("is_primary_state", pa.bool_()),
            pa.field("study_domain", pa.string()),
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def _json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
