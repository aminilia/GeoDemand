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

from geodemand.catalog import (
    CatalogError,
    apply_catalog_decisions,
    build_provisional_catalog,
    discover_catalog_inputs,
    stable_provisional_id,
    validate_catalog,
)
from geodemand.cli import app

RULES = Path(__file__).parents[1] / "config" / "catalog_rules.yaml"


def test_catalog_build_is_deterministic_and_preserves_accounting(
    catalog_inputs: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    cohort, episodes, comparison = catalog_inputs
    inputs = discover_catalog_inputs(cohort, episodes, comparison)
    first = build_provisional_catalog(inputs, tmp_path / "first", RULES)
    second = build_provisional_catalog(inputs, tmp_path / "second", RULES)

    membership = _rows(first["episode_membership"])
    catalog = _rows(first["episodes"])
    evidence = {
        row["source_conservative_episode_id"]: row for row in _rows(first["episode_evidence"])
    }
    validation = validate_catalog(tmp_path / "first")

    assert len(membership) == len({row["event_record_id"] for row in membership}) == 5
    assert validation["valid"] is True
    assert all(row["source_conservative_episode_id"] for row in membership)
    assert evidence["c1"]["balanced_merges_multiple_conservative_episodes"] is True
    assert {
        row["source_conservative_episode_id"]: row["provisional_catalog_status"] for row in catalog
    } == {
        "c1": "merge_candidate",
        "c2": "merge_candidate",
        "c3": "manual_review_required",
        "c4": "retained_provisionally",
    }
    assert all(
        row["membership_action"] == "pending_review"
        for row in membership
        if row["source_conservative_episode_id"] in {"c1", "c2", "c3"}
    )
    for name, path in first.items():
        if name != "manifest":
            assert _sha256(path) == _sha256(second[name])
    assert _sha256(first["manifest"]) == _sha256(second["manifest"])
    manifest = json.loads(first["manifest"].read_text(encoding="utf-8"))
    assert "usgs_episode_summary" not in manifest["input_hashes"]
    assert "mrms_episode_metrics" not in manifest["input_hashes"]
    metadata = pq.read_metadata(first["episodes"]).metadata
    assert metadata is not None and b"geo" in metadata


def test_catalog_builds_without_physical_evidence_inputs(
    catalog_inputs: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    cohort, episodes, comparison = catalog_inputs
    inputs = discover_catalog_inputs(cohort, episodes, comparison)
    paths = build_provisional_catalog(inputs, tmp_path / "catalog", RULES)
    evidence = _rows(paths["episode_evidence"])
    statuses = {
        row["source_conservative_episode_id"]: row["provisional_catalog_status"]
        for row in _rows(paths["episodes"])
    }

    assert all("mrms_status" not in row for row in evidence)
    assert all("usgs_status" not in row for row in evidence)
    assert statuses["c1"] == statuses["c2"] == "merge_candidate"
    assert statuses["c3"] == "manual_review_required"
    assert statuses["c4"] == "retained_provisionally"
    assert all(
        row["membership_action"] != "provisional_merge"
        for row in _rows(paths["episode_membership"])
    )


def test_stable_ids_and_split_boundary_protection(
    catalog_inputs: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    assert stable_provisional_id("v1", ["b", "a"]) == stable_provisional_id("v1", ["a", "b"])
    cohort, episodes, comparison = catalog_inputs
    balanced = episodes / "balanced" / "episode_membership" / "part.parquet"
    rows = _rows(balanced)
    for row in rows:
        if row["event_record_id"] == "e5":
            row["episode_id"] = "b1"
    _write(balanced, rows)
    paths = build_provisional_catalog(
        discover_catalog_inputs(cohort, episodes, comparison),
        tmp_path / "catalog",
        RULES,
    )
    c4 = next(
        row for row in _rows(paths["episodes"]) if row["source_conservative_episode_id"] == "c4"
    )
    assert c4["split_boundary_flag"] is True
    assert c4["provisional_catalog_status"] == "split_candidate"
    assert "split_boundary" in c4["manual_review_reasons"]


def test_manual_decisions_reject_unknown_duplicates_and_incomplete_splits(
    catalog_inputs: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    cohort, episodes, comparison = catalog_inputs
    catalog_dir = tmp_path / "catalog"
    build_provisional_catalog(
        discover_catalog_inputs(cohort, episodes, comparison), catalog_dir, RULES
    )
    episode_id = _rows(catalog_dir / "episodes.parquet")[0]["provisional_episode_id"]

    unknown = tmp_path / "unknown.csv"
    _write_csv(
        unknown,
        [
            {
                "provisional_episode_id": "unknown",
                "reviewer_action": "retain",
                "reviewer_reason": "checked",
                "reviewer_name": "reviewer",
            }
        ],
    )
    with pytest.raises(CatalogError, match="Unknown"):
        apply_catalog_decisions(catalog_dir, unknown, "0.7C-v1", tmp_path / "unknown-out")

    duplicate = tmp_path / "duplicate.csv"
    decision = {
        "provisional_episode_id": episode_id,
        "reviewer_action": "retain",
        "reviewer_reason": "checked",
        "reviewer_name": "reviewer",
    }
    _write_csv(duplicate, [decision, decision])
    with pytest.raises(CatalogError, match="Duplicate"):
        apply_catalog_decisions(catalog_dir, duplicate, "0.7C-v1", tmp_path / "duplicate-out")

    incomplete = tmp_path / "incomplete.csv"
    _write_csv(
        incomplete,
        [{**decision, "reviewer_action": "split", "resulting_member_groups": "e1|missing"}],
    )
    with pytest.raises(CatalogError, match="omits or duplicates"):
        apply_catalog_decisions(catalog_dir, incomplete, "0.7C-v1", tmp_path / "split-out")

    by_source = {
        row["source_conservative_episode_id"]: row["provisional_episode_id"]
        for row in _rows(catalog_dir / "episodes.parquet")
    }
    cross_split = tmp_path / "cross-split.csv"
    _write_csv(
        cross_split,
        [
            {
                "provisional_episode_id": by_source["c1"],
                "merge_episode_ids": f"{by_source['c1']};{by_source['c4']}",
                "reviewer_action": "merge",
                "reviewer_reason": "clustering review",
                "reviewer_name": "reviewer",
            }
        ],
    )
    with pytest.raises(CatalogError, match="crosses splits"):
        apply_catalog_decisions(catalog_dir, cross_split, "0.7C-v1", tmp_path / "cross-split-out")


def test_catalog_cli_inspect_and_missing_required_inputs(
    catalog_inputs: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    cohort, episodes, comparison = catalog_inputs
    result = CliRunner().invoke(
        app,
        [
            "catalog",
            "inspect",
            "--cohort-root",
            str(cohort),
            "--episode-root",
            str(episodes),
            "--comparison-root",
            str(comparison),
        ],
    )
    assert result.exit_code == 0
    assert json.loads(result.stdout)["eligible_event_count"] == 5
    with pytest.raises(CatalogError, match="Required catalog inputs"):
        discover_catalog_inputs(cohort, tmp_path / "missing", comparison)


@pytest.fixture()
def catalog_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    cohort = tmp_path / "cohort"
    episode_root = tmp_path / "policies"
    comparison = tmp_path / "comparison"
    events = [
        _event("e1", "2023-06-01", "CA", 0.0),
        _event("e2", "2023-06-01", "CA", 0.1),
        _event("e3", "2023-06-02", "CA", 0.2),
        _event("e4", "2024-06-01", "TX", 1.0),
        _event("e5", "2025-06-01", "WA", 2.0),
    ]
    _write(cohort / "eligible_event_records" / "part.parquet", events, geo=True)
    _write(
        cohort / "event_state_records" / "part.parquet",
        [
            {"event_record_id": row["event_record_id"], "state_code": row["primary_state_code"]}
            for row in events
        ],
    )
    _json(cohort / "cohort_summary.json", {"eligible_event_records": 5})

    conservative_members = {"c1": ["e1", "e2"], "c2": ["e3"], "c3": ["e4"], "c4": ["e5"]}
    balanced_members = {"b1": ["e1", "e2", "e3"], "b2": ["e4"], "b3": ["e5"]}
    _policy(
        episode_root / "conservative",
        "conservative",
        conservative_members,
        events,
        review_episode="c3",
    )
    _policy(episode_root / "balanced", "balanced", balanced_members, events)
    comparison.mkdir()
    (comparison / "clustering_sensitivity.csv").write_text(
        "policy,episode_count\nconservative,4\nbalanced,3\n", encoding="utf-8"
    )
    (comparison / "clustering_agreement.csv").write_text(
        "comparison,adjusted_rand_index\nconservative_vs_balanced,0.9\n", encoding="utf-8"
    )

    return cohort, episode_root, comparison


def _policy(
    root: Path,
    policy: str,
    groups: dict[str, list[str]],
    events: list[dict[str, object]],
    review_episode: str | None = None,
) -> None:
    by_id = {str(row["event_record_id"]): row for row in events}
    episode_rows = []
    memberships = []
    for episode_id, member_ids in groups.items():
        member_rows = [by_id[item] for item in member_ids]
        start = min(row["start_date"] for row in member_rows)
        geometry = Polygon(
            [
                (float(member_rows[0]["representative_longitude"]) - 0.05, 0.0),
                (float(member_rows[-1]["representative_longitude"]) + 0.05, 0.0),
                (float(member_rows[-1]["representative_longitude"]) + 0.05, 0.2),
                (float(member_rows[0]["representative_longitude"]) - 0.05, 0.2),
            ]
        )
        review = episode_id == review_episode
        episode_rows.append(
            {
                "episode_id": episode_id,
                "policy": policy,
                "episode_start_date": start,
                "episode_end_date": start,
                "episode_duration_days": 8 if review else 0,
                "member_count": len(member_ids),
                "state_codes": ";".join(
                    sorted({str(row["primary_state_code"]) for row in member_rows})
                ),
                "state_count": 1,
                "primary_state_code": member_rows[0]["primary_state_code"],
                "union_area_km2": 1.0,
                "representative_longitude": geometry.representative_point().x,
                "representative_latitude": geometry.representative_point().y,
                "maximum_member_distance_km": 300.0 if review else 10.0,
                "candidate_split": _split(start),
                "crosses_split_boundary": False,
                "spatial_review_required": review,
                "clustering_review_reason": "long_duration" if review else "",
                "geometry": geometry.wkb,
                "bridge_edge_count": 1 if review else 0,
            }
        )
        memberships.extend(
            {"episode_id": episode_id, "event_record_id": event_id, "policy": policy}
            for event_id in member_ids
        )
    _write(root / "episodes" / "part.parquet", episode_rows, geo=True)
    _write(root / "episode_membership" / "part.parquet", memberships)
    _write(root / "episode_edges" / "part.parquet", [])
    _json(root / "episode_summary.json", {"episode_count": len(groups)})
    _json(root / "manifest.json", {"policy": policy})


def _event(event_id: str, start: str, state: str, x: float) -> dict[str, object]:
    geometry = Polygon([(x, 0), (x + 0.1, 0), (x + 0.1, 0.1), (x, 0.1)])
    return {
        "event_record_id": event_id,
        "start_date": date.fromisoformat(start),
        "end_date": date.fromisoformat(start),
        "primary_state_code": state,
        "intersecting_state_codes": state,
        "representative_longitude": geometry.representative_point().x,
        "representative_latitude": geometry.representative_point().y,
        "geometry": geometry.wkb,
    }


def _split(value: object) -> str:
    text = str(value)
    return "development" if text < "2024" else "validation" if text < "2025" else "test"


def _write(path: Path, rows: list[dict[str, object]], geo: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = (
        pa.Table.from_pylist(rows)
        if rows
        else pa.table(
            {
                "event_record_id_a": pa.array([], type=pa.string()),
                "event_record_id_b": pa.array([], type=pa.string()),
            }
        )
    )
    if geo:
        metadata = dict(table.schema.metadata or {})
        metadata[b"geo"] = json.dumps(
            {
                "version": "1.0.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Polygon"],
                        "crs": "EPSG:4326",
                    }
                },
            },
            sort_keys=True,
        ).encode()
        table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path)


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def _json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
