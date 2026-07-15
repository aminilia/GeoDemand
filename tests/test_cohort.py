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

from geodemand.cohort import build_cohort, classify_study_domain


def test_domain_classification_sets() -> None:
    assert classify_study_domain(["CA"]) == "conus"
    assert classify_study_domain(["DC"]) == "conus"
    assert classify_study_domain(["AK"]) == "alaska"
    assert classify_study_domain(["HI"]) == "hawaii"
    assert classify_study_domain(["PR"]) == "puerto_rico"
    assert classify_study_domain(["GU"]) == "other_us_territory"
    assert classify_study_domain(["CA", "AK"]) == "multi_domain"
    assert classify_study_domain([]) == "unclassified"


def test_build_cohort_classifies_outputs_and_row_accounting(
    cohort_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events_path, states_path = cohort_inputs
    artifacts = build_cohort(events_path, states_path, tmp_path / "cohort", batch_size=4)

    eligible = _rows(artifacts["eligible_event_records"] / "part-00000.parquet")
    secondary = _rows(artifacts["secondary_domain_records"] / "part-00000.parquet")
    excluded = _rows(artifacts["excluded_event_records"] / "part-00000.parquet")
    state_records = _rows(artifacts["event_state_records"] / "part-00000.parquet")
    summary = json.loads(artifacts["cohort_summary"].read_text(encoding="utf-8"))

    assert {row["event_record_id"] for row in eligible} >= {
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000008",
        "00000000-0000-0000-0000-000000000009",
    }
    assert any(row["study_domain"] == "alaska" for row in secondary)
    assert any(row["study_domain"] == "hawaii" for row in secondary)
    assert any(row["study_domain"] == "puerto_rico" for row in secondary)
    assert any(row["study_domain"] == "other_us_territory" for row in secondary)
    assert {row["primary_exclusion_reason"] for row in excluded} >= {
        "outside_temporal_window",
        "missing_start_date",
        "invalid_duration",
        "missing_state_assignment",
    }
    assert any(
        row["event_record_id"] == "00000000-0000-0000-0000-000000000002" for row in state_records
    )
    assert summary["row_accounting"] == {
        "balanced": True,
        "eligible_primary_rows": 4,
        "other_excluded_rows": 4,
        "secondary_domain_rows": 4,
        "source_us_intersecting_rows": 12,
    }
    assert summary["candidate_split_counts"] == {
        "development": 2,
        "test": 1,
        "validation": 1,
    }


def test_cohort_overlap_diagnostics_and_temporal_quality(
    cohort_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events_path, states_path = cohort_inputs
    artifacts = build_cohort(events_path, states_path, tmp_path / "cohort")

    diagnostics = _rows_csv(artifacts["overlap_diagnostics"])
    temporal = _rows_csv(artifacts["temporal_quality"])

    assert any(
        row["temporal_rule"] == "same_start_date"
        and row["spatial_rule"] == "geometry_intersects"
        and int(row["candidate_pair_count"]) >= 1
        for row in diagnostics
    )
    assert any(
        row["spatial_rule"] == "representative_point_distance_le_10km" for row in diagnostics
    )
    assert any(row["spatial_rule"] == "iou_ge_0.05" for row in diagnostics)
    assert any(row["metric"] == "end_date_before_start_date" for row in temporal)
    assert {row["scope"] for row in temporal} == {"eligible_primary", "source_us_intersecting"}


def test_cohort_state_year_counts_include_terminal_scope(
    cohort_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events_path, states_path = cohort_inputs
    artifacts = build_cohort(events_path, states_path, tmp_path / "cohort")
    state_year_rows = _rows_csv(artifacts["state_year_counts"])
    summary = json.loads(artifacts["cohort_summary"].read_text(encoding="utf-8"))

    assert {
        "state_code",
        "year",
        "candidate_split",
        "terminal_category",
        "study_domain",
        "primary_exclusion_reason",
        "count",
    } <= set(state_year_rows[0])
    assert any(
        row["terminal_category"] == "excluded"
        and row["candidate_split"] == "not_applicable"
        and row["primary_exclusion_reason"] == "outside_temporal_window"
        for row in state_year_rows
    )
    assert any(
        row["terminal_category"] == "eligible"
        and row["candidate_split"] == "development"
        and row["primary_exclusion_reason"] == "not_applicable"
        for row in state_year_rows
    )
    assert set(summary["temporal_quality"]) == {"eligible_primary", "source_us_intersecting"}


def test_cohort_max_rows_bounds_everything(
    cohort_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events_path, states_path = cohort_inputs
    artifacts = build_cohort(events_path, states_path, tmp_path / "cohort", max_rows=5)
    summary = json.loads(artifacts["cohort_summary"].read_text(encoding="utf-8"))

    assert summary["row_accounting"]["source_us_intersecting_rows"] == 5
    assert summary["row_accounting"]["balanced"] is True
    assert len(_rows(artifacts["eligible_event_records"] / "part-00000.parquet")) == 2


def test_cohort_summary_is_deterministic_and_geoparquet_preserved(
    cohort_inputs: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    events_path, states_path = cohort_inputs
    first = build_cohort(events_path, states_path, tmp_path / "first")
    second = build_cohort(events_path, states_path, tmp_path / "second")

    assert _sha256(first["cohort_summary"]) == _sha256(second["cohort_summary"])
    metadata = pq.read_metadata(first["eligible_event_records"] / "part-00000.parquet").metadata
    assert metadata is not None
    assert json.loads(metadata[b"geo"].decode("utf-8"))["primary_column"] == "geometry"


@pytest.fixture()
def cohort_inputs(tmp_path: Path) -> tuple[Path, Path]:
    events_path = tmp_path / "events_enriched"
    states_path = tmp_path / "event_state_overlaps"
    events_path.mkdir()
    states_path.mkdir()
    events = [
        _event(
            "00000000-0000-0000-0000-000000000001", "2022-01-01", "2022-01-01", "CA", _box(0, 0)
        ),
        _event(
            "00000000-0000-0000-0000-000000000002",
            "2022-01-01",
            "2022-01-02",
            "CA",
            _box(0.02, 0.0),
            multi=True,
            state_count=2,
        ),
        _event(
            "00000000-0000-0000-0000-000000000003", "2022-02-01", "2022-02-01", "AK", _box(10, 0)
        ),
        _event(
            "00000000-0000-0000-0000-000000000004", "2024-06-01", "2024-06-02", "HI", _box(20, 0)
        ),
        _event(
            "00000000-0000-0000-0000-000000000005", "2025-03-01", "2025-03-02", "PR", _box(30, 0)
        ),
        _event(
            "00000000-0000-0000-0000-000000000006", "2025-04-01", "2025-04-02", "GU", _box(40, 0)
        ),
        _event(
            "00000000-0000-0000-0000-000000000007", "2021-12-31", "2022-01-01", "CA", _box(50, 0)
        ),
        _event(
            "00000000-0000-0000-0000-000000000008",
            "2024-07-01",
            "2024-07-01",
            "CA",
            _box(0.02, 0.0),
            country=None,
        ),
        _event(
            "00000000-0000-0000-0000-000000000009",
            "2025-07-01",
            "2025-07-04",
            "DC",
            _box(60, 0),
            review=True,
        ),
        _event("00000000-0000-0000-0000-000000000010", None, None, "CA", _box(70, 0)),
        _event(
            "00000000-0000-0000-0000-000000000011", "2023-01-03", "2023-01-01", "CA", _box(80, 0)
        ),
        _event(
            "00000000-0000-0000-0000-000000000012",
            "2023-01-01",
            "2023-01-01",
            None,
            _box(90, 0),
            state_count=0,
        ),
    ]
    states = [
        _state("00000000-0000-0000-0000-000000000001", "CA", "California", True),
        _state("00000000-0000-0000-0000-000000000002", "CA", "California", True, 0.6),
        _state("00000000-0000-0000-0000-000000000002", "NV", "Nevada", False, 0.4),
        _state("00000000-0000-0000-0000-000000000003", "AK", "Alaska", True),
        _state("00000000-0000-0000-0000-000000000004", "HI", "Hawaii", True),
        _state("00000000-0000-0000-0000-000000000005", "PR", "Puerto Rico", True),
        _state("00000000-0000-0000-0000-000000000006", "GU", "Guam", True),
        _state("00000000-0000-0000-0000-000000000007", "CA", "California", True),
        _state("00000000-0000-0000-0000-000000000008", "CA", "California", True),
        _state("00000000-0000-0000-0000-000000000009", "DC", "District of Columbia", True),
        _state("00000000-0000-0000-0000-000000000010", "CA", "California", True),
        _state("00000000-0000-0000-0000-000000000011", "CA", "California", True),
    ]
    _write_events(events_path / "part-00000.parquet", events)
    _write_states(states_path / "part-00000.parquet", states)
    return events_path, states_path


def _box(x: float, y: float) -> Polygon:
    return Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1)])


def _event(
    source_id: str,
    start: str | None,
    end: str | None,
    state: str | None,
    geometry: Polygon,
    multi: bool = False,
    state_count: int = 1,
    country: str | None = "US",
    review: bool = False,
) -> dict[str, object]:
    return {
        "source_record_id": source_id,
        "event_start_date": date.fromisoformat(start) if start else None,
        "event_end_date": date.fromisoformat(end) if end else None,
        "primary_country_code": country,
        "intersects_us_state_union": True,
        "primary_state_code": state,
        "primary_state_name": state,
        "state_candidate_count": state_count,
        "multi_state_event": multi,
        "spatial_review_required": review,
        "spatial_review_reason": "synthetic_review" if review else "",
        "event_area_km2": 1.0,
        "us_overlap_area_km2": 1.0,
        "representative_longitude": float(geometry.representative_point().x),
        "representative_latitude": float(geometry.representative_point().y),
        "geometry": geometry.wkb,
    }


def _state(
    source_id: str,
    code: str,
    name: str,
    primary: bool,
    fraction: float = 1.0,
) -> dict[str, object]:
    return {
        "source_record_id": source_id,
        "state_code": code,
        "state_name": name,
        "overlap_area_km2": fraction,
        "event_area_fraction": fraction,
        "us_overlap_fraction": fraction,
        "assignment_rank": 1 if primary else 2,
        "is_primary_state": primary,
    }


def _write_events(path: Path, rows: list[dict[str, object]]) -> None:
    schema = pa.schema(
        [
            pa.field("source_record_id", pa.string()),
            pa.field("event_start_date", pa.date32()),
            pa.field("event_end_date", pa.date32()),
            pa.field("primary_country_code", pa.string()),
            pa.field("intersects_us_state_union", pa.bool_()),
            pa.field("primary_state_code", pa.string()),
            pa.field("primary_state_name", pa.string()),
            pa.field("state_candidate_count", pa.int64()),
            pa.field("multi_state_event", pa.bool_()),
            pa.field("spatial_review_required", pa.bool_()),
            pa.field("spatial_review_reason", pa.string()),
            pa.field("event_area_km2", pa.float64()),
            pa.field("us_overlap_area_km2", pa.float64()),
            pa.field("representative_longitude", pa.float64()),
            pa.field("representative_latitude", pa.float64()),
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
            ).encode("utf-8")
        },
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _write_states(path: Path, rows: list[dict[str, object]]) -> None:
    schema = pa.schema(
        [
            pa.field("source_record_id", pa.string()),
            pa.field("state_code", pa.string()),
            pa.field("state_name", pa.string()),
            pa.field("overlap_area_km2", pa.float64()),
            pa.field("event_area_fraction", pa.float64()),
            pa.field("us_overlap_fraction", pa.float64()),
            pa.field("assignment_rank", pa.int64()),
            pa.field("is_primary_state", pa.bool_()),
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def _rows_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
