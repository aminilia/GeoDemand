from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform

from geodemand.boundaries import (
    BoundaryError,
    _canonical_country_rows,
    _canonical_state_rows,
    _reproject_source_rows,
)
from geodemand.spatial import AREA_TOLERANCE, _fraction_valid, enrich_spatial


def test_spatial_enrichment_assigns_countries_states_and_preserves_cases(
    spatial_groundsource_path: Path,
    prepared_boundary_paths: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    countries, states = prepared_boundary_paths
    artifacts = enrich_spatial(
        spatial_groundsource_path,
        countries,
        states,
        tmp_path / "spatial",
        batch_size=3,
    )

    events = _rows(artifacts["events_enriched"] / "part-00000.parquet")
    by_id = {row["source_record_id"]: row for row in events}

    assert len(events) == 9
    assert by_id["00000000-0000-0000-0000-000000000001"]["primary_country_code"] == "AA"
    assert by_id["00000000-0000-0000-0000-000000000002"]["country_candidate_count"] == 2
    assert by_id["00000000-0000-0000-0000-000000000003"]["primary_country_code"] is None
    assert by_id["00000000-0000-0000-0000-000000000004"]["primary_state_code"] == "CA"
    assert by_id["00000000-0000-0000-0000-000000000005"]["multi_state_event"] is True
    assert by_id["00000000-0000-0000-0000-000000000006"]["primary_country_code"] == "MX"
    assert by_id["00000000-0000-0000-0000-000000000006"]["intersects_united_states"] is True
    assert by_id["00000000-0000-0000-0000-000000000006"]["intersects_us_country_boundary"] is True
    assert by_id["00000000-0000-0000-0000-000000000006"]["intersects_us_state_union"] is True
    assert by_id["00000000-0000-0000-0000-000000000007"]["geometry_type"] == "MultiPolygon"
    assert by_id["00000000-0000-0000-0000-000000000009"]["spatial_review_required"] is True

    state_rows = _rows(artifacts["event_state_overlaps"] / "part-00000.parquet")
    assert {row["state_code"] for row in state_rows} >= {"CA", "NV"}
    assert all(0.0 <= row["event_area_fraction"] <= 1.0 for row in state_rows)
    for source_record_id in {
        row["source_record_id"] for row in state_rows if row["source_record_id"] is not None
    }:
        rows = [row for row in state_rows if row["source_record_id"] == source_record_id]
        assert sum(float(row["event_area_fraction"] or 0.0) for row in rows) <= 1.0 + 1e-8
        assert sum(float(row["us_overlap_fraction"] or 0.0) for row in rows) <= 1.0 + 1e-8


def test_state_union_denominator_handles_detailed_state_exceeding_country_boundary(
    tmp_path: Path,
) -> None:
    countries = _canonical_country_rows(
        [
            _country("US", "USA", "United States", Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])),
            _country("MX", "MEX", "Mexico", Polygon([(-1, 0), (0, 0), (0, 1), (-1, 1)])),
        ]
    )
    states = _canonical_state_rows(
        [_state("06", "CA", "California", Polygon([(0, 0), (2, 0), (2, 1), (0, 1)]))]
    )
    country_path = tmp_path / "countries.parquet"
    state_path = tmp_path / "us_states.parquet"
    _write_table(country_path, countries, _country_schema(), ["Polygon"])
    _write_table(state_path, states, _state_schema(), ["Polygon"])

    event = Polygon([(0.5, 0.2), (1.5, 0.2), (1.5, 0.8), (0.5, 0.8)])
    source = tmp_path / "coastal.parquet"
    _write_groundsource(source, _groundsource_table([event]))
    artifacts = enrich_spatial(source, country_path, state_path, tmp_path / "out")

    event_row = _rows(artifacts["events_enriched"] / "part-00000.parquet")[0]
    state_row = _rows(artifacts["event_state_overlaps"] / "part-00000.parquet")[0]

    assert event_row["intersects_us_country_boundary"] is True
    assert event_row["intersects_us_state_union"] is True
    assert event_row["us_country_boundary_overlap_area_km2"] < event_row["us_overlap_area_km2"]
    assert state_row["us_overlap_fraction"] == pytest.approx(1.0)
    assert "state_fraction_out_of_range" not in str(event_row["spatial_review_reason"])


def test_zero_area_state_union_intersection_does_not_flag_fraction(
    tmp_path: Path,
) -> None:
    countries = _canonical_country_rows(
        [_country("US", "USA", "United States", Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]))]
    )
    states = _canonical_state_rows(
        [_state("06", "CA", "California", Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]))]
    )
    country_path = tmp_path / "countries.parquet"
    state_path = tmp_path / "us_states.parquet"
    _write_table(country_path, countries, _country_schema(), ["Polygon"])
    _write_table(state_path, states, _state_schema(), ["Polygon"])

    source = tmp_path / "touch.parquet"
    _write_groundsource(
        source,
        _groundsource_table([Polygon([(-1, 0.2), (0, 0.2), (0, 0.8), (-1, 0.8)])]),
    )
    artifacts = enrich_spatial(source, country_path, state_path, tmp_path / "out")

    event_row = _rows(artifacts["events_enriched"] / "part-00000.parquet")[0]
    state_row = _rows(artifacts["event_state_overlaps"] / "part-00000.parquet")[0]

    assert event_row["intersects_us_state_union"] is True
    assert event_row["us_overlap_area_km2"] == 0.0
    assert state_row["overlap_area_km2"] == 0.0
    assert state_row["us_overlap_fraction"] is None
    assert "state_fraction_out_of_range" not in str(event_row["spatial_review_reason"])


def test_fraction_tolerance_allows_tiny_excess_but_not_material_excess() -> None:
    assert _fraction_valid(1.0 + (AREA_TOLERANCE / 2.0))
    assert not _fraction_valid(1.0 + (AREA_TOLERANCE * 2.0))


def test_spatial_enrichment_outputs_are_deterministic(
    spatial_groundsource_path: Path,
    prepared_boundary_paths: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    countries, states = prepared_boundary_paths
    first = enrich_spatial(spatial_groundsource_path, countries, states, tmp_path / "first")
    second = enrich_spatial(spatial_groundsource_path, countries, states, tmp_path / "second")

    assert _rows(first["events_enriched"] / "part-00000.parquet") == _rows(
        second["events_enriched"] / "part-00000.parquet"
    )
    assert _rows(first["event_country_membership"] / "part-00000.parquet") == _rows(
        second["event_country_membership"] / "part-00000.parquet"
    )
    assert _sha256(first["spatial_enrichment_summary"]) == _sha256(
        second["spatial_enrichment_summary"]
    )


def test_spatial_enrichment_respects_max_rows(
    spatial_groundsource_path: Path,
    prepared_boundary_paths: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    countries, states = prepared_boundary_paths
    artifacts = enrich_spatial(
        spatial_groundsource_path,
        countries,
        states,
        tmp_path / "limited",
        max_rows=5,
    )

    summary = json.loads(artifacts["spatial_enrichment_summary"].read_text(encoding="utf-8"))
    assert summary["row_accounting"] == {
        "balanced": True,
        "enriched_rows": 5,
        "rejected_rows": 0,
        "source_rows": 5,
    }


def test_spatial_enrichment_respects_max_row_groups(
    multi_row_group_spatial_path: Path,
    prepared_boundary_paths: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    countries, states = prepared_boundary_paths
    artifacts = enrich_spatial(
        multi_row_group_spatial_path,
        countries,
        states,
        tmp_path / "rowgroups",
        max_row_groups=1,
    )

    events = _rows(artifacts["events_enriched"] / "part-00000.parquet")
    assert len(events) == 2
    assert {row["source_record_id"] for row in events} == {
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    }


def test_spatial_enrichment_preserves_geoparquet_metadata(
    spatial_groundsource_path: Path,
    prepared_boundary_paths: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    countries, states = prepared_boundary_paths
    artifacts = enrich_spatial(spatial_groundsource_path, countries, states, tmp_path / "geo")
    metadata = pq.read_metadata(artifacts["events_enriched"] / "part-00000.parquet").metadata

    assert metadata is not None
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    assert geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"]["encoding"] == "WKB"


def test_boundary_reprojection_and_validation_errors() -> None:
    rows = _country_source_rows()
    countries = _canonical_country_rows(rows)
    assert countries[0]["country_code_alpha3"] == "AAA"

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    projected = transform(transformer.transform, Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]))
    reprojected = _reproject_source_rows([{**rows[0], "geometry": projected.wkb}], "EPSG:3857")
    normalized = shapely.from_wkb(reprojected[0]["geometry"])
    assert normalized.bounds == pytest.approx((0.0, 0.0, 1.0, 1.0))

    duplicate = [*rows, rows[0]]
    with pytest.raises(BoundaryError, match="Duplicate country"):
        _canonical_country_rows(duplicate)

    invalid = [
        {
            **rows[0],
            "geometry": Polygon([(0.0, 0.0), (1.0, 1.0), (1.0, 0.0), (0.0, 1.0)]).wkb,
        }
    ]
    with pytest.raises(BoundaryError, match="invalid geometry"):
        _canonical_country_rows(invalid)

    state_rows = _state_source_rows()
    del state_rows[0]["STATEFP"]
    with pytest.raises(BoundaryError, match="missing required fields"):
        _canonical_state_rows(state_rows)

    duplicate_state_rows = _state_source_rows()
    duplicate_state_rows.append(duplicate_state_rows[0])
    with pytest.raises(BoundaryError, match="Duplicate state"):
        _canonical_state_rows(duplicate_state_rows)


@pytest.fixture()
def prepared_boundary_paths(tmp_path: Path) -> tuple[Path, Path]:
    countries = _canonical_country_rows(_country_source_rows())
    states = _canonical_state_rows(_state_source_rows())
    country_path = tmp_path / "countries.parquet"
    state_path = tmp_path / "us_states.parquet"
    _write_table(country_path, countries, _country_schema(), ["Polygon", "MultiPolygon"])
    _write_table(state_path, states, _state_schema(), ["Polygon", "MultiPolygon"])
    return country_path, state_path


@pytest.fixture()
def spatial_groundsource_path(tmp_path: Path) -> Path:
    path = tmp_path / "spatial-groundsource.parquet"
    polygons = [
        Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
        Polygon([(8, 1), (12, 1), (12, 4), (8, 4)]),
        Polygon([(40, 1), (41, 1), (41, 2), (40, 2)]),
        Polygon([(101, 1), (102, 1), (102, 2), (101, 2)]),
        Polygon([(104, 1), (106, 1), (106, 2), (104, 2)]),
        Polygon([(98, 1), (103, 1), (103, 2), (98, 2)]),
        MultiPolygon(
            [
                Polygon([(1, 3), (2, 3), (2, 4), (1, 4)]),
                Polygon([(11, 3), (12, 3), (12, 4), (11, 4)]),
            ]
        ),
        Polygon([(9, 4), (10, 4), (10, 5), (9, 5)]),
        Polygon([(-179, 0), (179, 0), (179, 1), (-179, 1)]),
    ]
    table = pa.table(
        {
            "uuid": [f"00000000-0000-0000-0000-{index:012d}" for index in range(1, 10)],
            "area_km2": [float(index) for index in range(1, 10)],
            "geometry": [geometry.wkb for geometry in polygons],
            "start_date": ["2026-01-01"] * 9,
            "end_date": ["2026-01-02"] * 9,
            "__index_level_0__": list(range(9)),
        }
    )
    _write_groundsource(path, table)
    return path


@pytest.fixture()
def multi_row_group_spatial_path(spatial_groundsource_path: Path, tmp_path: Path) -> Path:
    table = pq.read_table(spatial_groundsource_path)
    path = tmp_path / "spatial-rowgroups.parquet"
    _write_groundsource(path, table, row_group_size=2)
    return path


def _country_source_rows() -> list[dict[str, object]]:
    return [
        _country("AA", "AAA", "Alpha", Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])),
        _country("BB", "BBB", "Beta", Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])),
        _country("US", "USA", "United States", Polygon([(100, 0), (110, 0), (110, 10), (100, 10)])),
        _country("MX", "MEX", "Mexico", Polygon([(95, 0), (102, 0), (102, 10), (95, 10)])),
    ]


def _state_source_rows() -> list[dict[str, object]]:
    return [
        _state("06", "CA", "California", Polygon([(100, 0), (105, 0), (105, 10), (100, 10)])),
        _state("32", "NV", "Nevada", Polygon([(105, 0), (110, 0), (110, 10), (105, 10)])),
    ]


def _country(alpha2: str, alpha3: str, name: str, geometry: Polygon) -> dict[str, object]:
    return {
        "ISO_A2_EH": alpha2,
        "ADM0_A3": alpha3,
        "NAME": name,
        "SOV_A3": alpha3,
        "TYPE": "Sovereign country",
        "NE_ID": alpha3,
        "geometry": geometry.wkb,
    }


def _state(fips: str, code: str, name: str, geometry: Polygon) -> dict[str, object]:
    return {
        "STATEFP": fips,
        "STUSPS": code,
        "NAME": name,
        "GEOID": fips,
        "LSAD": "00",
        "MTFCC": "G4000",
        "FUNCSTAT": "A",
        "geometry": geometry.wkb,
    }


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _groundsource_table(geometries: list[Polygon | MultiPolygon]) -> pa.Table:
    return pa.table(
        {
            "uuid": [
                f"10000000-0000-0000-0000-{index:012d}" for index in range(1, len(geometries) + 1)
            ],
            "area_km2": [float(index) for index in range(1, len(geometries) + 1)],
            "geometry": [geometry.wkb for geometry in geometries],
            "start_date": ["2026-01-01"] * len(geometries),
            "end_date": ["2026-01-02"] * len(geometries),
            "__index_level_0__": list(range(len(geometries))),
        }
    )


def _write_groundsource(path: Path, table: pa.Table, row_group_size: int | None = None) -> None:
    metadata = {
        "version": "0.4.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "crs": "EPSG:4326",
                "geometry_types": ["Polygon", "MultiPolygon"],
            }
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(metadata).encode("utf-8")})
    pq.write_table(table, path, row_group_size=row_group_size)


def _write_table(
    path: Path,
    rows: list[dict[str, object]],
    schema: pa.Schema,
    geometry_types: list[str],
) -> None:
    metadata = {
        "version": "0.4.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "crs": "EPSG:4326",
                "geometry_types": geometry_types,
            }
        },
    }
    table = pa.Table.from_pylist(rows, schema=schema)
    table = table.replace_schema_metadata({b"geo": json.dumps(metadata).encode("utf-8")})
    pq.write_table(table, path)


def _country_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("country_code_alpha2", pa.string()),
            pa.field("country_code_alpha3", pa.string()),
            pa.field("country_name", pa.string()),
            pa.field("sovereign_code", pa.string()),
            pa.field("boundary_status", pa.string()),
            pa.field("source_feature_id", pa.string()),
            pa.field("geometry", pa.binary()),
            pa.field("geometry_type", pa.string()),
            pa.field("source_dataset", pa.string()),
            pa.field("source_version", pa.string()),
        ]
    )


def _state_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("state_fips", pa.string()),
            pa.field("state_code", pa.string()),
            pa.field("state_name", pa.string()),
            pa.field("state_geoid", pa.string()),
            pa.field("state_type", pa.string()),
            pa.field("mtfcc", pa.string()),
            pa.field("funcstat", pa.string()),
            pa.field("geometry", pa.binary()),
            pa.field("geometry_type", pa.string()),
            pa.field("source_dataset", pa.string()),
            pa.field("source_version", pa.string()),
        ]
    )
