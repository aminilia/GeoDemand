from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import Polygon

from conftest import _write_geoparquet
from geodemand.ingestion.groundsource import (
    GroundsourceError,
    audit_groundsource,
    filter_groundsource,
    inspect_groundsource,
    profile_groundsource,
    validate_groundsource,
    write_canonical_groundsource,
)


def test_inspect_groundsource_reports_raw_schema_without_canonical_fields(
    groundsource_path: Path,
) -> None:
    inspection = inspect_groundsource(groundsource_path)

    assert inspection.columns == [
        "uuid",
        "area_km2",
        "geometry",
        "start_date",
        "end_date",
        "__index_level_0__",
        "unexpected_source_field",
    ]
    assert inspection.missing_required_fields == []
    assert inspection.geoparquet.version == "0.4.0"
    assert inspection.geoparquet.primary_column == "geometry"
    assert inspection.geoparquet.encoding == "WKB"
    assert inspection.geoparquet.source_crs == "EPSG:4326"
    assert inspection.geoparquet.geometry_types == ["Polygon", "MultiPolygon"]
    assert inspection.geoparquet.bbox == [-180.0, -76.812618, 180.0, 81.164611]
    assert inspection.ignored_fields == ["__index_level_0__"]
    assert inspection.unknown_fields == ["unexpected_source_field"]
    assert all("mapping" not in key for key in inspection.to_dict())


def test_actual_six_column_source_schema_is_supported(clean_groundsource_path: Path) -> None:
    schema = {
        entry["name"]: entry["arrow_type"]
        for entry in inspect_groundsource(clean_groundsource_path).schema
    }

    assert schema == {
        "uuid": "string",
        "area_km2": "double",
        "geometry": "binary",
        "start_date": "string",
        "end_date": "string",
        "__index_level_0__": "int64",
    }


def test_geoparquet_geometry_type_singular_metadata_is_supported(tmp_path: Path) -> None:
    path = tmp_path / "singular-geometry-type.parquet"
    polygon = Polygon([(-75.0, 40.0), (-74.0, 40.0), (-74.0, 41.0), (-75.0, 41.0)])
    table = pa.table(
        {
            "uuid": ["550e8400-e29b-41d4-a716-446655440000"],
            "area_km2": [10.5],
            "geometry": [polygon.wkb],
            "start_date": ["2026-01-01"],
            "end_date": ["2026-01-02"],
            "__index_level_0__": [0],
        }
    )
    _write_geoparquet(path, table, geometry_type_key="geometry_type")

    inspection = inspect_groundsource(path)

    assert inspection.geoparquet.version == "0.4.0"
    assert inspection.geoparquet.geometry_types == ["Polygon", "MultiPolygon"]


def test_validate_groundsource_counts_wkb_geometry_failures(groundsource_path: Path) -> None:
    with pytest.raises(GroundsourceError, match="3 rejected records"):
        validate_groundsource(groundsource_path)

    output_dir = groundsource_path.parent / "audit"
    artifacts = audit_groundsource(groundsource_path, output_dir)
    rejected = json.loads(artifacts["rejected_records_summary"].read_text(encoding="utf-8"))
    profile = json.loads(artifacts["profile"].read_text(encoding="utf-8"))

    assert rejected["rejection_reasons"]["undecodable_geometry"] == 1
    assert rejected["rejection_reasons"]["empty_geometry"] == 1
    assert rejected["rejection_reasons"]["malformed_start_date"] == 1
    assert rejected["duplicate_uuid"]["groups"] == 2
    assert rejected["duplicate_uuid"]["participating_rows"] == 4
    assert rejected["duplicate_uuid"]["exact_duplicate_records"] == 1
    assert rejected["duplicate_uuid"]["conflicting_duplicate_records"] == 2
    assert rejected["duplicate_uuid"]["geometry_conflicts"] == 1
    assert rejected["duplicate_uuid"]["date_conflicts"] == 1
    assert rejected["duplicate_uuid"]["reported_area_conflicts"] == 1
    assert profile["quarantined_record_count"] == 5
    assert profile["row_accounting"]["balanced"] is True
    assert profile["geometry_type_distribution"] == {"MultiPolygon": 1, "Polygon": 7}
    assert profile["invalid_geometry_count"] == 1
    assert profile["empty_geometry_count"] == 1
    assert profile["undecodable_geometry_count"] == 1
    assert artifacts["quarantine"].exists()
    quarantine_rows = pq.read_table(artifacts["quarantine"]).to_pylist()
    assert len(quarantine_rows) == 5
    assert {"source_uuid", "source_row_index", "conflict_reason"}.issubset(set(quarantine_rows[0]))


def test_representative_point_extraction_uses_lon_lat(
    clean_groundsource_path: Path, tmp_path: Path
) -> None:
    output_path = tmp_path / "canonical.parquet"

    result = write_canonical_groundsource(clean_groundsource_path, output_path, batch_size=1)
    table = pq.read_table(output_path)
    rows = table.to_pylist()

    assert result.input_rows == 2
    assert result.output_rows == 2
    assert rows[0]["geometry_type"] == "Polygon"
    assert -75.0 <= rows[0]["representative_longitude"] <= -74.0
    assert 40.0 <= rows[0]["representative_latitude"] <= 41.0
    assert rows[1]["geometry_type"] == "MultiPolygon"
    assert rows[1]["event_end_date"] is None
    assert rows[1]["reported_area_km2"] is None


def test_canonical_output_preserves_geoparquet_metadata(
    clean_groundsource_path: Path,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "canonical.parquet"

    write_canonical_groundsource(clean_groundsource_path, output_path)
    metadata = pq.read_metadata(output_path).metadata

    assert metadata is not None
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    assert geo["version"] == "0.4.0"
    assert geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"]["encoding"] == "WKB"


def test_profile_groundsource_is_deterministic(
    clean_groundsource_path: Path, tmp_path: Path
) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    profile_groundsource(clean_groundsource_path, first)
    profile_groundsource(clean_groundsource_path, second)

    assert first.read_text(encoding="utf-8") == second.read_text(encoding="utf-8")


def test_audit_non_manifest_outputs_are_deterministic(
    groundsource_path: Path,
    tmp_path: Path,
) -> None:
    first = audit_groundsource(groundsource_path, tmp_path / "first")
    second = audit_groundsource(groundsource_path, tmp_path / "second")

    for artifact_name in [
        "schema",
        "profile",
        "field_quality",
        "temporal_coverage",
        "geometry_quality",
        "rejected_records_summary",
    ]:
        assert first[artifact_name].read_bytes() == second[artifact_name].read_bytes()


@pytest.mark.parametrize(
    ("duplicate_policy", "accepted_rows", "quarantined_rows", "rejected_rows"),
    [
        ("quarantine", 1, 5, 3),
        ("keep-first", 3, 3, 3),
        ("keep-last", 3, 3, 3),
        ("reject-all", 1, 1, 7),
    ],
)
def test_duplicate_uuid_policy_controls_terminal_category(
    groundsource_path: Path,
    tmp_path: Path,
    duplicate_policy: str,
    accepted_rows: int,
    quarantined_rows: int,
    rejected_rows: int,
) -> None:
    result = write_canonical_groundsource(
        groundsource_path,
        tmp_path / f"{duplicate_policy}.parquet",
        duplicate_policy=duplicate_policy,  # type: ignore[arg-type]
    )

    assert result.input_rows == accepted_rows + quarantined_rows + rejected_rows
    assert result.output_rows == accepted_rows
    assert result.quarantined_rows == quarantined_rows
    assert result.rejected_rows == rejected_rows


def test_preserve_invalid_geometries_is_explicit_opt_in(
    groundsource_path: Path,
    tmp_path: Path,
) -> None:
    default_result = write_canonical_groundsource(groundsource_path, tmp_path / "default.parquet")
    preserve_result = write_canonical_groundsource(
        groundsource_path,
        tmp_path / "preserve.parquet",
        preserve_invalid_geometries=True,
    )

    assert default_result.quarantined_rows == preserve_result.quarantined_rows + 1
    assert preserve_result.output_rows == default_result.output_rows + 1


def test_bounded_audit_options_limit_smoke_test_scope(
    clean_groundsource_path: Path,
    tmp_path: Path,
) -> None:
    artifacts = audit_groundsource(
        clean_groundsource_path,
        tmp_path / "audit",
        batch_size=1,
        max_rows=1,
        max_row_groups=1,
    )
    profile = json.loads(artifacts["profile"].read_text(encoding="utf-8"))
    manifest = json.loads(artifacts["manifest"].read_text(encoding="utf-8"))

    assert profile["total_record_count"] == 1
    assert manifest["configuration"]["batch_size"] == 1
    assert manifest["configuration"]["max_rows"] == 1
    assert manifest["configuration"]["max_row_groups"] == 1


def test_country_filtering_requires_boundary_dataset(
    clean_groundsource_path: Path,
    tmp_path: Path,
) -> None:
    with pytest.raises(GroundsourceError, match="country column"):
        filter_groundsource(clean_groundsource_path, tmp_path / "out", country="US")


def test_missing_required_source_fields_raise_clear_error(tmp_path: Path) -> None:
    path = tmp_path / "missing.parquet"
    pq.write_table(pa.table({"uuid": ["550e8400-e29b-41d4-a716-446655440000"]}), path)

    inspection = inspect_groundsource(path)

    assert inspection.missing_required_fields == ["geometry", "start_date"]
    with pytest.raises(GroundsourceError, match="missing required fields"):
        validate_groundsource(path)
