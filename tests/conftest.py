from __future__ import annotations

import json
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import MultiPolygon, Polygon


@pytest.fixture()
def groundsource_path(tmp_path: Path) -> Path:
    path = tmp_path / "groundsource-geoparquet.parquet"
    valid_polygon = Polygon([(-75.0, 40.0), (-74.0, 40.0), (-74.0, 41.0), (-75.0, 41.0)])
    valid_multipolygon = MultiPolygon(
        [
            Polygon([(-95.0, 29.0), (-94.0, 29.0), (-94.0, 30.0), (-95.0, 30.0)]),
            Polygon([(-93.0, 28.0), (-92.0, 28.0), (-92.0, 29.0), (-93.0, 29.0)]),
        ]
    )
    empty_polygon = Polygon()
    invalid_but_decodable = Polygon([(0.0, 0.0), (1.0, 1.0), (1.0, 0.0), (0.0, 1.0)])
    duplicate_uuid = str(uuid.uuid4())
    exact_duplicate_uuid = str(uuid.uuid4())

    table = pa.table(
        {
            "uuid": [
                str(uuid.uuid4()),
                duplicate_uuid,
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                duplicate_uuid,
                str(uuid.uuid4()),
                exact_duplicate_uuid,
                exact_duplicate_uuid,
            ],
            "area_km2": [10.5, None, 5.0, 2.0, None, 11.0, 3.0, 7.0, 7.0],
            "geometry": [
                valid_polygon.wkb,
                valid_multipolygon.wkb,
                b"not-wkb",
                empty_polygon.wkb,
                invalid_but_decodable.wkb,
                valid_polygon.wkb,
                valid_polygon.wkb,
                valid_polygon.wkb,
                valid_polygon.wkb,
            ],
            "start_date": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
                "2026-01-06",
                "not-a-date",
                "2026-01-08",
                "2026-01-08",
            ],
            "end_date": [
                "2026-01-02",
                None,
                "2026-01-04",
                None,
                "2026-01-06",
                "2026-01-07",
                "2026-01-08",
                "2026-01-09",
                "2026-01-09",
            ],
            "__index_level_0__": [0, 1, 2, 3, 4, 5, 6, 7, 8],
            "unexpected_source_field": ["x0", "x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8"],
        }
    )
    _write_geoparquet(path, table)
    return path


@pytest.fixture()
def clean_groundsource_path(tmp_path: Path) -> Path:
    path = tmp_path / "clean-groundsource.parquet"
    polygon = Polygon([(-75.0, 40.0), (-74.0, 40.0), (-74.0, 41.0), (-75.0, 41.0)])
    multipolygon = MultiPolygon(
        [Polygon([(-95.0, 29.0), (-94.0, 29.0), (-94.0, 30.0), (-95.0, 30.0)])]
    )
    table = pa.table(
        {
            "uuid": [str(uuid.uuid4()), str(uuid.uuid4())],
            "area_km2": [10.5, None],
            "geometry": [polygon.wkb, multipolygon.wkb],
            "start_date": ["2026-01-01", "2026-01-02"],
            "end_date": ["2026-01-02", None],
            "__index_level_0__": [0, 1],
        }
    )
    _write_geoparquet(path, table)
    return path


@pytest.fixture()
def multi_row_group_groundsource_path(tmp_path: Path) -> Path:
    path = tmp_path / "multi-row-group-groundsource.parquet"
    polygon = Polygon([(-75.0, 40.0), (-74.0, 40.0), (-74.0, 41.0), (-75.0, 41.0)])
    duplicate_uuid = str(uuid.uuid4())
    table = pa.table(
        {
            "uuid": [
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                duplicate_uuid,
                duplicate_uuid,
                str(uuid.uuid4()),
                str(uuid.uuid4()),
            ],
            "area_km2": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            "geometry": [
                polygon.wkb,
                polygon.wkb,
                polygon.wkb,
                polygon.wkb,
                polygon.wkb,
                polygon.wkb,
                b"not-wkb",
                polygon.wkb,
            ],
            "start_date": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
                "2026-01-06",
                "2026-01-07",
                "not-a-date",
            ],
            "end_date": [
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
                "2026-01-06",
                "2026-01-07",
                "2026-01-08",
                "2026-01-09",
            ],
            "__index_level_0__": list(range(8)),
        }
    )
    _write_geoparquet(path, table, row_group_size=3)
    return path


def _write_geoparquet(
    path: Path,
    table: pa.Table,
    geometry_type_key: str = "geometry_types",
    row_group_size: int | None = None,
) -> None:
    geo_metadata = {
        "version": "0.4.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
                geometry_type_key: ["Polygon", "MultiPolygon"],
                "bbox": [-180.0, -76.812618, 180.0, 81.164611],
            }
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo_metadata).encode("utf-8")})
    pq.write_table(table, path, row_group_size=row_group_size)
