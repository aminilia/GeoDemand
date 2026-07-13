from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely
from pyproj import Transformer
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from geodemand import __version__

COUNTRY_VERSION = "5.1.1"
STATE_VERSION = "2025"
COUNTRY_ARCHIVE = Path("natural_earth") / COUNTRY_VERSION / "ne_10m_admin_0_countries.zip"
STATE_ARCHIVE = Path("census") / STATE_VERSION / "tl_2025_us_state.zip"
UNRESOLVED_CODES = {"", "-99"}


class BoundaryError(ValueError):
    """Raised when boundary archives or prepared datasets are invalid."""


@dataclass(frozen=True)
class PreparedBoundaryPaths:
    countries: Path
    states: Path
    manifest: Path


def inspect_boundaries(boundary_root: Path) -> dict[str, Any]:
    root = boundary_root.expanduser().resolve()
    countries = root / COUNTRY_ARCHIVE
    states = root / STATE_ARCHIVE
    return {
        "boundary_root": str(root),
        "expected_inputs": {
            "natural_earth_countries": str(countries),
            "census_us_states": str(states),
        },
        "exists": {
            "natural_earth_countries": countries.exists(),
            "census_us_states": states.exists(),
        },
        "versions": {
            "natural_earth": COUNTRY_VERSION,
            "natural_earth_worldview": "de_facto",
            "census_tiger_line": STATE_VERSION,
            "census_legal_boundaries_as_of": "2025-01-01",
        },
    }


def prepare_boundaries(boundary_root: Path, output_dir: Path) -> PreparedBoundaryPaths:
    started_at = time.perf_counter()
    root = boundary_root.expanduser().resolve()
    country_archive = root / COUNTRY_ARCHIVE
    state_archive = root / STATE_ARCHIVE
    _require_file(country_archive, "Natural Earth countries archive")
    _require_file(state_archive, "Census TIGER/Line states archive")

    countries_source = _read_vector_archive(country_archive)
    states_source = _read_vector_archive(state_archive)
    country_invalid_count = _invalid_geometry_count(countries_source)
    state_invalid_count = _invalid_geometry_count(states_source)
    countries = _canonical_country_rows(countries_source, repair_invalid_geometry=True)
    states = _canonical_state_rows(states_source, repair_invalid_geometry=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    country_path = output_dir / "countries.parquet"
    state_path = output_dir / "us_states.parquet"
    _write_geoparquet(country_path, countries, _country_schema(), ["Polygon", "MultiPolygon"])
    _write_geoparquet(state_path, states, _state_schema(), ["Polygon", "MultiPolygon"])

    manifest_path = output_dir / "boundary_manifest.json"
    manifest = {
        "prepared_timestamp_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "package_version": __version__,
        "inputs": {
            "natural_earth_countries": {
                "path": str(country_archive),
                "sha256": _sha256(country_archive),
                "source_version": COUNTRY_VERSION,
                "worldview": "de_facto",
                "licensing_note": "Natural Earth data is public domain; verify local use terms.",
            },
            "census_us_states": {
                "path": str(state_archive),
                "sha256": _sha256(state_archive),
                "source_version": STATE_VERSION,
                "legal_boundaries_as_of": "2025-01-01",
                "licensing_note": "U.S. Census TIGER/Line files are public U.S. government data.",
            },
        },
        "outputs": {
            "countries": {
                "path": str(country_path),
                "sha256": _sha256(country_path),
                "feature_count": len(countries),
                "crs": "EPSG:4326",
                "geometry_types": sorted({str(row["geometry_type"]) for row in countries}),
                "code_fields": ["country_code_alpha2", "country_code_alpha3"],
                "invalid_geometry_normalized_count": country_invalid_count,
                "invalid_geometry_policy": "shapely.make_valid during boundary preparation",
            },
            "us_states": {
                "path": str(state_path),
                "sha256": _sha256(state_path),
                "feature_count": len(states),
                "crs": "EPSG:4326",
                "geometry_types": sorted({str(row["geometry_type"]) for row in states}),
                "code_fields": ["state_fips", "state_code", "state_geoid"],
                "invalid_geometry_normalized_count": state_invalid_count,
                "invalid_geometry_policy": "shapely.make_valid during boundary preparation",
            },
        },
        "processing_seconds": round(time.perf_counter() - started_at, 6),
    }
    _write_json(manifest_path, manifest)
    return PreparedBoundaryPaths(countries=country_path, states=state_path, manifest=manifest_path)


def _read_vector_archive(path: Path) -> list[dict[str, Any]]:
    try:
        metadata, table = pyogrio.read_arrow(path)
    except Exception as exc:  # pragma: no cover - depends on GDAL archive errors
        raise BoundaryError(f"Could not read boundary archive {path}: {exc}") from exc
    geometry_name = _geometry_column_name(metadata, table)
    rows = table.to_pylist()
    if geometry_name != "geometry":
        for row in rows:
            row["geometry"] = row.pop(geometry_name)
    return _reproject_source_rows(rows, metadata.get("crs"))


def _geometry_column_name(metadata: dict[str, Any], table: pa.Table) -> str:
    configured = str(metadata.get("geometry_name") or "")
    if configured and configured in table.column_names:
        return configured
    if "geometry" in table.column_names:
        return "geometry"
    if "wkb_geometry" in table.column_names:
        return "wkb_geometry"
    for field in table.schema:
        field_metadata = field.metadata or {}
        extension_name = field_metadata.get(b"ARROW:extension:name")
        if extension_name == b"geoarrow.wkb":
            return str(field.name)
    raise BoundaryError("Boundary archive does not contain a recognizable WKB geometry column.")


def _reproject_source_rows(
    rows: list[dict[str, Any]],
    source_crs: object,
) -> list[dict[str, Any]]:
    if source_crs is None or str(source_crs).upper() in {"EPSG:4326", "OGC:CRS84"}:
        return rows
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
    reprojected: list[dict[str, Any]] = []
    for row in rows:
        geometry = _geometry_from_row(row)
        normalized = transform(transformer.transform, geometry)
        reprojected.append({**row, "geometry": shapely.to_wkb(normalized)})
    return reprojected


def _invalid_geometry_count(rows: list[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        geometry = _geometry_from_row(row)
        if not geometry.is_valid:
            count += 1
    return count


def _canonical_country_rows(
    source_rows: list[dict[str, Any]],
    repair_invalid_geometry: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_alpha3: set[str] = set()
    for index, row in enumerate(source_rows):
        geometry = _geometry_from_row(row)
        geometry = _validate_geometry(
            geometry,
            "country",
            index,
            repair_invalid_geometry=repair_invalid_geometry,
        )
        alpha2 = _first_resolved(row, ["ISO_A2_EH", "ISO_A2"])
        alpha3 = _first_resolved(row, ["ADM0_A3", "ISO_A3_EH", "ISO_A3"])
        if alpha3 is None:
            raise BoundaryError(f"Country boundary row {index} has no resolved alpha-3 code.")
        if alpha3 in seen_alpha3:
            raise BoundaryError(f"Duplicate country alpha-3 code in boundaries: {alpha3}")
        seen_alpha3.add(alpha3)
        rows.append(
            {
                "country_code_alpha2": alpha2,
                "country_code_alpha3": alpha3,
                "country_name": _first_resolved(row, ["NAME", "NAME_EN", "ADMIN"]) or alpha3,
                "sovereign_code": _first_resolved(row, ["SOV_A3", "ADM0_A3"]),
                "boundary_status": _first_resolved(row, ["TYPE", "ADMIN"]) or "unknown",
                "source_feature_id": str(row.get("NE_ID") or row.get("featurecla") or index),
                "geometry": shapely.to_wkb(geometry),
                "geometry_type": geometry.geom_type,
                "source_dataset": "Natural Earth Admin 0 Countries",
                "source_version": COUNTRY_VERSION,
            }
        )
    return sorted(rows, key=lambda item: str(item["country_code_alpha3"]))


def _canonical_state_rows(
    source_rows: list[dict[str, Any]],
    repair_invalid_geometry: bool = False,
) -> list[dict[str, Any]]:
    required = ["STATEFP", "STUSPS", "NAME", "GEOID", "LSAD", "MTFCC", "FUNCSTAT"]
    rows: list[dict[str, Any]] = []
    seen_fips: set[str] = set()
    seen_codes: set[str] = set()
    for index, row in enumerate(source_rows):
        missing = [field for field in required if _unresolved(row.get(field))]
        if missing:
            raise BoundaryError(
                f"State boundary row {index} is missing required fields: {', '.join(missing)}"
            )
        geometry = _geometry_from_row(row)
        geometry = _validate_geometry(
            geometry,
            "state",
            index,
            repair_invalid_geometry=repair_invalid_geometry,
        )
        state_fips = str(row["STATEFP"])
        state_code = str(row["STUSPS"])
        if state_fips in seen_fips:
            raise BoundaryError(f"Duplicate state FIPS code in boundaries: {state_fips}")
        if state_code in seen_codes:
            raise BoundaryError(f"Duplicate state code in boundaries: {state_code}")
        seen_fips.add(state_fips)
        seen_codes.add(state_code)
        rows.append(
            {
                "state_fips": state_fips,
                "state_code": state_code,
                "state_name": str(row["NAME"]),
                "state_geoid": str(row["GEOID"]),
                "state_type": str(row["LSAD"]),
                "mtfcc": str(row["MTFCC"]),
                "funcstat": str(row["FUNCSTAT"]),
                "geometry": shapely.to_wkb(geometry),
                "geometry_type": geometry.geom_type,
                "source_dataset": "U.S. Census TIGER/Line States and Equivalent Entities",
                "source_version": STATE_VERSION,
            }
        )
    return sorted(rows, key=lambda item: str(item["state_code"]))


def _geometry_from_row(row: dict[str, Any]) -> BaseGeometry:
    value = row.get("geometry")
    if isinstance(value, BaseGeometry):
        return value
    if isinstance(value, bytes):
        geometry = shapely.from_wkb(value, on_invalid="ignore")
        if isinstance(geometry, BaseGeometry):
            return geometry
    raise BoundaryError("Boundary row does not contain WKB geometry.")


def _validate_geometry(
    geometry: BaseGeometry,
    label: str,
    index: int,
    repair_invalid_geometry: bool = False,
) -> BaseGeometry:
    if geometry.is_empty:
        raise BoundaryError(f"{label.title()} boundary row {index} has empty geometry.")
    if not geometry.is_valid:
        if not repair_invalid_geometry:
            raise BoundaryError(f"{label.title()} boundary row {index} has invalid geometry.")
        geometry = shapely.make_valid(geometry)
        if geometry.is_empty or not geometry.is_valid:
            raise BoundaryError(
                f"{label.title()} boundary row {index} has invalid geometry that "
                "could not be normalized."
            )
    if geometry.geom_type not in {"Polygon", "MultiPolygon"}:
        raise BoundaryError(
            f"{label.title()} boundary row {index} has unsupported geometry type: "
            f"{geometry.geom_type}"
        )
    return geometry


def _first_resolved(row: dict[str, Any], fields: list[str]) -> str | None:
    for field in fields:
        value = row.get(field)
        if not _unresolved(value):
            return str(value).strip()
    return None


def _unresolved(value: object) -> bool:
    if value is None:
        return True
    return str(value).strip() in UNRESOLVED_CODES


def _require_file(path: Path, label: str) -> None:
    if not path.exists() or not path.is_file():
        raise BoundaryError(f"{label} does not exist: {path}")


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


def _write_geoparquet(
    path: Path,
    rows: list[dict[str, Any]],
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
    table = table.replace_schema_metadata({b"geo": json.dumps(metadata, sort_keys=True).encode()})
    pq.write_table(table, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
