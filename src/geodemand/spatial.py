from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import Transformer
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform
from shapely.strtree import STRtree

from geodemand import __version__
from geodemand.ingestion.groundsource import (
    _iter_limited_batches,
    _process_batch,
    inspect_groundsource,
)

LOGGER = logging.getLogger(__name__)
AREA_CRS = "EPSG:6933"
AREA_TOLERANCE = 1e-9
AREA_TRANSFORMER = Transformer.from_crs("EPSG:4326", AREA_CRS, always_xy=True)


class SpatialEnrichmentError(ValueError):
    """Raised when spatial enrichment cannot be completed."""


@dataclass(frozen=True)
class BoundaryFeature:
    code: str
    name: str
    geometry: BaseGeometry
    projected_geometry: BaseGeometry
    properties: dict[str, Any]


@dataclass(frozen=True)
class BoundaryIndex:
    features: list[BoundaryFeature]
    tree: STRtree
    version: str
    union: BaseGeometry | None = None
    projected_union: BaseGeometry | None = None


@dataclass
class SpatialAccumulator:
    source_rows: int = 0
    enriched_rows: int = 0
    rejected_rows: int = 0
    country_assigned: int = 0
    country_unassigned: int = 0
    us_intersecting: int = 0
    state_assigned: int = 0
    spatial_review: int = 0
    manual_review: int = 0
    offshore_unassigned: int = 0
    cross_country: int = 0
    multistate: int = 0
    processing_seconds: float = 0.0
    rows_per_second: float = 0.0

    @property
    def balanced(self) -> bool:
        return self.source_rows == self.enriched_rows + self.rejected_rows


def enrich_spatial(
    input_path: Path,
    countries_path: Path,
    states_path: Path,
    output_dir: Path,
    batch_size: int = 10_000,
    max_rows: int | None = None,
    max_row_groups: int | None = None,
) -> dict[str, Path]:
    started_at = time.perf_counter()
    inspection = inspect_groundsource(input_path)
    countries = _load_countries(countries_path)
    states = _load_states(states_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    artifacts = {
        "events_enriched": output_dir / "events_enriched",
        "event_country_membership": output_dir / "event_country_membership",
        "event_state_overlaps": output_dir / "event_state_overlaps",
        "us_events": output_dir / "us_events",
        "spatial_enrichment_summary": output_dir / "spatial_enrichment_summary.json",
        "country_assignment_quality": output_dir / "country_assignment_quality.csv",
        "state_assignment_quality": output_dir / "state_assignment_quality.csv",
        "boundary_manifest": output_dir / "boundary_manifest.json",
        "manifest": output_dir / "manifest.json",
    }
    writers = _SpatialWriters(artifacts, inspection.geoparquet.raw)
    accumulator = SpatialAccumulator()
    try:
        for audit_batch in _iter_limited_batches(
            input_path,
            batch_size=batch_size,
            max_rows=max_rows,
            max_row_groups=max_row_groups,
        ):
            result = _process_batch(
                audit_batch.batch,
                inspection.geoparquet,
                row_offset=audit_batch.row_offset,
            )
            accumulator.source_rows += audit_batch.batch.num_rows
            accumulator.rejected_rows += result.rejected_rows
            batch_enriched: list[dict[str, Any]] = []
            batch_country: list[dict[str, Any]] = []
            batch_state: list[dict[str, Any]] = []
            batch_us: list[dict[str, Any]] = []
            for row in result.rows:
                enriched, memberships, state_overlaps = _enrich_row(row, countries, states)
                batch_enriched.append(enriched)
                batch_country.extend(memberships)
                batch_state.extend(state_overlaps)
                if bool(enriched["intersects_united_states"]):
                    batch_us.append(enriched)
                _merge_enriched(accumulator, enriched)
            accumulator.enriched_rows += len(batch_enriched)
            writers.write(batch_enriched, batch_country, batch_state, batch_us)
            _log_progress(audit_batch.batch_number, accumulator, started_at)
    finally:
        writers.close()

    accumulator.processing_seconds = time.perf_counter() - started_at
    accumulator.rows_per_second = (
        accumulator.source_rows / accumulator.processing_seconds
        if accumulator.processing_seconds > 0
        else 0.0
    )
    summary = _summary_payload(accumulator)
    _write_json(artifacts["spatial_enrichment_summary"], summary)
    _write_assignment_quality(
        artifacts["country_assignment_quality"],
        summary["country_assignment"],
    )
    _write_assignment_quality(artifacts["state_assignment_quality"], summary["state_assignment"])
    _write_json(
        artifacts["boundary_manifest"],
        {
            "countries": _boundary_manifest_entry(countries_path, countries),
            "states": _boundary_manifest_entry(states_path, states),
        },
    )
    _write_json(
        artifacts["manifest"],
        {
            "run_timestamp_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "package_version": __version__,
            "source_file_name": input_path.name,
            "source_sha256": _sha256(input_path),
            "output_dir": str(output_dir),
            "run_id": str(uuid.uuid4()),
            "configuration": {
                "batch_size": batch_size,
                "max_rows": max_rows,
                "max_row_groups": max_row_groups,
                "area_crs": AREA_CRS,
                "area_fraction_tolerance": AREA_TOLERANCE,
            },
            "row_accounting": {
                "source_rows": accumulator.source_rows,
                "enriched_rows": accumulator.enriched_rows,
                "rejected_rows": accumulator.rejected_rows,
                "balanced": accumulator.balanced,
            },
            "timing": {
                "processing_seconds": round(accumulator.processing_seconds, 6),
                "rows_per_second": round(accumulator.rows_per_second, 3),
            },
        },
    )
    return artifacts


def _enrich_row(
    row: dict[str, Any],
    countries: BoundaryIndex,
    states: BoundaryIndex,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    geometry = shapely.from_wkb(row["geometry"])
    projected_geometry = _project_geometry(geometry)
    representative = geometry.representative_point()
    event_area = _projected_area_km2(projected_geometry)
    country_matches = _country_matches(
        row,
        geometry,
        projected_geometry,
        representative,
        event_area,
        countries,
    )
    primary_country, country_method, country_ambiguous = _primary_country(country_matches)
    intersects_us_country_boundary, country_boundary_us_overlap_area = _us_overlap(
        geometry,
        projected_geometry,
        countries,
    )
    intersects_us_state_union, us_overlap_area = _us_overlap(geometry, projected_geometry, states)
    intersects_us = intersects_us_state_union
    us_fraction = _fraction(us_overlap_area, event_area)
    state_overlaps: list[dict[str, Any]] = []
    primary_state: dict[str, Any] | None = None
    if intersects_us_state_union:
        state_overlaps = _state_matches(
            row,
            geometry,
            projected_geometry,
            event_area,
            us_overlap_area,
            states,
        )
        primary_state = state_overlaps[0] if state_overlaps else None

    review_reasons: list[str] = []
    if bool(row["antimeridian_review"]):
        review_reasons.append("antimeridian_review")
    if country_method == "manual_review":
        review_reasons.append("country_assignment_ambiguous")
    if primary_country is None:
        review_reasons.append("country_unassigned")
    if not _fraction_valid(us_fraction):
        review_reasons.append("us_overlap_fraction_out_of_range")
    for match in country_matches:
        if not _fraction_valid(match["event_area_fraction"]):
            review_reasons.append("country_fraction_out_of_range")
            break
    for match in state_overlaps:
        if not _fraction_valid(match["event_area_fraction"]) or not _fraction_valid(
            match["us_overlap_fraction"]
        ):
            review_reasons.append("state_fraction_out_of_range")
            break

    state_method = "maximum_overlap" if primary_state is not None else "unassigned"
    enriched = {
        **row,
        "primary_country_code": (
            primary_country["country_code"] if primary_country is not None else None
        ),
        "primary_country_name": (
            primary_country["country_name"] if primary_country is not None else None
        ),
        "country_assignment_method": country_method,
        "country_candidate_count": len(country_matches),
        "country_assignment_ambiguous": country_ambiguous,
        "intersects_united_states": intersects_us,
        "intersects_us_country_boundary": intersects_us_country_boundary,
        "intersects_us_state_union": intersects_us_state_union,
        "us_overlap_area_km2": us_overlap_area,
        "us_country_boundary_overlap_area_km2": country_boundary_us_overlap_area,
        "us_event_area_fraction": us_fraction,
        "primary_state_code": primary_state["state_code"] if primary_state is not None else None,
        "primary_state_name": primary_state["state_name"] if primary_state is not None else None,
        "state_assignment_method": state_method,
        "state_candidate_count": len(state_overlaps),
        "multi_state_event": len(state_overlaps) > 1,
        "spatial_review_required": bool(review_reasons),
        "spatial_review_reason": ";".join(dict.fromkeys(review_reasons)),
        "country_boundary_version": countries.version,
        "state_boundary_version": states.version,
        "event_area_km2": event_area,
    }
    return enriched, country_matches, state_overlaps


def _country_matches(
    row: dict[str, Any],
    geometry: BaseGeometry,
    projected_geometry: BaseGeometry,
    representative: BaseGeometry,
    event_area: float,
    countries: BoundaryIndex,
) -> list[dict[str, Any]]:
    point_matches = set(_query_indices(countries, representative, predicate="intersects"))
    point_matches = {
        index
        for index in point_matches
        if countries.features[index].geometry.covers(representative)
    }
    intersecting = set(_query_indices(countries, geometry, predicate="intersects"))
    matches: list[dict[str, Any]] = []
    for index in sorted(intersecting, key=lambda value: countries.features[value].code):
        feature = countries.features[index]
        overlap_area = _projected_area_km2(
            projected_geometry.intersection(feature.projected_geometry)
        )
        if overlap_area <= 0 and not geometry.touches(feature.geometry):
            continue
        matches.append(
            {
                "source_record_id": row["source_record_id"],
                "country_code": feature.properties["country_code_alpha2"]
                or feature.properties["country_code_alpha3"],
                "country_name": feature.name,
                "representative_point_match": index in point_matches,
                "geometry_intersects": True,
                "overlap_area_km2": overlap_area,
                "event_area_fraction": _fraction(overlap_area, event_area),
                "assignment_rank": 0,
                "_alpha3": feature.properties["country_code_alpha3"],
            }
        )
    matches.sort(
        key=lambda item: (
            -float(item["overlap_area_km2"]),
            str(item["country_code"] or ""),
            str(item["_alpha3"]),
        )
    )
    for rank, match in enumerate(matches, start=1):
        match["assignment_rank"] = rank
        del match["_alpha3"]
    return matches


def _primary_country(
    matches: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str, bool]:
    if not matches:
        return None, "unassigned", False
    point_matches = [match for match in matches if bool(match["representative_point_match"])]
    if len(point_matches) == 1:
        return point_matches[0], "representative_point", False
    if len(matches) == 1:
        return matches[0], "single_intersection", False
    first = matches[0]
    tied = [
        match
        for match in matches
        if abs(float(match["overlap_area_km2"]) - float(first["overlap_area_km2"]))
        <= AREA_TOLERANCE
    ]
    if len(tied) > 1:
        return first, "manual_review", True
    return first, "maximum_overlap", len(point_matches) > 1


def _us_overlap(
    geometry: BaseGeometry,
    projected_geometry: BaseGeometry,
    countries: BoundaryIndex,
) -> tuple[bool, float]:
    if countries.union is None or countries.projected_union is None:
        return False, 0.0
    if not geometry.intersects(countries.union):
        return False, 0.0
    return True, _projected_area_km2(projected_geometry.intersection(countries.projected_union))


def _state_matches(
    row: dict[str, Any],
    geometry: BaseGeometry,
    projected_geometry: BaseGeometry,
    event_area: float,
    us_overlap_area: float,
    states: BoundaryIndex,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for index in _query_indices(states, geometry, predicate="intersects"):
        feature = states.features[index]
        overlap_area = _projected_area_km2(
            projected_geometry.intersection(feature.projected_geometry)
        )
        if overlap_area <= 0 and not geometry.touches(feature.geometry):
            continue
        matches.append(
            {
                "source_record_id": row["source_record_id"],
                "state_fips": feature.properties["state_fips"],
                "state_code": feature.properties["state_code"],
                "state_name": feature.name,
                "overlap_area_km2": overlap_area,
                "event_area_fraction": _fraction(overlap_area, event_area),
                "us_overlap_fraction": _fraction(overlap_area, us_overlap_area),
                "assignment_rank": 0,
                "is_primary_state": False,
            }
        )
    matches.sort(key=lambda item: (-float(item["overlap_area_km2"]), str(item["state_code"])))
    for rank, match in enumerate(matches, start=1):
        match["assignment_rank"] = rank
        match["is_primary_state"] = rank == 1
    return matches


def _query_indices(
    index: BoundaryIndex,
    geometry: BaseGeometry,
    predicate: str,
) -> list[int]:
    return [int(value) for value in index.tree.query(geometry, predicate=predicate)]


def _project_geometry(geometry: BaseGeometry) -> BaseGeometry:
    return transform(AREA_TRANSFORMER.transform, geometry)


def _projected_area_km2(geometry: BaseGeometry) -> float:
    if geometry.is_empty:
        return 0.0
    return float(geometry.area) / 1_000_000.0


def _fraction(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _fraction_valid(value: float | None) -> bool:
    if value is None:
        return True
    return -AREA_TOLERANCE <= value <= 1.0 + AREA_TOLERANCE


def _load_countries(path: Path) -> BoundaryIndex:
    rows = pq.read_table(path).to_pylist()
    features: list[BoundaryFeature] = []
    for row in rows:
        geometry = _geometry(row["geometry"])
        features.append(
            BoundaryFeature(
                code=str(row["country_code_alpha3"]),
                name=str(row["country_name"]),
                geometry=geometry,
                projected_geometry=_project_geometry(geometry),
                properties=row,
            )
        )
    _validate_unique(features, "country_code_alpha3", "country")
    version = str(rows[0].get("source_version", "")) if rows else ""
    us_features = [
        feature
        for feature in features
        if feature.properties["country_code_alpha2"] == "US"
        or feature.properties["country_code_alpha3"] == "USA"
    ]
    return BoundaryIndex(
        features,
        STRtree([feature.geometry for feature in features]),
        version,
        union=shapely.union_all([feature.geometry for feature in us_features])
        if us_features
        else None,
        projected_union=shapely.union_all([feature.projected_geometry for feature in us_features])
        if us_features
        else None,
    )


def _load_states(path: Path) -> BoundaryIndex:
    rows = pq.read_table(path).to_pylist()
    features: list[BoundaryFeature] = []
    for row in rows:
        geometry = _geometry(row["geometry"])
        features.append(
            BoundaryFeature(
                code=str(row["state_code"]),
                name=str(row["state_name"]),
                geometry=geometry,
                projected_geometry=_project_geometry(geometry),
                properties=row,
            )
        )
    _validate_unique(features, "state_code", "state")
    version = str(rows[0].get("source_version", "")) if rows else ""
    return BoundaryIndex(
        features,
        STRtree([feature.geometry for feature in features]),
        version,
        union=shapely.union_all([feature.geometry for feature in features]) if features else None,
        projected_union=shapely.union_all([feature.projected_geometry for feature in features])
        if features
        else None,
    )


def _geometry(value: object) -> BaseGeometry:
    if isinstance(value, bytes):
        geometry = shapely.from_wkb(value, on_invalid="ignore")
        if isinstance(geometry, BaseGeometry):
            return geometry
    raise SpatialEnrichmentError("Prepared boundary contains invalid WKB geometry.")


def _validate_unique(features: list[BoundaryFeature], field: str, label: str) -> None:
    seen: set[str] = set()
    for feature in features:
        value = str(feature.properties[field])
        if value in seen:
            raise SpatialEnrichmentError(f"Duplicate {label} boundary code: {value}")
        seen.add(value)


def _merge_enriched(accumulator: SpatialAccumulator, row: dict[str, Any]) -> None:
    if row["primary_country_code"] is None:
        accumulator.country_unassigned += 1
        accumulator.offshore_unassigned += 1
    else:
        accumulator.country_assigned += 1
    if int(row["country_candidate_count"]) > 1:
        accumulator.cross_country += 1
    if bool(row["intersects_united_states"]):
        accumulator.us_intersecting += 1
    if row["primary_state_code"] is not None:
        accumulator.state_assigned += 1
    if bool(row["multi_state_event"]):
        accumulator.multistate += 1
    if bool(row["spatial_review_required"]):
        accumulator.spatial_review += 1
    if row["country_assignment_method"] == "manual_review":
        accumulator.manual_review += 1


def _summary_payload(accumulator: SpatialAccumulator) -> dict[str, Any]:
    return {
        "row_accounting": {
            "source_rows": accumulator.source_rows,
            "enriched_rows": accumulator.enriched_rows,
            "rejected_rows": accumulator.rejected_rows,
            "balanced": accumulator.balanced,
        },
        "country_assignment": {
            "assigned": accumulator.country_assigned,
            "unassigned": accumulator.country_unassigned,
            "cross_country": accumulator.cross_country,
            "manual_review": accumulator.manual_review,
        },
        "state_assignment": {
            "us_intersecting": accumulator.us_intersecting,
            "assigned": accumulator.state_assigned,
            "multistate": accumulator.multistate,
        },
        "spatial_review_required": accumulator.spatial_review,
    }


def _boundary_manifest_entry(path: Path, index: BoundaryIndex) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _sha256(path),
        "feature_count": len(index.features),
        "source_version": index.version,
    }


def _log_progress(batch_number: int, accumulator: SpatialAccumulator, started_at: float) -> None:
    elapsed = time.perf_counter() - started_at
    rows_per_second = accumulator.source_rows / elapsed if elapsed > 0 else 0.0
    LOGGER.info(
        "spatial_enrichment_batch_processed",
        extra={
            "batch_number": batch_number,
            "source_rows_processed": accumulator.source_rows,
            "country_assigned": accumulator.country_assigned,
            "country_unassigned": accumulator.country_unassigned,
            "us_intersecting": accumulator.us_intersecting,
            "state_assigned": accumulator.state_assigned,
            "spatial_review": accumulator.spatial_review,
            "rejected": accumulator.rejected_rows,
            "elapsed_seconds": round(elapsed, 6),
            "rows_per_second": round(rows_per_second, 3),
        },
    )


class _SpatialWriters:
    def __init__(self, artifacts: dict[str, Path], source_geo: dict[str, Any] | None) -> None:
        self._events = _DatasetWriter(
            artifacts["events_enriched"] / "part-00000.parquet",
            _events_schema(source_geo),
        )
        self._countries = _DatasetWriter(
            artifacts["event_country_membership"] / "part-00000.parquet",
            _country_membership_schema(),
        )
        self._states = _DatasetWriter(
            artifacts["event_state_overlaps"] / "part-00000.parquet",
            _state_overlap_schema(),
        )
        self._us = _DatasetWriter(
            artifacts["us_events"] / "part-00000.parquet",
            _events_schema(source_geo),
        )

    def write(
        self,
        events: list[dict[str, Any]],
        countries: list[dict[str, Any]],
        states: list[dict[str, Any]],
        us_events: list[dict[str, Any]],
    ) -> None:
        self._events.write(events)
        self._countries.write(countries)
        self._states.write(states)
        self._us.write(us_events)

    def close(self) -> None:
        self._events.close()
        self._countries.close()
        self._states.close()
        self._us.close()


class _DatasetWriter:
    def __init__(self, path: Path, schema: pa.Schema) -> None:
        self.path = path
        self.schema = schema
        self.writer: pq.ParquetWriter | None = None

    def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(self.path, self.schema)
        self.writer.write_table(pa.Table.from_pylist(rows, schema=self.schema))

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist([], schema=self.schema), self.path)


def _events_schema(source_geo: dict[str, Any] | None) -> pa.Schema:
    metadata = {b"geo": json.dumps(source_geo or _default_geo_metadata(), sort_keys=True).encode()}
    return pa.schema(
        [
            pa.field("source_record_id", pa.string()),
            pa.field("source_row_index", pa.int64()),
            pa.field("source_name", pa.string()),
            pa.field("event_start_date", pa.date32()),
            pa.field("event_end_date", pa.date32()),
            pa.field("geometry", pa.binary()),
            pa.field("geometry_encoding", pa.string()),
            pa.field("source_crs", pa.string()),
            pa.field("reported_area_km2", pa.float64()),
            pa.field("geometry_type", pa.string()),
            pa.field("geometry_is_valid", pa.bool_()),
            pa.field("geometry_is_empty", pa.bool_()),
            pa.field("representative_longitude", pa.float64()),
            pa.field("representative_latitude", pa.float64()),
            pa.field("bounds_minx", pa.float64()),
            pa.field("bounds_miny", pa.float64()),
            pa.field("bounds_maxx", pa.float64()),
            pa.field("bounds_maxy", pa.float64()),
            pa.field("primary_country_code", pa.string()),
            pa.field("primary_country_name", pa.string()),
            pa.field("country_assignment_method", pa.string()),
            pa.field("country_candidate_count", pa.int64()),
            pa.field("country_assignment_ambiguous", pa.bool_()),
            pa.field("intersects_united_states", pa.bool_()),
            pa.field("intersects_us_country_boundary", pa.bool_()),
            pa.field("intersects_us_state_union", pa.bool_()),
            pa.field("us_overlap_area_km2", pa.float64()),
            pa.field("us_country_boundary_overlap_area_km2", pa.float64()),
            pa.field("us_event_area_fraction", pa.float64()),
            pa.field("primary_state_code", pa.string()),
            pa.field("primary_state_name", pa.string()),
            pa.field("state_assignment_method", pa.string()),
            pa.field("state_candidate_count", pa.int64()),
            pa.field("multi_state_event", pa.bool_()),
            pa.field("spatial_review_required", pa.bool_()),
            pa.field("spatial_review_reason", pa.string()),
            pa.field("country_boundary_version", pa.string()),
            pa.field("state_boundary_version", pa.string()),
            pa.field("event_area_km2", pa.float64()),
            pa.field("provenance", pa.string()),
            pa.field("antimeridian_review", pa.bool_()),
        ],
        metadata=metadata,
    )


def _country_membership_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("source_record_id", pa.string()),
            pa.field("country_code", pa.string()),
            pa.field("country_name", pa.string()),
            pa.field("representative_point_match", pa.bool_()),
            pa.field("geometry_intersects", pa.bool_()),
            pa.field("overlap_area_km2", pa.float64()),
            pa.field("event_area_fraction", pa.float64()),
            pa.field("assignment_rank", pa.int64()),
        ]
    )


def _state_overlap_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("source_record_id", pa.string()),
            pa.field("state_fips", pa.string()),
            pa.field("state_code", pa.string()),
            pa.field("state_name", pa.string()),
            pa.field("overlap_area_km2", pa.float64()),
            pa.field("event_area_fraction", pa.float64()),
            pa.field("us_overlap_fraction", pa.float64()),
            pa.field("assignment_rank", pa.int64()),
            pa.field("is_primary_state", pa.bool_()),
        ]
    )


def _default_geo_metadata() -> dict[str, Any]:
    return {
        "version": "0.4.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "crs": "EPSG:4326"}},
    }


def _write_assignment_quality(path: Path, payload: dict[str, Any]) -> None:
    rows = [{"metric": key, "value": value} for key, value in sorted(payload.items())]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["metric", "value"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
