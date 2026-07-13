from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, SupportsFloat, cast

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pydantic import BaseModel, ConfigDict
from shapely.geometry.base import BaseGeometry

from geodemand import __version__

LOGGER = logging.getLogger(__name__)

REQUIRED_SOURCE_FIELDS: frozenset[str] = frozenset({"uuid", "geometry", "start_date"})
OPTIONAL_SOURCE_FIELDS: frozenset[str] = frozenset({"area_km2", "end_date"})
IGNORED_SOURCE_FIELDS: frozenset[str] = frozenset({"__index_level_0__"})
SUPPORTED_GEOMETRY_ENCODINGS: frozenset[str] = frozenset({"WKB"})
SUPPORTED_GEOMETRY_TYPES: frozenset[str] = frozenset({"Polygon", "MultiPolygon"})
MAX_SAMPLE_TEXT_LENGTH = 120
TRUNCATED_SAMPLE_TEXT_LENGTH = 117
ANTIMERIDIAN_MIN_REVIEW_LONGITUDE = -179.0
ANTIMERIDIAN_MAX_REVIEW_LONGITUDE = 179.0
ANTIMERIDIAN_WIDTH_REVIEW_DEGREES = 180.0
DuplicatePolicy = Literal["quarantine", "keep-first", "keep-last", "reject-all"]


class GroundsourceError(ValueError):
    """Raised when Groundsource input cannot be processed."""


class GroundsourceFieldMapping(BaseModel):
    """Compatibility shell for older callers.

    Groundsource is now geometry-based. Source field names are fixed by the
    confirmed GeoParquet schema instead of mapped from country/latitude/longitude
    columns.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    @classmethod
    def from_json_file(cls, path: Path | None) -> GroundsourceFieldMapping:
        if path is None:
            return cls()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload:
            raise GroundsourceError(
                "Groundsource no longer accepts country/latitude/longitude mappings. "
                "The confirmed source schema uses uuid, geometry, start_date, "
                "end_date, and area_km2."
            )
        return cls()


@dataclass(frozen=True)
class GeoParquetMetadata:
    version: str | None
    primary_column: str | None
    encoding: str | None
    crs: Any
    geometry_types: list[str]
    bbox: list[float] | None
    raw: dict[str, Any] | None

    @property
    def source_crs(self) -> str | None:
        if self.crs is None:
            return None
        if isinstance(self.crs, str):
            return self.crs
        if isinstance(self.crs, dict):
            crs_id = self.crs.get("id")
            if isinstance(crs_id, dict):
                authority = crs_id.get("authority")
                code = crs_id.get("code")
                if authority and code:
                    return f"{authority}:{code}"
            name = self.crs.get("name")
            if isinstance(name, str):
                return name
        return str(self.crs)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "primary_column": self.primary_column,
            "encoding": self.encoding,
            "crs": self.crs,
            "source_crs": self.source_crs,
            "geometry_types": self.geometry_types,
            "bbox": self.bbox,
            "detected": self.raw is not None,
        }


@dataclass(frozen=True)
class GroundsourceInspection:
    input_path: Path
    rows: int
    columns: list[str]
    schema: list[dict[str, Any]]
    parquet_key_value_metadata: dict[str, Any]
    geoparquet: GeoParquetMetadata
    required_fields: list[str]
    missing_required_fields: list[str]
    optional_fields: list[str]
    ignored_fields: list[str]
    unknown_fields: list[str]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_path": str(self.input_path),
            "rows": self.rows,
            "columns": self.columns,
            "schema": self.schema,
            "parquet_key_value_metadata": self.parquet_key_value_metadata,
            "geoparquet": self.geoparquet.to_dict(),
            "required_fields": self.required_fields,
            "missing_required_fields": self.missing_required_fields,
            "optional_fields": self.optional_fields,
            "ignored_fields": self.ignored_fields,
            "unknown_fields": self.unknown_fields,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class FilterResult:
    output_path: Path
    input_rows: int
    valid_rows: int
    output_rows: int
    rejected_rows: int
    quarantined_rows: int = 0
    quarantine_path: Path | None = None


@dataclass
class BatchValidationResult:
    rows: list[dict[str, Any]]
    rejected_rows: int
    null_uuid_count: int = 0
    malformed_uuid_count: int = 0
    null_geometry_count: int = 0
    undecodable_geometry_count: int = 0
    empty_geometry_count: int = 0
    invalid_geometry_count: int = 0
    unsupported_geometry_count: int = 0
    malformed_start_date_count: int = 0
    null_start_date_count: int = 0
    null_end_date_count: int = 0
    null_area_count: int = 0
    antimeridian_review_count: int = 0
    geometry_type_counts: Counter[str] = field(default_factory=Counter)
    years: Counter[int] = field(default_factory=Counter)
    durations: Counter[int] = field(default_factory=Counter)


@dataclass(frozen=True)
class AuditBatch:
    row_group_index: int
    batch_number: int
    row_offset: int
    batch: pa.RecordBatch


@dataclass
class AuditAccumulator:
    total_rows: int = 0
    valid_rows: int = 0
    rejected_rows: int = 0
    quarantined_rows: int = 0
    null_uuid_count: int = 0
    malformed_uuid_count: int = 0
    duplicate_uuid_count: int = 0
    duplicate_uuid_group_count: int = 0
    duplicate_uuid_participating_row_count: int = 0
    exact_duplicate_record_count: int = 0
    conflicting_duplicate_record_count: int = 0
    duplicate_geometry_conflict_count: int = 0
    duplicate_date_conflict_count: int = 0
    duplicate_area_conflict_count: int = 0
    null_geometry_count: int = 0
    undecodable_geometry_count: int = 0
    empty_geometry_count: int = 0
    invalid_geometry_count: int = 0
    unsupported_geometry_count: int = 0
    malformed_start_date_count: int = 0
    null_start_date_count: int = 0
    null_end_date_count: int = 0
    null_area_count: int = 0
    antimeridian_review_count: int = 0
    geometry_type_counts: Counter[str] = field(default_factory=Counter)
    years: Counter[int] = field(default_factory=Counter)
    durations: Counter[int] = field(default_factory=Counter)
    uuid_counts: Counter[str] = field(default_factory=Counter)
    min_start_date: date | None = None
    max_start_date: date | None = None
    min_end_date: date | None = None
    max_end_date: date | None = None
    min_area_km2: float | None = None
    max_area_km2: float | None = None
    area_sum: float = 0.0
    area_count: int = 0
    minx: float | None = None
    miny: float | None = None
    maxx: float | None = None
    maxy: float | None = None
    processing_seconds: float = 0.0
    rows_per_second: float = 0.0

    def to_rejected_summary(self, inspection: GroundsourceInspection) -> dict[str, Any]:
        return {
            "total_records": self.total_rows,
            "valid_records": self.valid_rows,
            "rejected_records": self.rejected_rows,
            "quarantined_records": self.quarantined_rows,
            "rejection_reasons": {
                "null_uuid": self.null_uuid_count,
                "malformed_uuid": self.malformed_uuid_count,
                "duplicate_uuid": self.duplicate_uuid_count,
                "null_geometry": self.null_geometry_count,
                "undecodable_geometry": self.undecodable_geometry_count,
                "empty_geometry": self.empty_geometry_count,
                "unsupported_geometry": self.unsupported_geometry_count,
                "null_start_date": self.null_start_date_count,
                "malformed_start_date": self.malformed_start_date_count,
            },
            "duplicate_uuid": {
                "groups": self.duplicate_uuid_group_count,
                "participating_rows": self.duplicate_uuid_participating_row_count,
                "exact_duplicate_records": self.exact_duplicate_record_count,
                "conflicting_duplicate_records": self.conflicting_duplicate_record_count,
                "geometry_conflicts": self.duplicate_geometry_conflict_count,
                "date_conflicts": self.duplicate_date_conflict_count,
                "reported_area_conflicts": self.duplicate_area_conflict_count,
            },
            "reported_not_rejected": {
                "null_end_date": self.null_end_date_count,
                "null_area_km2": self.null_area_count,
                "country_unassigned": self.valid_rows,
                "state_unassigned": self.valid_rows,
            },
            "ignored_source_columns": inspection.ignored_fields,
        }


@dataclass
class DuplicateStats:
    count: int = 0
    source_row_indexes: list[int] = field(default_factory=list)
    signatures: Counter[str] = field(default_factory=Counter)
    geometry_signatures: set[str] = field(default_factory=set)
    date_signatures: set[str] = field(default_factory=set)
    area_signatures: set[str] = field(default_factory=set)

    @property
    def is_duplicate(self) -> bool:
        return self.count > 1

    @property
    def first_row_index(self) -> int | None:
        return min(self.source_row_indexes) if self.source_row_indexes else None

    @property
    def last_row_index(self) -> int | None:
        return max(self.source_row_indexes) if self.source_row_indexes else None

    @property
    def exact_duplicate_records(self) -> int:
        return sum(count - 1 for count in self.signatures.values() if count > 1)

    @property
    def has_conflicts(self) -> bool:
        return (
            len(self.signatures) > 1
            or self.has_geometry_conflict
            or self.has_date_conflict
            or self.has_area_conflict
        )

    @property
    def has_geometry_conflict(self) -> bool:
        return len(self.geometry_signatures) > 1

    @property
    def has_date_conflict(self) -> bool:
        return len(self.date_signatures) > 1

    @property
    def has_area_conflict(self) -> bool:
        return len(self.area_signatures) > 1


def inspect_groundsource(
    input_path: Path,
    mapping: GroundsourceFieldMapping | None = None,
) -> GroundsourceInspection:
    _ = mapping
    path = _validate_input_path(input_path)
    parquet_file = pq.ParquetFile(path)
    schema = parquet_file.schema_arrow
    columns = schema.names
    metadata = _key_value_metadata(parquet_file)
    geoparquet = _geoparquet_metadata(metadata)
    missing_required = sorted(REQUIRED_SOURCE_FIELDS.difference(columns))
    ignored = sorted(IGNORED_SOURCE_FIELDS.intersection(columns))
    unknown = sorted(
        set(columns).difference(
            REQUIRED_SOURCE_FIELDS | OPTIONAL_SOURCE_FIELDS | IGNORED_SOURCE_FIELDS
        )
    )
    warnings = [f"Unknown source field reported but not used: {column}" for column in unknown]
    if missing_required:
        warnings.append("Missing required Groundsource fields: " + ", ".join(missing_required))

    inspection = GroundsourceInspection(
        input_path=path,
        rows=parquet_file.metadata.num_rows,
        columns=columns,
        schema=_arrow_schema_entries(path, schema),
        parquet_key_value_metadata=metadata,
        geoparquet=geoparquet,
        required_fields=sorted(REQUIRED_SOURCE_FIELDS),
        missing_required_fields=missing_required,
        optional_fields=sorted(OPTIONAL_SOURCE_FIELDS),
        ignored_fields=ignored,
        unknown_fields=unknown,
        warnings=warnings,
    )
    LOGGER.info(
        "groundsource_inspected",
        extra={"input_path": str(path), "rows": inspection.rows},
    )
    return inspection


def validate_groundsource(
    input_path: Path,
    mapping: GroundsourceFieldMapping | None = None,
    duplicate_policy: DuplicatePolicy = "quarantine",
    preserve_invalid_geometries: bool = False,
    batch_size: int = 10_000,
    max_rows: int | None = None,
    max_row_groups: int | None = None,
) -> dict[str, Any]:
    _ = mapping
    path = _validate_input_path(input_path)
    inspection = inspect_groundsource(path)
    _raise_for_missing_required(inspection)
    summary = _audit_accumulator(
        path,
        inspection.geoparquet,
        duplicate_policy=duplicate_policy,
        preserve_invalid_geometries=preserve_invalid_geometries,
        batch_size=batch_size,
        max_rows=max_rows,
        max_row_groups=max_row_groups,
    ).to_rejected_summary(inspection)
    if int(summary["rejected_records"]) > 0:
        raise GroundsourceError(
            "Groundsource validation failed with "
            f"{summary['rejected_records']} rejected records. "
            "Run audit-groundsource for rejected_records_summary.json."
        )
    return summary


def filter_groundsource(
    input_path: Path,
    output_path: Path,
    country: str,
    start_date: date | None = None,
    end_date: date | None = None,
    mapping: GroundsourceFieldMapping | None = None,
    preserve_raw_fields: bool = False,
    country_boundaries: Path | None = None,
) -> FilterResult:
    _ = (output_path, start_date, end_date, mapping, preserve_raw_fields)
    normalized_country = _normalize_country(country)
    _validate_input_path(input_path)
    if country_boundaries is None:
        raise GroundsourceError(
            f"Cannot filter Groundsource to country {normalized_country} from raw source fields. "
            "The confirmed Groundsource schema has geometry but no country column; provide "
            "--country-boundaries PATH for a future spatial enrichment workflow."
        )
    raise GroundsourceError(
        "Country-boundary spatial enrichment is not implemented in this milestone. "
        f"Received boundary dataset: {country_boundaries}"
    )


def profile_groundsource(
    input_path: Path,
    output_path: Path,
    mapping: GroundsourceFieldMapping | None = None,
    duplicate_policy: DuplicatePolicy = "quarantine",
    preserve_invalid_geometries: bool = False,
    batch_size: int = 10_000,
    max_rows: int | None = None,
    max_row_groups: int | None = None,
) -> dict[str, Any]:
    _ = mapping
    path = _validate_input_path(input_path)
    inspection = inspect_groundsource(path)
    _raise_for_missing_required(inspection)
    profile = _profile_payload(
        path,
        inspection,
        duplicate_policy=duplicate_policy,
        preserve_invalid_geometries=preserve_invalid_geometries,
        batch_size=batch_size,
        max_rows=max_rows,
        max_row_groups=max_row_groups,
    )
    _write_json(output_path, profile)
    return profile


def audit_groundsource(
    input_path: Path,
    output_dir: Path,
    mapping: GroundsourceFieldMapping | None = None,
    preserve_raw_fields: bool = False,
    duplicate_policy: DuplicatePolicy = "quarantine",
    preserve_invalid_geometries: bool = False,
    batch_size: int = 10_000,
    max_rows: int | None = None,
    max_row_groups: int | None = None,
) -> dict[str, Path]:
    _ = (mapping, preserve_raw_fields)
    path = _validate_input_path(input_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    inspection = inspect_groundsource(path)
    _raise_for_missing_required(inspection)
    quarantine_path = output_dir / "quarantine.parquet"
    accumulator = _audit_accumulator(
        path,
        inspection.geoparquet,
        duplicate_policy=duplicate_policy,
        preserve_invalid_geometries=preserve_invalid_geometries,
        batch_size=batch_size,
        max_rows=max_rows,
        max_row_groups=max_row_groups,
        quarantine_path=quarantine_path,
    )

    artifacts = {
        "schema": output_dir / "schema.json",
        "profile": output_dir / "profile.json",
        "field_quality": output_dir / "field_quality.csv",
        "temporal_coverage": output_dir / "temporal_coverage.csv",
        "geometry_quality": output_dir / "geometry_quality.csv",
        "rejected_records_summary": output_dir / "rejected_records_summary.json",
        "quarantine": quarantine_path,
        "manifest": output_dir / "manifest.json",
    }
    _write_json(artifacts["schema"], _schema_payload(inspection))
    _write_json(
        artifacts["profile"],
        _profile_payload_from_accumulator(path, inspection, accumulator),
    )
    _write_field_quality_csv(artifacts["field_quality"], inspection, accumulator)
    _write_temporal_coverage_csv(artifacts["temporal_coverage"], accumulator)
    _write_geometry_quality_csv(artifacts["geometry_quality"], accumulator)
    _write_json(artifacts["rejected_records_summary"], accumulator.to_rejected_summary(inspection))
    _write_json(
        artifacts["manifest"],
        _manifest(
            path,
            inspection,
            accumulator,
            duplicate_policy=duplicate_policy,
            preserve_invalid_geometries=preserve_invalid_geometries,
            batch_size=batch_size,
            max_rows=max_rows,
            max_row_groups=max_row_groups,
        ),
    )
    LOGGER.info(
        "groundsource_audited",
        extra={
            "input_path": str(path),
            "output_dir": str(output_dir),
            "input_rows": accumulator.total_rows,
            "valid_rows": accumulator.valid_rows,
        },
    )
    return artifacts


def write_canonical_groundsource(
    input_path: Path,
    output_path: Path,
    batch_size: int = 10_000,
    duplicate_policy: DuplicatePolicy = "quarantine",
    preserve_invalid_geometries: bool = False,
    quarantine_path: Path | None = None,
    max_rows: int | None = None,
    max_row_groups: int | None = None,
) -> FilterResult:
    path = _validate_input_path(input_path)
    inspection = inspect_groundsource(path)
    _raise_for_missing_required(inspection)
    duplicate_index = _duplicate_index(
        path,
        inspection.geoparquet,
        batch_size=batch_size,
        max_rows=max_rows,
        max_row_groups=max_row_groups,
    )
    if quarantine_path is None:
        quarantine_path = output_path.with_name(f"{output_path.stem}_quarantine.parquet")
    output_schema = _canonical_arrow_schema(inspection.geoparquet)
    quarantine_schema = _quarantine_arrow_schema(inspection.geoparquet)
    writer: pq.ParquetWriter | None = None
    quarantine_writer: pq.ParquetWriter | None = None
    input_rows = 0
    accepted_rows = 0
    quarantined_rows = 0
    rejected_rows = 0
    try:
        for audit_batch in _iter_limited_batches(
            path,
            batch_size=batch_size,
            max_rows=max_rows,
            max_row_groups=max_row_groups,
        ):
            result = _process_batch(
                audit_batch.batch,
                inspection.geoparquet,
                row_offset=audit_batch.row_offset,
            )
            input_rows += audit_batch.batch.num_rows
            rejected_rows += result.rejected_rows
            accepted, quarantined = _terminal_rows(
                result.rows,
                duplicate_index,
                duplicate_policy=duplicate_policy,
                preserve_invalid_geometries=preserve_invalid_geometries,
            )
            rejected_rows += len(result.rows) - len(accepted) - len(quarantined)
            accepted_rows += len(accepted)
            quarantined_rows += len(quarantined)
            if accepted:
                table = pa.Table.from_pylist(accepted, schema=output_schema)
                if writer is None:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(output_path, output_schema)
                writer.write_table(table)
            if quarantined:
                table = pa.Table.from_pylist(quarantined, schema=quarantine_schema)
                if quarantine_writer is None:
                    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
                    quarantine_writer = pq.ParquetWriter(quarantine_path, quarantine_schema)
                quarantine_writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
        if quarantine_writer is not None:
            quarantine_writer.close()
    return FilterResult(
        output_path=output_path,
        input_rows=input_rows,
        valid_rows=accepted_rows,
        output_rows=accepted_rows,
        rejected_rows=rejected_rows,
        quarantined_rows=quarantined_rows,
        quarantine_path=quarantine_path,
    )


def _validate_input_path(input_path: Path) -> Path:
    path = input_path.expanduser().resolve()
    if not path.exists():
        raise GroundsourceError(f"Groundsource input path does not exist: {path}")
    if path.is_dir():
        raise GroundsourceError(
            f"Groundsource input path must be a Parquet file, got directory: {path}"
        )
    return path


def _raise_for_missing_required(inspection: GroundsourceInspection) -> None:
    if inspection.missing_required_fields:
        raise GroundsourceError(
            "Groundsource input is missing required fields: "
            + ", ".join(inspection.missing_required_fields)
        )
    if inspection.geoparquet.encoding not in SUPPORTED_GEOMETRY_ENCODINGS:
        raise GroundsourceError(
            "Groundsource geometry encoding must be WKB. "
            f"Detected: {inspection.geoparquet.encoding or 'unknown'}"
        )


def _key_value_metadata(parquet_file: pq.ParquetFile) -> dict[str, Any]:
    raw_metadata = parquet_file.metadata.metadata or {}
    decoded: dict[str, Any] = {}
    for key, value in raw_metadata.items():
        key_text = key.decode("utf-8", errors="replace")
        value_text = value.decode("utf-8", errors="replace")
        if key_text == "geo":
            try:
                decoded[key_text] = json.loads(value_text)
            except json.JSONDecodeError:
                decoded[key_text] = value_text
        else:
            decoded[key_text] = value_text
    return decoded


def _geoparquet_metadata(metadata: dict[str, Any]) -> GeoParquetMetadata:
    raw_geo = metadata.get("geo")
    if not isinstance(raw_geo, dict):
        return GeoParquetMetadata(
            version=None,
            primary_column=None,
            encoding=None,
            crs=None,
            geometry_types=[],
            bbox=None,
            raw=None,
        )
    primary = raw_geo.get("primary_column")
    columns = raw_geo.get("columns", {})
    column_metadata = columns.get(primary, {}) if isinstance(columns, dict) else {}
    return GeoParquetMetadata(
        version=_optional_str(raw_geo.get("version")),
        primary_column=_optional_str(primary),
        encoding=_optional_str(column_metadata.get("encoding")),
        crs=column_metadata.get("crs"),
        geometry_types=_geometry_types_from_metadata(column_metadata),
        bbox=_optional_float_list(column_metadata.get("bbox")),
        raw=raw_geo,
    )


def _geometry_types_from_metadata(column_metadata: dict[str, Any]) -> list[str]:
    plural = column_metadata.get("geometry_types")
    if isinstance(plural, list):
        return [str(value) for value in plural]
    singular = column_metadata.get("geometry_type")
    if isinstance(singular, list):
        return [str(value) for value in singular]
    if singular is not None:
        return [str(singular)]
    return []


def _arrow_schema_entries(path: Path, schema: pa.Schema) -> list[dict[str, Any]]:
    samples = _sample_values(path, schema.names)
    return [
        {
            "name": field.name,
            "arrow_type": str(field.type),
            "nullable": field.nullable,
            "metadata": _decode_field_metadata(field.metadata),
            "sample_values": samples.get(field.name, []),
        }
        for field in schema
    ]


def _sample_values(path: Path, columns: list[str], limit: int = 3) -> dict[str, list[str]]:
    parquet_file = pq.ParquetFile(path)
    samples: dict[str, list[str]] = {column: [] for column in columns}
    for batch in parquet_file.iter_batches(batch_size=limit):
        data = batch.to_pydict()
        for column in columns:
            for value in data.get(column, []):
                if value is None:
                    continue
                samples[column].append(_safe_sample_value(column, value))
                if len(samples[column]) >= limit:
                    break
        break
    return samples


def _safe_sample_value(column: str, value: object) -> str:
    if column == "geometry" and isinstance(value, bytes):
        return f"<WKB {len(value)} bytes>"
    text = str(value)
    if len(text) > MAX_SAMPLE_TEXT_LENGTH:
        return text[:TRUNCATED_SAMPLE_TEXT_LENGTH] + "..."
    return text


def _decode_field_metadata(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
    if metadata is None:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def _schema_payload(inspection: GroundsourceInspection) -> dict[str, Any]:
    return {
        "source_file": inspection.input_path.name,
        "columns": inspection.schema,
        "parquet_key_value_metadata": inspection.parquet_key_value_metadata,
        "geoparquet": inspection.geoparquet.to_dict(),
        "ignored_fields": inspection.ignored_fields,
        "unknown_fields": inspection.unknown_fields,
        "warnings": inspection.warnings,
    }


def _audit_accumulator(
    path: Path,
    geoparquet: GeoParquetMetadata,
    duplicate_policy: DuplicatePolicy = "quarantine",
    preserve_invalid_geometries: bool = False,
    batch_size: int = 10_000,
    max_rows: int | None = None,
    max_row_groups: int | None = None,
    quarantine_path: Path | None = None,
) -> AuditAccumulator:
    started_at = time.perf_counter()
    accumulator = AuditAccumulator()
    duplicate_index = _duplicate_index(
        path,
        geoparquet,
        batch_size=batch_size,
        max_rows=max_rows,
        max_row_groups=max_row_groups,
    )
    _merge_duplicate_stats(accumulator, duplicate_index)
    quarantine_writer: pq.ParquetWriter | None = None
    quarantine_schema = _quarantine_arrow_schema(geoparquet)
    try:
        for audit_batch in _iter_limited_batches(
            path,
            batch_size=batch_size,
            max_rows=max_rows,
            max_row_groups=max_row_groups,
        ):
            result = _process_batch(
                audit_batch.batch,
                geoparquet,
                row_offset=audit_batch.row_offset,
            )
            accepted, quarantined = _terminal_rows(
                result.rows,
                duplicate_index,
                duplicate_policy=duplicate_policy,
                preserve_invalid_geometries=preserve_invalid_geometries,
            )
            _merge_batch_result(accumulator, result, audit_batch.batch, accepted, quarantined)
            _log_audit_progress(audit_batch, accumulator, started_at)
            if quarantine_path is not None and quarantined:
                table = pa.Table.from_pylist(quarantined, schema=quarantine_schema)
                if quarantine_writer is None:
                    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
                    quarantine_writer = pq.ParquetWriter(quarantine_path, quarantine_schema)
                quarantine_writer.write_table(table)
    finally:
        if quarantine_writer is not None:
            quarantine_writer.close()
    accumulator.processing_seconds = time.perf_counter() - started_at
    accumulator.rows_per_second = (
        accumulator.total_rows / accumulator.processing_seconds
        if accumulator.processing_seconds > 0
        else 0.0
    )
    return accumulator


def _process_batch(  # noqa: PLR0912, PLR0915
    batch: pa.RecordBatch,
    geoparquet: GeoParquetMetadata,
    row_offset: int,
) -> BatchValidationResult:
    data = batch.to_pydict()
    result = BatchValidationResult(rows=[], rejected_rows=0)
    geometries = _decode_wkb_values(data["geometry"])

    for index in range(batch.num_rows):
        source_row_index = _source_row_index(data, index, row_offset)
        source_id = _parse_uuid(data["uuid"][index])
        start_date = _parse_date(data["start_date"][index])
        end_date = _parse_date(data.get("end_date", [None] * batch.num_rows)[index])
        area = _optional_float(data.get("area_km2", [None] * batch.num_rows)[index])
        geometry_value = data["geometry"][index]
        geometry = geometries[index]
        rejected = False

        if source_id is None:
            rejected = True
            if data["uuid"][index] is None or str(data["uuid"][index]).strip() == "":
                result.null_uuid_count += 1
            else:
                result.malformed_uuid_count += 1
        if start_date is None:
            rejected = True
            if data["start_date"][index] is None or str(data["start_date"][index]).strip() == "":
                result.null_start_date_count += 1
            else:
                result.malformed_start_date_count += 1
        if data.get("end_date", [None] * batch.num_rows)[index] is None:
            result.null_end_date_count += 1
        if area is None:
            result.null_area_count += 1
        if geometry_value is None:
            rejected = True
            result.null_geometry_count += 1
        elif geometry is None:
            rejected = True
            result.undecodable_geometry_count += 1
        else:
            geometry_type = geometry.geom_type
            result.geometry_type_counts[geometry_type] += 1
            if geometry_type not in SUPPORTED_GEOMETRY_TYPES:
                rejected = True
                result.unsupported_geometry_count += 1
            if geometry.is_empty:
                rejected = True
                result.empty_geometry_count += 1
            if not geometry.is_valid:
                result.invalid_geometry_count += 1

        if rejected:
            result.rejected_rows += 1
            continue

        assert source_id is not None
        assert start_date is not None
        assert geometry is not None
        representative = geometry.representative_point()
        bounds = geometry.bounds
        duration_days = (end_date - start_date).days if end_date is not None else None
        antimeridian_review = _needs_antimeridian_review(bounds)
        if antimeridian_review:
            result.antimeridian_review_count += 1
        result.years[start_date.year] += 1
        if duration_days is not None:
            result.durations[duration_days] += 1
        result.rows.append(
            {
                "source_record_id": source_id,
                "source_row_index": source_row_index,
                "raw_start_date": data["start_date"][index],
                "raw_end_date": data.get("end_date", [None] * batch.num_rows)[index],
                "source_name": "Groundsource",
                "event_start_date": start_date,
                "event_end_date": end_date,
                "geometry": geometry_value,
                "geometry_encoding": geoparquet.encoding or "WKB",
                "source_crs": geoparquet.source_crs,
                "reported_area_km2": area,
                "geometry_type": geometry.geom_type,
                "geometry_is_valid": bool(geometry.is_valid),
                "geometry_is_empty": bool(geometry.is_empty),
                "representative_longitude": float(representative.x),
                "representative_latitude": float(representative.y),
                "bounds_minx": float(bounds[0]),
                "bounds_miny": float(bounds[1]),
                "bounds_maxx": float(bounds[2]),
                "bounds_maxy": float(bounds[3]),
                "country_code": None,
                "country_assignment_method": None,
                "state_code": None,
                "provenance": json.dumps(
                    {"source_file_schema": "groundsource-geoparquet-0.4.0"},
                    sort_keys=True,
                ),
                "antimeridian_review": antimeridian_review,
            }
        )
    return result


def _decode_wkb_values(values: list[object]) -> list[BaseGeometry | None]:
    geometries: list[BaseGeometry | None] = []
    for value in values:
        if value is None:
            geometries.append(None)
            continue
        try:
            geometry = shapely.from_wkb(value, on_invalid="ignore")
        except Exception:
            geometry = None
        geometries.append(cast_geometry(geometry))
    return geometries


def cast_geometry(value: object) -> BaseGeometry | None:
    if isinstance(value, BaseGeometry):
        return value
    return None


def _iter_limited_batches(
    path: Path,
    batch_size: int,
    max_rows: int | None,
    max_row_groups: int | None,
) -> Iterator[AuditBatch]:
    parquet_file = pq.ParquetFile(path)
    emitted_rows = 0
    row_group_indexes = range(parquet_file.num_row_groups)
    if max_row_groups is not None:
        row_group_indexes = range(min(max_row_groups, parquet_file.num_row_groups))
    batch_number = 0
    for row_group_index in row_group_indexes:
        for batch in parquet_file.iter_batches(
            batch_size=batch_size,
            row_groups=[row_group_index],
        ):
            if max_rows is not None and emitted_rows >= max_rows:
                return
            limited_batch = batch
            if max_rows is not None:
                remaining = max_rows - emitted_rows
                if batch.num_rows > remaining:
                    limited_batch = batch.slice(0, remaining)
            batch_number += 1
            yield AuditBatch(
                row_group_index=row_group_index,
                batch_number=batch_number,
                row_offset=emitted_rows,
                batch=limited_batch,
            )
            emitted_rows += limited_batch.num_rows


def _duplicate_index(
    path: Path,
    geoparquet: GeoParquetMetadata,
    batch_size: int,
    max_rows: int | None,
    max_row_groups: int | None,
) -> dict[str, DuplicateStats]:
    _ = geoparquet
    index: dict[str, DuplicateStats] = {}
    for audit_batch in _iter_limited_batches(
        path,
        batch_size=batch_size,
        max_rows=max_rows,
        max_row_groups=max_row_groups,
    ):
        data = audit_batch.batch.to_pydict()
        for index_in_batch in range(audit_batch.batch.num_rows):
            source_id = _parse_uuid(data["uuid"][index_in_batch])
            if source_id is None:
                continue
            source_row_index = _source_row_index(
                data,
                index_in_batch,
                audit_batch.row_offset,
            )
            geometry_signature = _raw_geometry_signature(data["geometry"][index_in_batch])
            date_signature = _raw_date_signature(
                data["start_date"][index_in_batch],
                data.get("end_date", [None] * audit_batch.batch.num_rows)[index_in_batch],
            )
            area_signature = str(
                data.get("area_km2", [None] * audit_batch.batch.num_rows)[index_in_batch]
            )
            stats = index.setdefault(source_id, DuplicateStats())
            stats.count += 1
            stats.source_row_indexes.append(source_row_index)
            stats.signatures[
                _raw_row_signature(geometry_signature, date_signature, area_signature)
            ] += 1
            stats.geometry_signatures.add(geometry_signature)
            stats.date_signatures.add(date_signature)
            stats.area_signatures.add(area_signature)
    return index


def _terminal_rows(
    rows: list[dict[str, Any]],
    duplicate_index: dict[str, DuplicateStats],
    duplicate_policy: DuplicatePolicy,
    preserve_invalid_geometries: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    for row in rows:
        reason = _quarantine_reason(
            row,
            duplicate_index,
            duplicate_policy=duplicate_policy,
            preserve_invalid_geometries=preserve_invalid_geometries,
        )
        if reason is None:
            accepted.append(_canonical_row(row))
        elif reason != "reject_duplicate_uuid":
            quarantined.append(_quarantine_row(row, duplicate_index, reason))
    return accepted, quarantined


def _quarantine_reason(  # noqa: PLR0911
    row: dict[str, Any],
    duplicate_index: dict[str, DuplicateStats],
    duplicate_policy: DuplicatePolicy,
    preserve_invalid_geometries: bool,
) -> str | None:
    if not bool(row["geometry_is_valid"]) and not preserve_invalid_geometries:
        return "invalid_geometry"
    source_id = str(row["source_record_id"])
    stats = duplicate_index.get(source_id)
    if stats is None or not stats.is_duplicate:
        return None
    row_index = int(row["source_row_index"])
    if duplicate_policy == "quarantine":
        return "duplicate_uuid"
    if duplicate_policy == "reject-all":
        return "reject_duplicate_uuid"
    if duplicate_policy == "keep-first" and row_index != stats.first_row_index:
        return "duplicate_uuid_keep_first"
    if duplicate_policy == "keep-last" and row_index != stats.last_row_index:
        return "duplicate_uuid_keep_last"
    return None


def _canonical_row(row: dict[str, Any]) -> dict[str, Any]:
    return {field.name: row.get(field.name) for field in _canonical_arrow_schema_fields()}


def _quarantine_row(
    row: dict[str, Any],
    duplicate_index: dict[str, DuplicateStats],
    reason: str,
) -> dict[str, Any]:
    stats = duplicate_index.get(str(row["source_record_id"]))
    conflict_parts = [reason]
    if stats is not None and stats.is_duplicate:
        if stats.has_geometry_conflict:
            conflict_parts.append("geometry_conflict")
        if stats.has_date_conflict:
            conflict_parts.append("date_conflict")
        if stats.has_area_conflict:
            conflict_parts.append("reported_area_conflict")
        if not stats.has_conflicts:
            conflict_parts.append("exact_duplicate")
    return {
        **_canonical_row(row),
        "source_uuid": row["source_record_id"],
        "source_row_index": row["source_row_index"],
        "conflict_reason": ";".join(conflict_parts),
    }


def _source_row_index(data: dict[str, list[Any]], index: int, row_offset: int) -> int:
    source_indexes = data.get("__index_level_0__")
    if source_indexes is not None and source_indexes[index] is not None:
        return int(source_indexes[index])
    return row_offset + index


def _row_signature(row: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "geometry": _geometry_signature(row),
                "start": _date_string(row["event_start_date"]),
                "end": _date_string(row["event_end_date"]),
                "area": row["reported_area_km2"],
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _geometry_signature(row: dict[str, Any]) -> str:
    geometry = row["geometry"]
    if isinstance(geometry, bytes):
        return hashlib.sha256(geometry).hexdigest()
    return str(geometry)


def _date_signature(row: dict[str, Any]) -> str:
    return json.dumps(
        {
            "start": _date_string(row["event_start_date"]),
            "end": _date_string(row["event_end_date"]),
        },
        sort_keys=True,
    )


def _area_signature(row: dict[str, Any]) -> str:
    return str(row["reported_area_km2"])


def _raw_geometry_signature(value: object) -> str:
    if isinstance(value, bytes):
        return hashlib.sha256(value).hexdigest()
    return str(value)


def _raw_date_signature(start_date: object, end_date: object) -> str:
    return json.dumps({"start": str(start_date), "end": str(end_date)}, sort_keys=True)


def _raw_row_signature(
    geometry_signature: str,
    date_signature: str,
    area_signature: str,
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "geometry": geometry_signature,
                "date": date_signature,
                "area": area_signature,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _log_audit_progress(
    audit_batch: AuditBatch,
    accumulator: AuditAccumulator,
    started_at: float,
) -> None:
    elapsed = time.perf_counter() - started_at
    rows_per_second = accumulator.total_rows / elapsed if elapsed > 0 else 0.0
    LOGGER.info(
        "groundsource_audit_batch_processed",
        extra={
            "row_group": audit_batch.row_group_index,
            "batch_number": audit_batch.batch_number,
            "rows_processed": accumulator.total_rows,
            "accepted": accumulator.valid_rows,
            "quarantined": accumulator.quarantined_rows,
            "rejected": accumulator.rejected_rows,
            "elapsed_seconds": round(elapsed, 6),
            "rows_per_second": round(rows_per_second, 3),
        },
    )


def _merge_duplicate_stats(
    accumulator: AuditAccumulator,
    duplicate_index: dict[str, DuplicateStats],
) -> None:
    duplicate_groups = [stats for stats in duplicate_index.values() if stats.is_duplicate]
    accumulator.duplicate_uuid_group_count = len(duplicate_groups)
    accumulator.duplicate_uuid_participating_row_count = sum(
        stats.count for stats in duplicate_groups
    )
    accumulator.duplicate_uuid_count = sum(stats.count - 1 for stats in duplicate_groups)
    accumulator.exact_duplicate_record_count = sum(
        stats.exact_duplicate_records for stats in duplicate_groups
    )
    accumulator.conflicting_duplicate_record_count = sum(
        stats.count for stats in duplicate_groups if stats.has_conflicts
    )
    accumulator.duplicate_geometry_conflict_count = sum(
        1 for stats in duplicate_groups if stats.has_geometry_conflict
    )
    accumulator.duplicate_date_conflict_count = sum(
        1 for stats in duplicate_groups if stats.has_date_conflict
    )
    accumulator.duplicate_area_conflict_count = sum(
        1 for stats in duplicate_groups if stats.has_area_conflict
    )


def _merge_batch_result(
    accumulator: AuditAccumulator,
    result: BatchValidationResult,
    batch: pa.RecordBatch,
    accepted: list[dict[str, Any]],
    quarantined: list[dict[str, Any]],
) -> None:
    accumulator.total_rows += batch.num_rows
    accumulator.valid_rows += len(accepted)
    accumulator.quarantined_rows += len(quarantined)
    duplicate_rejected_rows = len(result.rows) - len(accepted) - len(quarantined)
    accumulator.rejected_rows += result.rejected_rows + duplicate_rejected_rows
    accumulator.null_uuid_count += result.null_uuid_count
    accumulator.malformed_uuid_count += result.malformed_uuid_count
    accumulator.null_geometry_count += result.null_geometry_count
    accumulator.undecodable_geometry_count += result.undecodable_geometry_count
    accumulator.empty_geometry_count += result.empty_geometry_count
    accumulator.invalid_geometry_count += result.invalid_geometry_count
    accumulator.unsupported_geometry_count += result.unsupported_geometry_count
    accumulator.malformed_start_date_count += result.malformed_start_date_count
    accumulator.null_start_date_count += result.null_start_date_count
    accumulator.null_end_date_count += result.null_end_date_count
    accumulator.null_area_count += result.null_area_count
    accumulator.antimeridian_review_count += result.antimeridian_review_count
    accumulator.geometry_type_counts.update(result.geometry_type_counts)
    accumulator.years.update(result.years)
    accumulator.durations.update(result.durations)
    for row in accepted:
        source_id = str(row["source_record_id"])
        accumulator.uuid_counts[source_id] += 1
        start_date = row["event_start_date"]
        end_date = row["event_end_date"]
        area = row["reported_area_km2"]
        assert isinstance(start_date, date)
        accumulator.min_start_date = _min_date(accumulator.min_start_date, start_date)
        accumulator.max_start_date = _max_date(accumulator.max_start_date, start_date)
        if isinstance(end_date, date):
            accumulator.min_end_date = _min_date(accumulator.min_end_date, end_date)
            accumulator.max_end_date = _max_date(accumulator.max_end_date, end_date)
        if isinstance(area, float):
            accumulator.min_area_km2 = _min_float(accumulator.min_area_km2, area)
            accumulator.max_area_km2 = _max_float(accumulator.max_area_km2, area)
            accumulator.area_sum += area
            accumulator.area_count += 1
        accumulator.minx = _min_float(accumulator.minx, float(row["bounds_minx"]))
        accumulator.miny = _min_float(accumulator.miny, float(row["bounds_miny"]))
        accumulator.maxx = _max_float(accumulator.maxx, float(row["bounds_maxx"]))
        accumulator.maxy = _max_float(accumulator.maxy, float(row["bounds_maxy"]))


def _profile_payload(
    path: Path,
    inspection: GroundsourceInspection,
    duplicate_policy: DuplicatePolicy,
    preserve_invalid_geometries: bool,
    batch_size: int,
    max_rows: int | None,
    max_row_groups: int | None,
) -> dict[str, Any]:
    return _profile_payload_from_accumulator(
        path,
        inspection,
        _audit_accumulator(
            path,
            inspection.geoparquet,
            duplicate_policy=duplicate_policy,
            preserve_invalid_geometries=preserve_invalid_geometries,
            batch_size=batch_size,
            max_rows=max_rows,
            max_row_groups=max_row_groups,
        ),
    )


def _profile_payload_from_accumulator(
    path: Path,
    inspection: GroundsourceInspection,
    accumulator: AuditAccumulator,
) -> dict[str, Any]:
    return {
        "source_file": path.name,
        "total_record_count": accumulator.total_rows,
        "valid_record_count": accumulator.valid_rows,
        "quarantined_record_count": accumulator.quarantined_rows,
        "rejected_record_count": accumulator.rejected_rows,
        "row_accounting": {
            "source_rows": accumulator.total_rows,
            "canonical_accepted_rows": accumulator.valid_rows,
            "quarantined_rows": accumulator.quarantined_rows,
            "rejected_rows": accumulator.rejected_rows,
            "balanced": accumulator.total_rows
            == accumulator.valid_rows + accumulator.quarantined_rows + accumulator.rejected_rows,
        },
        "timing": {
            "processing_seconds": round(accumulator.processing_seconds, 6),
            "rows_per_second": round(accumulator.rows_per_second, 3),
        },
        "geoparquet": inspection.geoparquet.to_dict(),
        "geometry_type_distribution": dict(sorted(accumulator.geometry_type_counts.items())),
        "valid_geometry_count": accumulator.valid_rows,
        "invalid_geometry_count": accumulator.invalid_geometry_count,
        "empty_geometry_count": accumulator.empty_geometry_count,
        "null_geometry_count": accumulator.null_geometry_count,
        "undecodable_geometry_count": accumulator.undecodable_geometry_count,
        "geometry_bounds": _bounds_payload(accumulator),
        "reported_area_distribution": {
            "min": accumulator.min_area_km2,
            "max": accumulator.max_area_km2,
            "mean": (
                accumulator.area_sum / accumulator.area_count
                if accumulator.area_count > 0
                else None
            ),
            "null_count": accumulator.null_area_count,
        },
        "start_date_coverage": {
            "min": _date_string(accumulator.min_start_date),
            "max": _date_string(accumulator.max_start_date),
            "records_by_year": {
                str(year): count for year, count in sorted(accumulator.years.items())
            },
        },
        "end_date_coverage": {
            "min": _date_string(accumulator.min_end_date),
            "max": _date_string(accumulator.max_end_date),
            "null_count": accumulator.null_end_date_count,
        },
        "event_duration_distribution_days": {
            str(duration): count for duration, count in sorted(accumulator.durations.items())
        },
        "duplicate_uuid_count": accumulator.duplicate_uuid_count,
        "ignored_source_columns": inspection.ignored_fields,
        "antimeridian_review_count": accumulator.antimeridian_review_count,
    }


def _write_field_quality_csv(
    output_path: Path,
    inspection: GroundsourceInspection,
    accumulator: AuditAccumulator,
) -> None:
    rows = [
        _field_quality_row("uuid", accumulator.total_rows, accumulator.null_uuid_count),
        _field_quality_row("geometry", accumulator.total_rows, accumulator.null_geometry_count),
        _field_quality_row("start_date", accumulator.total_rows, accumulator.null_start_date_count),
        _field_quality_row("end_date", accumulator.total_rows, accumulator.null_end_date_count),
        _field_quality_row("area_km2", accumulator.total_rows, accumulator.null_area_count),
    ]
    for ignored in inspection.ignored_fields:
        rows.append(
            {
                "field": ignored,
                "role": "ignored",
                "null_count": "",
                "null_percentage": "",
            }
        )
    _write_csv(output_path, rows, ["field", "role", "null_count", "null_percentage"])


def _field_quality_row(field_name: str, total_rows: int, null_count: int) -> dict[str, object]:
    role = "required" if field_name in REQUIRED_SOURCE_FIELDS else "optional"
    return {
        "field": field_name,
        "role": role,
        "null_count": null_count,
        "null_percentage": _percentage(null_count, total_rows),
    }


def _write_temporal_coverage_csv(output_path: Path, accumulator: AuditAccumulator) -> None:
    rows: list[dict[str, object]] = [
        {"year": year, "records": count} for year, count in sorted(accumulator.years.items())
    ]
    _write_csv(output_path, rows, ["year", "records"])


def _write_geometry_quality_csv(output_path: Path, accumulator: AuditAccumulator) -> None:
    rows: list[dict[str, object]] = [
        {"metric": "valid_geometry_count", "value": accumulator.valid_rows},
        {"metric": "invalid_geometry_count", "value": accumulator.invalid_geometry_count},
        {"metric": "empty_geometry_count", "value": accumulator.empty_geometry_count},
        {"metric": "null_geometry_count", "value": accumulator.null_geometry_count},
        {"metric": "undecodable_geometry_count", "value": accumulator.undecodable_geometry_count},
        {"metric": "unsupported_geometry_count", "value": accumulator.unsupported_geometry_count},
        {"metric": "antimeridian_review_count", "value": accumulator.antimeridian_review_count},
    ]
    _write_csv(output_path, rows, ["metric", "value"])


def _manifest(
    input_path: Path,
    inspection: GroundsourceInspection,
    accumulator: AuditAccumulator,
    duplicate_policy: DuplicatePolicy,
    preserve_invalid_geometries: bool,
    batch_size: int,
    max_rows: int | None,
    max_row_groups: int | None,
) -> dict[str, Any]:
    return {
        "source_file_name": input_path.name,
        "source_file_size_bytes": input_path.stat().st_size,
        "source_sha256": _sha256(input_path),
        "audit_timestamp_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "package_version": __version__,
        "configuration": {
            "required_source_fields": sorted(REQUIRED_SOURCE_FIELDS),
            "optional_source_fields": sorted(OPTIONAL_SOURCE_FIELDS),
            "ignored_source_fields": inspection.ignored_fields,
            "geometry_encoding": inspection.geoparquet.encoding,
            "source_crs": inspection.geoparquet.source_crs,
            "duplicate_policy": duplicate_policy,
            "preserve_invalid_geometries": preserve_invalid_geometries,
            "batch_size": batch_size,
            "max_rows": max_rows,
            "max_row_groups": max_row_groups,
        },
        "input_row_count": accumulator.total_rows,
        "output_row_count": accumulator.valid_rows,
        "rejected_row_count": accumulator.rejected_rows,
        "quarantined_row_count": accumulator.quarantined_rows,
        "processing_seconds": round(accumulator.processing_seconds, 6),
        "rows_per_second": round(accumulator.rows_per_second, 3),
    }


def _geo_output_metadata(geoparquet: GeoParquetMetadata) -> dict[bytes, bytes]:
    raw = geoparquet.raw or {
        "version": "0.4.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": geoparquet.encoding or "WKB",
                "crs": geoparquet.crs,
                "geometry_types": geoparquet.geometry_types,
                "bbox": geoparquet.bbox,
            }
        },
    }
    return {b"geo": json.dumps(raw, sort_keys=True).encode("utf-8")}


def _canonical_arrow_schema(geoparquet: GeoParquetMetadata) -> pa.Schema:
    return pa.schema(_canonical_arrow_schema_fields(), metadata=_geo_output_metadata(geoparquet))


def _quarantine_arrow_schema(geoparquet: GeoParquetMetadata) -> pa.Schema:
    return pa.schema(
        [
            *_canonical_arrow_schema_fields(),
            pa.field("source_uuid", pa.string()),
            pa.field("source_row_index", pa.int64()),
            pa.field("conflict_reason", pa.string()),
        ],
        metadata=_geo_output_metadata(geoparquet),
    )


def _canonical_arrow_schema_fields() -> list[pa.Field]:
    return [
        pa.field("source_record_id", pa.string()),
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
        pa.field("country_code", pa.string()),
        pa.field("country_assignment_method", pa.string()),
        pa.field("state_code", pa.string()),
        pa.field("provenance", pa.string()),
        pa.field("antimeridian_review", pa.bool_()),
    ]


def _parse_uuid(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return str(uuid.UUID(text))
    except ValueError:
        return None


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _normalize_country(country: str) -> str:
    normalized = country.strip().upper()
    if len(normalized) not in {2, 3}:
        raise GroundsourceError("Country must be a 2- or 3-character country code.")
    return normalized


def _needs_antimeridian_review(bounds: tuple[float, float, float, float]) -> bool:
    minx, _, maxx, _ = bounds
    return (
        minx <= ANTIMERIDIAN_MIN_REVIEW_LONGITUDE
        or maxx >= ANTIMERIDIAN_MAX_REVIEW_LONGITUDE
        or (maxx - minx) > ANTIMERIDIAN_WIDTH_REVIEW_DEGREES
    )


def _bounds_payload(accumulator: AuditAccumulator) -> dict[str, float | None]:
    return {
        "minx": accumulator.minx,
        "miny": accumulator.miny,
        "maxx": accumulator.maxx,
        "maxy": accumulator.maxy,
    }


def _min_date(current: date | None, value: date) -> date:
    return value if current is None or value < current else current


def _max_date(current: date | None, value: date) -> date:
    return value if current is None or value > current else current


def _min_float(current: float | None, value: float) -> float:
    return value if current is None or value < current else current


def _max_float(current: float | None, value: float) -> float:
    return value if current is None or value > current else current


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(cast(SupportsFloat, value))
    except (TypeError, ValueError):
        return None


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _optional_float_list(value: object) -> list[float] | None:
    if not isinstance(value, list):
        return None
    return [float(item) for item in value]


def _date_string(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _percentage(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 6)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
