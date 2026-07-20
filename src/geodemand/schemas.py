from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA_VERSION = "1.0.0"
PHYSICAL_EVIDENCE_SCHEMA_VERSION = "physical-evidence-v1"
DataOrigin = Literal["observed", "synthetic_fixture"]
ALLOWED_DATA_ORIGINS = frozenset({"observed", "synthetic_fixture"})

PROVENANCE_FIELDS = [
    pa.field("schema_version", pa.string(), nullable=False),
    pa.field("data_origin", pa.string(), nullable=False),
    pa.field("source_dataset", pa.string(), nullable=False),
    pa.field("source_product", pa.string(), nullable=False),
    pa.field("source_manifest_hash", pa.string(), nullable=False),
    pa.field("decoder_version", pa.string(), nullable=False),
    pa.field("code_commit", pa.string(), nullable=False),
    pa.field("rule_version", pa.string(), nullable=False),
]


def _schema(name: str, fields: Sequence[pa.Field]) -> pa.Schema:
    return pa.schema(
        [pa.field("schema_version", pa.string(), nullable=False), *fields],
        metadata={
            b"geodemand.schema_name": name.encode(),
            b"geodemand.schema_version": SCHEMA_VERSION.encode(),
        },
    )


SCHEMAS: dict[str, pa.Schema] = {
    "candidate_events": _schema(
        "candidate_events",
        [
            pa.field("event_record_id", pa.string(), nullable=False),
            pa.field("start_date", pa.string()),
            pa.field("end_date", pa.string()),
            pa.field("geometry", pa.binary()),
            pa.field("primary_state_code", pa.string()),
        ],
    ),
    "event_state_records": _schema(
        "event_state_records",
        [
            pa.field("event_record_id", pa.string(), nullable=False),
            pa.field("state_code", pa.string(), nullable=False),
            pa.field("event_area_fraction", pa.float64()),
            pa.field("us_overlap_fraction", pa.float64()),
        ],
    ),
    "episodes": _schema(
        "episodes",
        [
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("policy", pa.string(), nullable=False),
            pa.field("episode_start_date", pa.string()),
            pa.field("episode_end_date", pa.string()),
            pa.field("member_count", pa.int64(), nullable=False),
            pa.field("geometry", pa.binary()),
        ],
    ),
    "episode_membership": _schema(
        "episode_membership",
        [
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("event_record_id", pa.string(), nullable=False),
            pa.field("policy", pa.string()),
        ],
    ),
    "catalog_evidence": _schema(
        "catalog_evidence",
        [
            pa.field("provisional_episode_id", pa.string(), nullable=False),
            pa.field("source_conservative_episode_id", pa.string(), nullable=False),
            pa.field("mrms_status", pa.string()),
            pa.field("usgs_status", pa.string()),
            pa.field("rule_version", pa.string(), nullable=False),
        ],
    ),
    "mrms_episode_metrics": pa.schema(
        [
            *PROVENANCE_FIELDS,
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("coverage_fraction", pa.float64()),
            pa.field("max_gridcell_1h_mm", pa.float64()),
        ],
        metadata={
            b"geodemand.schema_name": b"mrms_episode_metrics",
            b"geodemand.schema_version": PHYSICAL_EVIDENCE_SCHEMA_VERSION.encode(),
        },
    ),
    "mrms_member_metrics": pa.schema(
        [
            *PROVENANCE_FIELDS,
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("event_record_id", pa.string(), nullable=False),
            pa.field("max_1h_mm", pa.float64()),
        ],
        metadata={
            b"geodemand.schema_name": b"mrms_member_metrics",
            b"geodemand.schema_version": PHYSICAL_EVIDENCE_SCHEMA_VERSION.encode(),
        },
    ),
    "imerg_episode_metrics": pa.schema(
        [
            *PROVENANCE_FIELDS,
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("coverage_fraction", pa.float64()),
            pa.field("maximum_gridcell_30m_mm", pa.float64()),
        ],
        metadata={
            b"geodemand.schema_name": b"imerg_episode_metrics",
            b"geodemand.schema_version": PHYSICAL_EVIDENCE_SCHEMA_VERSION.encode(),
        },
    ),
    "imerg_member_metrics": pa.schema(
        [
            *PROVENANCE_FIELDS,
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("event_record_id", pa.string(), nullable=False),
            pa.field("maximum_30m_mm", pa.float64()),
        ],
        metadata={
            b"geodemand.schema_name": b"imerg_member_metrics",
            b"geodemand.schema_version": PHYSICAL_EVIDENCE_SCHEMA_VERSION.encode(),
        },
    ),
    "usgs_observations": pa.schema(
        [
            *PROVENANCE_FIELDS,
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("monitoring_location_id", pa.string(), nullable=False),
            pa.field("parameter_code", pa.string(), nullable=False),
            pa.field("time_utc", pa.string(), nullable=False),
            pa.field("value", pa.float64()),
            pa.field("unit", pa.string()),
            pa.field("qualifier", pa.string()),
            pa.field("approval_status", pa.string()),
            pa.field("backend", pa.string()),
        ],
        metadata={
            b"geodemand.schema_name": b"usgs_observations",
            b"geodemand.schema_version": PHYSICAL_EVIDENCE_SCHEMA_VERSION.encode(),
        },
    ),
    "usgs_response_metrics": pa.schema(
        [
            *PROVENANCE_FIELDS,
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("monitoring_location_id", pa.string(), nullable=False),
            pa.field("parameter_code", pa.string(), nullable=False),
            pa.field("response_score", pa.float64()),
            pa.field("response_detected", pa.bool_()),
            pa.field("association_quality", pa.string()),
        ],
        metadata={
            b"geodemand.schema_name": b"usgs_response_metrics",
            b"geodemand.schema_version": PHYSICAL_EVIDENCE_SCHEMA_VERSION.encode(),
        },
    ),
    "trends_observations": _schema(
        "trends_observations",
        [
            pa.field("request_id", pa.string(), nullable=False),
            pa.field("repeat_id", pa.string(), nullable=False),
            pa.field("export_attempt_id", pa.string()),
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("geography", pa.string(), nullable=False),
            pa.field("concept_id", pa.string(), nullable=False),
            pa.field("terminology_version", pa.string(), nullable=False),
            pa.field("date", pa.date32(), nullable=False),
            pa.field("interest", pa.float64()),
        ],
    ),
    "phase_response_metrics": _schema(
        "phase_response_metrics",
        [
            pa.field("request_id", pa.string(), nullable=False),
            pa.field("repeat_id", pa.string(), nullable=False),
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("concept_id", pa.string(), nullable=False),
            pa.field("phase", pa.string(), nullable=False),
            pa.field("standardized_phase_lift", pa.float64()),
            pa.field("robust_phase_lift", pa.float64()),
        ],
    ),
    "concurrence_metrics": _schema(
        "concurrence_metrics",
        [
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("repeat_id", pa.string(), nullable=False),
            pa.field("concept_id", pa.string(), nullable=False),
            pa.field("concurrence_class", pa.string()),
        ],
    ),
    "control_adjusted_metrics": _schema(
        "control_adjusted_metrics",
        [
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("repeat_id", pa.string(), nullable=False),
            pa.field("concept_id", pa.string(), nullable=False),
            pa.field("phase", pa.string(), nullable=False),
            pa.field("control_count", pa.int64()),
            pa.field("adjusted_standardized_lift", pa.float64()),
        ],
    ),
    "peak_attribution": _schema(
        "peak_attribution",
        [
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("concept_id", pa.string(), nullable=False),
            pa.field("global_peak_date", pa.string()),
            pa.field("global_peak_phase", pa.string()),
            pa.field("global_peak_value", pa.float64()),
            pa.field("dominant_event_response_phase", pa.string()),
            pa.field("dominant_standardized_phase_lift", pa.float64()),
            pa.field("dominant_robust_phase_lift", pa.float64()),
            pa.field("attribution_category", pa.string()),
        ],
    ),
}


def schema_for(name: str) -> pa.Schema:
    try:
        return SCHEMAS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown GeoDemand schema: {name}") from exc


def validate_required_fields(table: pa.Table, name: str) -> None:
    expected = schema_for(name)
    missing = [
        field.name
        for field in expected
        if not field.nullable and field.name not in table.column_names
    ]
    if missing:
        raise ValueError(f"{name} is missing required fields: {', '.join(missing)}")
    for field in expected:
        if field.name in table.column_names and not table.schema.field(field.name).type.equals(
            field.type
        ):
            raise ValueError(
                f"{name}.{field.name} has type {table.schema.field(field.name).type}; "
                f"expected {field.type}."
            )


def validate_physical_evidence_provenance(
    table: pa.Table, label: str, *, require_observed: bool = True
) -> None:
    required = {field.name for field in PROVENANCE_FIELDS}
    missing = sorted(required - set(table.column_names))
    if missing:
        raise ValueError(f"{label} is missing provenance fields: {', '.join(missing)}")
    for row_index, row in enumerate(table.select(sorted(required)).to_pylist()):
        if row["schema_version"] != PHYSICAL_EVIDENCE_SCHEMA_VERSION:
            raise ValueError(
                f"{label} row {row_index} has unsupported schema_version: {row['schema_version']!r}"
            )
        origin = row["data_origin"]
        if origin not in ALLOWED_DATA_ORIGINS:
            raise ValueError(f"{label} row {row_index} has invalid data_origin: {origin!r}")
        if require_observed and origin != "observed":
            raise ValueError(f"{label} row {row_index} has rejected data_origin: {origin!r}")
        empty = sorted(name for name in required if row.get(name) in {None, ""})
        if empty:
            raise ValueError(
                f"{label} row {row_index} has empty provenance fields: {', '.join(empty)}"
            )


def write_rows(path: Path, rows: Sequence[Mapping[str, Any]], name: str) -> None:
    schema = schema_for(name)
    normalized = [
        {**row, "schema_version": row.get("schema_version", SCHEMA_VERSION)} for row in rows
    ]
    table = pa.Table.from_pylist(normalized, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")
