from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA_VERSION = "1.0.0"
DataOrigin = Literal["observed", "synthetic_fixture"]
ALLOWED_DATA_ORIGINS = frozenset({"observed", "synthetic_fixture"})


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
            pa.field("balanced_merges_multiple_conservative_episodes", pa.bool_()),
            pa.field("balanced_crosses_split_boundary", pa.bool_()),
            pa.field("rule_version", pa.string(), nullable=False),
        ],
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


def write_rows(path: Path, rows: Sequence[Mapping[str, Any]], name: str) -> None:
    schema = schema_for(name)
    normalized = [
        {**row, "schema_version": row.get("schema_version", SCHEMA_VERSION)} for row in rows
    ]
    table = pa.Table.from_pylist(normalized, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")
