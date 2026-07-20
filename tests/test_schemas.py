from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geodemand.schemas import (
    PHYSICAL_EVIDENCE_SCHEMA_VERSION,
    SCHEMAS,
    schema_for,
    validate_physical_evidence_provenance,
    validate_required_fields,
    write_rows,
)


def test_all_scientific_schemas_are_versioned_and_support_empty_outputs(
    tmp_path: Path,
) -> None:
    for name, schema in SCHEMAS.items():
        assert schema.metadata is not None
        assert schema.metadata[b"geodemand.schema_name"].decode() == name
        assert schema.metadata[b"geodemand.schema_version"]
        path = tmp_path / f"{name}.parquet"
        write_rows(path, [], name)
        written = pq.read_schema(path)
        assert written.equals(schema, check_metadata=True)


def test_schema_validation_rejects_missing_and_incompatible_required_fields() -> None:
    missing = pa.table({"schema_version": ["1.0.0"]})
    with pytest.raises(ValueError, match="missing required fields"):
        validate_required_fields(missing, "episode_membership")

    incompatible = pa.table(
        {
            "schema_version": ["1.0.0"],
            "episode_id": [1],
            "event_record_id": ["event-1"],
            "policy": ["conservative"],
        }
    )
    with pytest.raises(ValueError, match="episode_id has type"):
        validate_required_fields(incompatible, "episode_membership")


def test_nonempty_schema_write_is_explicit_and_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "membership.parquet"
    rows = [
        {
            "episode_id": "episode-1",
            "event_record_id": "event-1",
            "policy": "conservative",
        }
    ]
    write_rows(path, rows, "episode_membership")
    first = path.read_bytes()
    write_rows(path, rows, "episode_membership")
    assert path.read_bytes() == first
    assert pq.read_table(path).to_pylist()[0]["schema_version"] == "1.0.0"


def test_unknown_schema_fails_clearly() -> None:
    with pytest.raises(ValueError, match="Unknown GeoDemand schema"):
        schema_for("not-a-schema")


def test_shared_physical_provenance_accepts_observed_and_rejects_synthetic_imerg() -> None:
    provenance = {
        "schema_version": [PHYSICAL_EVIDENCE_SCHEMA_VERSION],
        "data_origin": ["observed"],
        "source_dataset": ["NASA GPM"],
        "source_product": ["GPM_3IMERGHH_07"],
        "source_manifest_hash": ["abc123"],
        "decoder_version": ["decoder-v1"],
        "code_commit": ["commit"],
        "rule_version": ["rules-v1"],
    }
    validate_physical_evidence_provenance(pa.table(provenance), "IMERG episode metrics")
    provenance["data_origin"] = ["synthetic_fixture"]
    with pytest.raises(ValueError, match="rejected data_origin"):
        validate_physical_evidence_provenance(pa.table(provenance), "IMERG episode metrics")
