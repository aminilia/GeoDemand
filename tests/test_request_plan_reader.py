from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geodemand.trends import TrendsError, read_request_plan_rows

LONG_ID = "d7825d9b97515a3207c965a659da8883e2f3663ea0dbd5402565a7b9febf5351"


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig"])
def test_csv_and_bom_csv_preserve_plan_values(tmp_path: Path, encoding: str) -> None:
    path = tmp_path / "plan.csv"
    _write_csv(path, [_row()], encoding=encoding)

    rows = read_request_plan_rows(path, required_fields={"request_id", "event_start_date"})

    assert rows == [_row()]
    assert rows[0]["request_id"] == LONG_ID
    assert isinstance(rows[0]["request_id"], str)
    assert rows[0]["event_start_date"] == "2023-04-10"
    assert rows[0]["geography"] == "US-FL"


def test_header_whitespace_is_normalized_without_case_changes(tmp_path: Path) -> None:
    path = tmp_path / "plan.csv"
    path.write_text(
        f"  request_id  , geography \n{LONG_ID},US-FL\n",
        encoding="utf-8-sig",
    )

    assert read_request_plan_rows(path) == [{"request_id": LONG_ID, "geography": "US-FL"}]


@pytest.mark.parametrize("suffix", [".parquet", ".pq"])
def test_parquet_and_pq_are_logically_equivalent_to_csv(tmp_path: Path, suffix: str) -> None:
    csv_path = tmp_path / "plan.csv"
    parquet_path = tmp_path / f"plan{suffix}"
    row = _row()
    _write_csv(csv_path, [row], encoding="utf-8-sig")
    pq.write_table(pa.Table.from_pylist([row]), parquet_path)

    assert read_request_plan_rows(csv_path) == read_request_plan_rows(parquet_path)


def test_duplicate_normalized_headers_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "plan.csv"
    path.write_text(f"request_id, request_id \n{LONG_ID},{LONG_ID}\n", encoding="utf-8-sig")

    with pytest.raises(TrendsError, match="request_plan_duplicate_headers"):
        read_request_plan_rows(path)


def test_missing_field_error_lists_missing_available_and_path(tmp_path: Path) -> None:
    path = tmp_path / "plan.csv"
    path.write_text("episode_id,geography\nepisode,US-FL\n", encoding="utf-8-sig")

    with pytest.raises(TrendsError) as raised:
        read_request_plan_rows(path)

    message = str(raised.value)
    assert "request_plan_missing_fields" in message
    assert "missing=['request_id']" in message
    assert "available=['episode_id', 'geography']" in message
    assert f"path={path}" in message
    assert type(raised.value) is TrendsError


def test_unsupported_suffix_is_domain_error(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(TrendsError, match=r"unsupported_tabular_format: \.json") as raised:
        read_request_plan_rows(path)

    assert type(raised.value) is TrendsError


def _row() -> dict[str, Any]:
    return {
        "request_id": LONG_ID,
        "episode_id": "episode",
        "geography": "US-FL",
        "event_start_date": "2023-04-10",
        "event_end_date": "2023-04-10",
        "terminology_version": "0.8A-v3",
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], *, encoding: str) -> None:
    with path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
