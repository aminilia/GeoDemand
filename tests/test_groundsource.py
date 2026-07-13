from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from geodemand.ingestion.groundsource import (
    GroundsourceError,
    filter_groundsource,
    inspect_groundsource,
    profile_groundsource,
)


def test_inspect_groundsource_reports_required_columns(groundsource_path: Path) -> None:
    inspection = inspect_groundsource(groundsource_path)

    assert inspection.rows == 3
    assert inspection.missing_columns == []
    assert "event_id" in inspection.columns


def test_filter_groundsource_writes_partitioned_output(
    groundsource_path: Path,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "processed"

    filter_groundsource(
        input_path=groundsource_path,
        output_path=output_path,
        country="us",
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 31),
    )

    files = list(output_path.rglob("*.parquet"))
    assert files
    result = pl.scan_parquet(output_path).collect()
    assert result.height == 2
    assert result["country"].unique().to_list() == ["US"]


def test_profile_groundsource_writes_json_report(groundsource_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "report.json"

    profile = profile_groundsource(groundsource_path, output_path)
    written_profile = json.loads(output_path.read_text(encoding="utf-8"))

    assert profile["rows"] == 3
    assert written_profile["country_counts"] == {"CA": 1, "US": 2}
    assert written_profile["date_range"]["min_event_date"] == "2026-01-05"
    assert written_profile["numeric_summary"]["flood_severity"]["max"] == 3.0


def test_missing_required_columns_raise_clear_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.parquet"
    pl.DataFrame({"event_id": ["flood-1"]}).write_parquet(path)

    with pytest.raises(GroundsourceError, match="missing required columns"):
        inspect_groundsource(path)
