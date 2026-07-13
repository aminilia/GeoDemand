from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture()
def groundsource_path(tmp_path: Path) -> Path:
    path = tmp_path / "groundsource.parquet"
    frame = pl.DataFrame(
        {
            "event_id": ["flood-1", "flood-2", "flood-3"],
            "event_date": [date(2026, 1, 5), date(2026, 1, 6), date(2026, 2, 1)],
            "country": ["US", "US", "CA"],
            "region_id": ["us-nyc", "us-hou", "ca-tor"],
            "region_name": ["New York", "Houston", "Toronto"],
            "latitude": [40.7128, 29.7604, 43.6532],
            "longitude": [-74.0060, -95.3698, -79.3832],
            "flood_severity": [2.5, 3.0, 1.0],
            "source": ["synthetic", "synthetic", "synthetic"],
        }
    )
    frame.write_parquet(path)
    return path
