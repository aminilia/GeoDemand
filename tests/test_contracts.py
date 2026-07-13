from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from geodemand.contracts import (
    EventRegionIntersection,
    FloodEventRecord,
    GeographicRegion,
    SearchInterestRecord,
)


def test_contracts_accept_valid_records() -> None:
    FloodEventRecord(
        event_id="flood-1",
        event_date=date(2026, 1, 5),
        country="US",
        latitude=40.0,
        longitude=-75.0,
        flood_severity=2.0,
        source="synthetic",
    )
    GeographicRegion(
        region_id="us-nyc",
        region_name="New York",
        country="US",
        latitude=40.0,
        longitude=-75.0,
    )
    SearchInterestRecord(
        region_id="us-nyc",
        query="flood cleanup",
        observation_date=date(2026, 1, 6),
        search_interest=83,
        source="synthetic",
    )
    EventRegionIntersection(
        event_id="flood-1",
        region_id="us-nyc",
        country="US",
        overlap_area_km2=12.5,
        overlap_fraction=0.25,
    )


def test_search_interest_is_bounded() -> None:
    with pytest.raises(ValidationError):
        SearchInterestRecord(
            region_id="us-nyc",
            query="flood cleanup",
            observation_date=date(2026, 1, 6),
            search_interest=101,
            source="synthetic",
        )
