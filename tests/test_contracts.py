from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from geodemand.contracts import (
    CanonicalFloodEventRecord,
    EventRegionIntersection,
    FloodEventRecord,
    GeographicRegion,
    RawGroundsourceRecord,
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
    RawGroundsourceRecord(
        uuid="550e8400-e29b-41d4-a716-446655440000",
        geometry=b"\x01\x02",
        start_date="2026-01-05",
        area_km2=None,
        end_date=None,
    )
    CanonicalFloodEventRecord(
        source_record_id="550e8400-e29b-41d4-a716-446655440000",
        source_name="Groundsource",
        event_start_date=date(2026, 1, 5),
        event_end_date=None,
        geometry=b"\x01\x02",
        geometry_encoding="WKB",
        source_crs="EPSG:4326",
        reported_area_km2=None,
        geometry_type="Polygon",
        geometry_is_valid=True,
        geometry_is_empty=False,
        representative_longitude=-74.5,
        representative_latitude=40.5,
        bounds_minx=-75.0,
        bounds_miny=40.0,
        bounds_maxx=-74.0,
        bounds_maxy=41.0,
        country_code=None,
        country_assignment_method=None,
        state_code=None,
        provenance="synthetic",
        antimeridian_review=False,
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
