from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class EventRegionIntersection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1)
    region_id: str = Field(min_length=1)
    country: str = Field(min_length=2, max_length=3)
    overlap_area_km2: float = Field(ge=0)
    overlap_fraction: float = Field(ge=0, le=1)
