from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class GeographicRegion(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    region_id: str = Field(min_length=1)
    region_name: str = Field(min_length=1)
    country: str = Field(min_length=2, max_length=3)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
