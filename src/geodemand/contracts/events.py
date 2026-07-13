from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class FloodEventRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1)
    event_date: date
    country: str = Field(min_length=2, max_length=3)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    flood_severity: float = Field(ge=0)
    source: str = Field(min_length=1)
