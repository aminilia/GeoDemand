from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class SearchInterestRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    region_id: str = Field(min_length=1)
    query: str = Field(min_length=1)
    observation_date: date
    search_interest: int = Field(ge=0, le=100)
    source: str = Field(min_length=1)
