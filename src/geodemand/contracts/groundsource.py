from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class RawGroundsourceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    uuid: str = Field(min_length=1)
    geometry: bytes = Field(min_length=1)
    start_date: str = Field(min_length=1)
    area_km2: float | None = Field(default=None, ge=0)
    end_date: str | None = Field(default=None, min_length=1)


class CanonicalFloodEventRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_record_id: str = Field(min_length=1)
    source_name: str = Field(min_length=1)
    event_start_date: date
    event_end_date: date | None = None
    geometry: bytes = Field(min_length=1)
    geometry_encoding: str = Field(min_length=1)
    source_crs: str | None = Field(default=None, min_length=1)
    reported_area_km2: float | None = Field(default=None, ge=0)
    geometry_type: str = Field(min_length=1)
    geometry_is_valid: bool
    geometry_is_empty: bool
    representative_longitude: float = Field(ge=-180, le=180)
    representative_latitude: float = Field(ge=-90, le=90)
    bounds_minx: float
    bounds_miny: float
    bounds_maxx: float
    bounds_maxy: float
    country_code: str | None = Field(default=None, min_length=2, max_length=3)
    country_assignment_method: str | None = Field(default=None, min_length=1)
    state_code: str | None = Field(default=None, min_length=1)
    provenance: str = Field(min_length=1)
    antimeridian_review: bool
