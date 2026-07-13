# Groundsource Data Card

## Intended Use

Groundsource records are used to establish candidate flash-flood events and
their affected regions for downstream search-demand analysis.

## Confirmed Source Schema

- `uuid`: required source identifier.
- `geometry`: required WKB geometry.
- `start_date`: required event start date string.
- `area_km2`: optional or nullable reported source area.
- `end_date`: optional or nullable event end date string.
- `__index_level_0__`: ignored pandas index artifact.

Groundsource locations are geometry-based. The source does not provide latitude,
longitude, country, or state columns. Representative coordinates are derived
from the geometry representative point and should not be treated as source
measurements.

## Audit Outputs

The Groundsource audit writes schema, profile, field quality, temporal coverage,
geometry quality, rejected-record summary, and manifest artifacts. These
outputs are generated locally and should not include raw full-size datasets.

## Privacy And Licensing

No raw data is committed to this repository. Users are responsible for ensuring
that local Groundsource files are licensed for research use.

## Known Limitations

Milestone 0.5 validates raw schema, GeoParquet metadata, WKB geometry decoding,
basic quality, and reproducible audit metadata. It does not assign countries or
states, evaluate event completeness, score flood attribution quality, analyze
search behavior, or forecast demand.
