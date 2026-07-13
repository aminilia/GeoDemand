# Groundsource Data Card

## Intended Use

Groundsource records are used to establish candidate flash-flood events and
their affected regions for downstream search-demand analysis.

## Required Schema

- `event_id`: stable event identifier.
- `event_date`: event date.
- `country`: country code used for filtering and partitioning.
- `region_id`: stable region identifier.
- `region_name`: human-readable region name.
- `latitude`: representative latitude.
- `longitude`: representative longitude.
- `flood_severity`: numeric severity indicator from the source dataset.
- `source`: source label or provenance field.

## Privacy And Licensing

No raw data is committed to this repository. Users are responsible for ensuring
that local Groundsource files are licensed for research use.

## Known Limitations

Milestone 0 validates structure and creates reproducible local outputs. It does
not evaluate event completeness, flood attribution quality, search behavior, or
forecasting accuracy.
