# Groundsource Audit

## Actual Schema Discovered

The confirmed Groundsource schema is:

- `uuid`: string
- `area_km2`: double
- `geometry`: binary
- `start_date`: string
- `end_date`: string
- `__index_level_0__`: int64

The confirmed GeoParquet metadata is:

- version: `0.4.0`
- primary geometry column: `geometry`
- geometry encoding: `WKB`
- CRS: `EPSG:4326` / WGS 84
- geometry types: `Polygon`, `MultiPolygon`
- dataset bounding box: `[-180.0, -76.812618, 180.0, 81.164611]`

`inspect-groundsource` reports raw Arrow schema and GeoParquet key-value
metadata before validation. It does not require country, latitude, or longitude.

## Source Contract

Required fields:

- `uuid`
- `geometry`
- `start_date`

Nullable or optional fields:

- `area_km2`
- `end_date`

Ignored fields:

- `__index_level_0__`

## Quality Limitations

The audit validates schema usability and basic record quality. It does not prove
that Groundsource event coverage is complete, that flood labels are correct, or
that event geography matches later Google search regions.

## Filtering Decisions

Raw Groundsource does not include a country field. `filter-groundsource
--country US` requires `--country-boundaries PATH`; until spatial enrichment is
implemented, the command fails clearly instead of returning an empty result or
inventing country from coordinates.

## Rejected-Record Rules

Records are rejected when any of the following are true:

- UUID is null or malformed
- UUID is duplicated according to the current duplicate policy
- geometry is null
- geometry WKB is undecodable
- geometry is empty
- geometry type is unsupported
- `start_date` is missing or malformed

Invalid but decodable geometries are counted separately and are not silently
repaired. Nullable `end_date`, nullable `area_km2`, unassigned country, and
unassigned state are reported but do not by themselves reject a record.

## Known Uncertainties

- Country and state assignment require a future boundary-based spatial
  enrichment step.
- The audit does not calculate planar area in EPSG:4326; it only reports
  source-provided `area_km2`.
- Duplicate UUIDs may reflect source updates or source errors; the audit counts
  them but does not resolve them.

## Reproduce The Audit

```powershell
uv run geodemand data inspect-groundsource --input PATH
uv run geodemand data audit-groundsource --input PATH --output-dir AUDIT_DIR
uv run geodemand data validate-groundsource --input PATH
uv run geodemand data filter-groundsource --input PATH --output OUTPUT_DIR --country US --country-boundaries BOUNDARIES
```

## Data Licensing And Redistribution

Do not commit raw Groundsource data or full-size generated outputs. Users are
responsible for ensuring that their local Groundsource data license permits
research use and any downstream sharing.
