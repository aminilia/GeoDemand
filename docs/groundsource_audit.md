# Groundsource Audit

## Actual Schema Discovered

The confirmed Groundsource schema from the full audit is:

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

## Full-Data Audit Results

The Milestone 0.5D full audit was generated from `groundsource_2026.parquet` on
2026-07-13 UTC.

- source SHA-256:
  `77c266ba5a5176d983edca989a81ff73f21c556fd98c82e2131c2f8d172546ce`
- source file size: 667,122,400 bytes
- source rows: 2,646,302
- accepted canonical rows: 2,646,302
- quarantined rows: 0
- rejected rows: 0
- row accounting: balanced
- processing time: 917.860619 seconds
- throughput: 2,883.12 rows per second

Terminal row accounting satisfies:

```text
2,646,302 source rows = 2,646,302 accepted + 0 quarantined + 0 rejected
```

## Field Quality

The required fields `uuid`, `geometry`, and `start_date` had zero nulls in the
full audit. The optional fields `area_km2` and `end_date` also had zero nulls.
`__index_level_0__` was ignored as a pandas index artifact.

No unknown source fields were present.

## Geometry Quality

The full audit decoded WKB in bounded batches and preserved the original WKB
geometry and GeoParquet metadata. Results:

- valid geometries: 2,646,302
- invalid geometries: 0
- empty geometries: 0
- null geometries: 0
- undecodable geometries: 0
- unsupported geometries: 0
- antimeridian review flags: 300

Geometry type distribution:

- `Polygon`: 2,478,877
- `MultiPolygon`: 167,425

The dataset bounds observed in the full audit were
`[-180.0, -76.812618, 180.0, 81.164611]`. The audit does not calculate planar
area in EPSG:4326; it records the source-reported `area_km2`, which ranged from
approximately `0.0000017489639049426842` to `4998.825448311713`, with a mean of
`142.29071616884667`.

## Temporal Coverage

The full audit found usable `start_date` and `end_date` values for all records.

- start date range: 2000-01-01 to 2026-02-03
- end date range: 2000-01-01 to 2026-02-03
- records with malformed or missing `start_date`: 0

Records by start year:

| Year | Records |
| --- | ---: |
| 2000 | 498 |
| 2001 | 477 |
| 2002 | 1,646 |
| 2003 | 653 |
| 2004 | 1,750 |
| 2005 | 2,939 |
| 2006 | 3,397 |
| 2007 | 7,498 |
| 2008 | 8,919 |
| 2009 | 12,804 |
| 2010 | 33,717 |
| 2011 | 30,311 |
| 2012 | 33,911 |
| 2013 | 62,284 |
| 2014 | 75,978 |
| 2015 | 74,391 |
| 2016 | 112,583 |
| 2017 | 127,318 |
| 2018 | 163,277 |
| 2019 | 162,860 |
| 2020 | 198,201 |
| 2021 | 219,768 |
| 2022 | 225,068 |
| 2023 | 261,813 |
| 2024 | 402,012 |
| 2025 | 395,506 |
| 2026 | 26,723 |

Event duration distribution was bounded from 0 to 6 days in the full audit:
1,449,361 records lasted 0 days, 701,118 lasted 1 day, 247,964 lasted 2 days,
117,654 lasted 3 days, 64,578 lasted 4 days, 37,076 lasted 5 days, and 28,551
lasted 6 days.

## Duplicate UUID Findings

The full audit found no duplicate UUID groups:

- duplicate UUID groups: 0
- rows participating in duplicate groups: 0
- exact duplicate records: 0
- conflicting duplicate records: 0
- geometry conflicts: 0
- start/end date conflicts: 0
- reported area conflicts: 0

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
