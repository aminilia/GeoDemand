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

## Spatial Enrichment Outputs

Milestone 0.5E prepares Natural Earth 5.1.1 country boundaries and Census
TIGER/Line 2025 state boundaries, then enriches Groundsource records with
country, U.S. intersection, and state-overlap fields. The enrichment keeps one
terminal row per accepted event in `events_enriched/`, preserves event-country
relationships in `event_country_membership/`, preserves U.S. state overlaps in
`event_state_overlaps/`, and writes a reproducible `us_events/` subset.

Spatial assignment uses `representative_point()` for country point coverage and
EPSG:6933 overlap areas for cross-border and state ranking. Offshore,
cross-border, multistate, antimeridian, and manual-review records remain in the
enriched dataset instead of being rejected.

## Measured Milestone 0.5D Results

The full audit of `groundsource_2026.parquet` measured 2,646,302 source rows.
All rows were accepted into the canonical dataset, with 0 quarantined rows and 0
rejected rows. Terminal row accounting was balanced.

The source SHA-256 verified for the audited file was:

```text
77c266ba5a5176d983edca989a81ff73f21c556fd98c82e2131c2f8d172546ce
```

Field quality was stronger than the initial nullable assumptions: `uuid`,
`geometry`, `start_date`, `end_date`, and `area_km2` all had zero nulls in the
full audit. `__index_level_0__` remained an ignored pandas index artifact.

Geometry quality was clean for canonical processing:

- valid geometries: 2,646,302
- invalid geometries: 0
- empty, null, undecodable, or unsupported geometries: 0
- antimeridian review flags: 300
- geometry types: 2,478,877 `Polygon` records and 167,425 `MultiPolygon`
  records

Temporal coverage spans 2000-01-01 through 2026-02-03 for both `start_date` and
`end_date`. The audit found no malformed or missing `start_date` values.

No duplicate UUID groups were found. Therefore there were no duplicate-row
conflicts in geometry, dates, or reported area.

## Privacy And Licensing

No raw data is committed to this repository. Users are responsible for ensuring
that local Groundsource files are licensed for research use.

## Known Limitations

Milestone 0.5 validates raw schema, GeoParquet metadata, WKB geometry decoding,
basic quality, reproducible audit metadata, and boundary-based spatial
enrichment. It does not evaluate event completeness, score flood attribution
quality, analyze search behavior, or forecast demand. The 300
antimeridian-review flags from the full audit require special attention during
spatial review before downstream region matching.
