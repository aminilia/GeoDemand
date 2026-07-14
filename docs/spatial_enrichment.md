# Spatial Enrichment

Milestone 0.5E assigns Groundsource event geometries to countries and U.S.
states without loading the full Groundsource dataset into a GeoDataFrame.

## Boundary Provenance

Country boundaries use Natural Earth Admin 0 Countries version 5.1.1 with the
`de_facto` worldview. U.S. state and equivalent-entity boundaries use U.S.
Census TIGER/Line 2025, representing legal boundaries as of 2025-01-01.

Boundary preparation reads local archives, validates required attributes,
normalizes geometry to EPSG:4326, and writes deterministic GeoParquet outputs:

- `countries.parquet`
- `us_states.parquet`
- `boundary_manifest.json`

If an official boundary feature is invalid under Shapely topology rules,
preparation applies deterministic `shapely.make_valid` normalization and records
the normalized feature count in `boundary_manifest.json`. Synthetic malformed
boundary fixtures remain strict validation failures in tests.

## Field Mapping

Natural Earth alpha-2 codes are resolved from `ISO_A2_EH` and then `ISO_A2`.
Alpha-3 codes are resolved from `ADM0_A3`, `ISO_A3_EH`, and then `ISO_A3`.
Values of `-99`, null, and empty strings are treated as unresolved.

State boundaries require `STATEFP`, `STUSPS`, `NAME`, `GEOID`, `LSAD`, `MTFCC`,
and `FUNCSTAT`.

## Assignment Methods

Country assignment uses the complete event geometry and its
`representative_point()`:

- `representative_point`: exactly one country covers the representative point.
- `maximum_overlap`: multiple countries intersect and the largest EPSG:6933
  overlap is unique.
- `single_intersection`: the representative point is offshore, but exactly one
  country intersects the event polygon.
- `unassigned`: no country intersects the event polygon.
- `manual_review`: overlap ranking is tied or otherwise ambiguous.

Spatially unassigned, offshore, cross-border, multistate, and manual-review
events remain enriched rows. They are not rejected.

## U.S. And State Logic

The enrichment records both U.S. boundary concepts:

- `intersects_us_country_boundary`: complete event geometry intersects the
  Natural Earth United States country boundary.
- `intersects_us_state_union`: complete event geometry intersects the
  deterministic Census state/equivalent union.

`intersects_united_states` follows `intersects_us_state_union` for the state
workflow and reproducible U.S. subset. Natural Earth remains the country
assignment source, but Census state/equivalent geometry is the denominator for
U.S. state-overlap fractions.

State assignment runs only for state-union-intersecting events. Intersecting
states are ranked by EPSG:6933 overlap area and then deterministic state code
order. The largest-overlap state is primary, and all state overlaps are
preserved in `event_state_overlaps/`.

## Area Calculations

The pipeline does not calculate area in EPSG:4326. Event areas and overlap
areas are projected to EPSG:6933 and stored in square kilometers.

Fractions are written without silent clamping:

- `event_area_fraction`
- `us_overlap_fraction`, using the Census state/equivalent union overlap as
  the denominator
- `us_event_area_fraction`

Values outside the documented floating-point tolerance are flagged for spatial
review.

## Antimeridian Handling

Original WKB is preserved. Events whose bounds touch or span the antimeridian
are retained and flagged for review when assignment uncertainty remains. Valid
antimeridian geometries are not automatically repaired or rejected.

## Outputs

`events_enriched/` contains one terminal row per accepted input event with
primary country, U.S. intersection, primary state, review flags, boundary
versions, and EPSG:6933 area fields.

`event_country_membership/` contains one row per event-country relationship.

`event_state_overlaps/` contains one row per event-state relationship.

`us_events/` contains the reproducible U.S.-intersecting subset.

The command also writes `spatial_enrichment_summary.json`,
`country_assignment_quality.csv`, `state_assignment_quality.csv`,
`boundary_manifest.json`, and `manifest.json`.

## Reproduction

```powershell
uv run geodemand boundaries inspect --boundary-root C:\Work\Data\GeoDemand\boundaries
uv run geodemand boundaries prepare --boundary-root C:\Work\Data\GeoDemand\boundaries --output-dir C:\Work\Data\GeoDemand\artifacts\boundaries_prepared
uv run geodemand data enrich-spatial --input C:\Work\Data\GeoDemand\groundsource\groundsource_2026.parquet --countries C:\Work\Data\GeoDemand\artifacts\boundaries_prepared\countries.parquet --states C:\Work\Data\GeoDemand\artifacts\boundaries_prepared\us_states.parquet --output-dir C:\Work\Data\GeoDemand\artifacts\groundsource_spatial
```

Use `--batch-size`, `--max-rows`, and `--max-row-groups` for bounded smoke
runs. Bounded runs compute metrics only over processed rows.

## Limitations

Political boundaries are approximate research references, not authoritative
legal determinations for every flood footprint. Offshore polygons,
cross-border polygons, boundary-touching polygons, and antimeridian records may
require manual review before downstream Google Trends region matching.
