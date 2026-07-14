# GeoDemand-FF

GeoDemand-FF is a reproducible research scaffold for studying regional Google
search-demand surges following urban flash floods.

Milestone 0.6A provides package structure, data contracts, local GeoParquet
Groundsource inspection, WKB geometry auditing, spatial enrichment, candidate
U.S. event cohort construction, overlap diagnostics, tests, and project
documentation.
It does not implement NOAA ingestion, Google Trends ingestion, machine-learning
models, event clustering, or maps.

## Installation

```powershell
uv venv
uv pip install -e ".[dev]"
```

## Tests And Checks

```powershell
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/run_tests.py
```

## First Data Audit

Use a local Parquet file. Do not commit raw or downloaded research data.

```powershell
uv run geodemand data inspect-groundsource --input PATH
uv run geodemand data audit-groundsource --input PATH --output-dir AUDIT_DIR
uv run geodemand data validate-groundsource --input PATH
uv run geodemand data profile --input PATH --output REPORT_JSON
uv run geodemand data filter-groundsource --input PATH --output OUTPUT_DIR --country US --country-boundaries BOUNDARIES
```

## Boundary Preparation And Spatial Enrichment

Milestone 0.5E prepares local Natural Earth and Census boundary files, then
streams Groundsource events through country and U.S. state spatial assignment.
Use local archives; do not commit boundary archives or prepared outputs.

```powershell
uv run geodemand boundaries inspect --boundary-root C:\Work\Data\GeoDemand\boundaries
uv run geodemand boundaries prepare --boundary-root C:\Work\Data\GeoDemand\boundaries --output-dir C:\Work\Data\GeoDemand\artifacts\boundaries_prepared
uv run geodemand data enrich-spatial --input C:\Work\Data\GeoDemand\groundsource\groundsource_2026.parquet --countries C:\Work\Data\GeoDemand\artifacts\boundaries_prepared\countries.parquet --states C:\Work\Data\GeoDemand\artifacts\boundaries_prepared\us_states.parquet --output-dir C:\Work\Data\GeoDemand\artifacts\groundsource_spatial
```

The enrichment command writes `events_enriched/`,
`event_country_membership/`, `event_state_overlaps/`, `us_events/`,
`spatial_enrichment_summary.json`, assignment-quality CSVs, boundary metadata,
and a run manifest.

## Candidate Event Cohort

Milestone 0.6A builds candidate flood-event cohorts from corrected spatial
enrichment outputs. Records are candidate events, not confirmed flash-flood
events.

```powershell
uv run geodemand cohort inspect --events C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\events_enriched --state-overlaps C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\event_state_overlaps
uv run geodemand cohort build --events C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\events_enriched --state-overlaps C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\event_state_overlaps --output-dir C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort --start-date 2022-01-01 --end-date 2025-12-31 --primary-domain conus
```

The default primary domain is the contiguous 48 states plus Washington, DC.
Alaska, Hawaii, Puerto Rico, and other U.S. territories are preserved as
secondary-domain records.

## Groundsource Required Fields

The confirmed Groundsource source is GeoParquet 0.4.0 with this schema:

- `uuid`: string, required
- `area_km2`: double, optional or nullable
- `geometry`: binary WKB, required
- `start_date`: string, required
- `end_date`: string, optional or nullable
- `__index_level_0__`: int64, ignored pandas index artifact

The source does not provide country, state, latitude, or longitude columns.
Representative coordinates are derived from WKB geometry with
`representative_point()`, not centroid. Country filtering requires an explicit
boundary dataset because country assignment is a later spatial-enrichment step.

Spatial enrichment assigns countries with representative-point coverage,
maximum-overlap fallback, single-intersection fallback, and explicit
manual-review flags. U.S. state assignment runs only for events whose complete
geometry intersects the United States boundary and ranks states by EPSG:6933
overlap area.

## Milestone 0.5D Results

The full Groundsource audit for `groundsource_2026.parquet` verified SHA-256
`77c266ba5a5176d983edca989a81ff73f21c556fd98c82e2131c2f8d172546ce` and
processed 2,646,302 rows. Row accounting was balanced: 2,646,302 accepted
canonical rows, 0 quarantined rows, and 0 rejected rows.

All required and optional source fields had zero nulls. The dataset contained
2,478,877 `Polygon` records and 167,425 `MultiPolygon` records; all geometries
were valid, decodable, non-empty, and supported. The audit flagged 300 records
for antimeridian review. Temporal coverage spans 2000-01-01 through
2026-02-03, and no duplicate UUID groups were found.

## Repository Policy

This repository intentionally excludes research data and local outputs. Store
raw data under ignored folders such as `data/raw/` or outside the repository.
Keep credentials in local environment files only; this milestone does not need
credentials.
