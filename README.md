# GeoDemand-FF

GeoDemand-FF is a reproducible research scaffold for studying regional Google
search-demand surges following urban flash floods.

Milestone 0.6B provides package structure, data contracts, local GeoParquet
Groundsource inspection, WKB geometry auditing, spatial enrichment, candidate
U.S. event cohort construction, candidate episode clustering, sensitivity
analysis, tests, and project documentation.
It does not implement Google Trends ingestion, machine-learning models, national
MRMS extraction, urban exposure, final event confirmation, or maps.

## Installation

```powershell
uv venv
uv pip install -e ".[dev]"
```

Source-specific clients are optional so ordinary offline development does not
install network and native decoding stacks unnecessarily:

```powershell
uv pip install -e ".[mrms]"
uv pip install -e ".[imerg]"
uv pip install -e ".[usgs]"
uv pip install -e ".[verification]"
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

## Candidate Episodes

Milestone 0.6B clusters eligible candidate records into provisional candidate
episodes. These clusters are for sensitivity analysis and downstream
verification; they are not confirmed flood episodes.

```powershell
uv run geodemand episodes inspect --events C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\eligible_event_records --event-states C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\event_state_records
uv run geodemand episodes build --events C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\eligible_event_records --event-states C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\event_state_records --output-dir C:\Work\Data\GeoDemand\artifacts\candidate_episodes --policy balanced
uv run geodemand episodes compare --events C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\eligible_event_records --event-states C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\event_state_records --output-dir C:\Work\Data\GeoDemand\artifacts\candidate_episode_sensitivity
```

Supported policies are `conservative`, `balanced`, and `broad`. Use
`--max-rows` for bounded smoke runs; full default runs process the complete
eligible cohort.

## MRMS Feasibility

Milestone 0.7A adds targeted MRMS feasibility commands. The default policy for
physical verification is conservative; balanced is used to inspect possible
merge candidates, and broad remains an upper sensitivity scenario.

```powershell
uv run geodemand mrms selfcheck --working-root C:\Work\Data\GeoDemand\mrms
uv run geodemand mrms inspect-episodes --episode-root C:\Work\Data\GeoDemand\artifacts\episode_comparison_full
uv run geodemand mrms sample --episode-root EPISODE_OUTPUT_ROOT --output-root C:\Work\Data\GeoDemand\mrms
uv run geodemand mrms inventory --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --output-root C:\Work\Data\GeoDemand\mrms --max-episodes 3
uv run geodemand mrms fetch --download-plan C:\Work\Data\GeoDemand\mrms\inventory\mrms_download_plan.csv --working-root C:\Work\Data\GeoDemand\mrms --max-episodes 5
uv run geodemand mrms extract --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --file-manifest C:\Work\Data\GeoDemand\mrms\manifests\mrms_file_manifest.parquet --output-root C:\Work\Data\GeoDemand\mrms
uv run geodemand mrms assess --metrics-root C:\Work\Data\GeoDemand\mrms\metrics
```

MRMS downloads are cached locally and must not be committed.

## Multi-Source Verification

Milestone 0.7B adds offline-testable NASA IMERG, USGS, and integrated
multi-source commands. These commands produce feasibility evidence and review
categories only; they do not relabel events.

The deterministic pilot selector uses quota strata followed by a seeded
coverage fill for years, seasons, states, and regions. Network clients and
native decoders are installed through the optional `imerg`, `usgs`, and
`verification` dependency groups. A real-data pilot remains gated on a populated
episode-policy output and MRMS verification sample.

```powershell
uv run geodemand imerg selfcheck --working-root C:\Work\Data\GeoDemand\imerg
uv run geodemand usgs selfcheck --working-root C:\Work\Data\GeoDemand\usgs
uv run geodemand observations pilot-sample --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --output-dir C:\Work\Data\GeoDemand\multisource_verification
uv run geodemand observations compare-precipitation --mrms-metrics MRMS_METRICS --mrms-timeseries MRMS_TIMESERIES --imerg-metrics IMERG_METRICS --imerg-timeseries IMERG_TIMESERIES --sample PILOT_SAMPLE --output-dir C:\Work\Data\GeoDemand\multisource_verification
uv run geodemand observations assess --sample PILOT_SAMPLE --mrms-metrics MRMS_METRICS --imerg-metrics IMERG_METRICS --precipitation-comparison PRECIP_COMPARISON --usgs-summary USGS_SUMMARY --output-dir C:\Work\Data\GeoDemand\multisource_verification
```

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

## Provisional Episode Catalog

Milestone 0.7C builds a deterministic provisional physically informed episode
catalog from conservative episode membership. Balanced relationships identify
possible undermerges; clustering diagnostics identify possible overmerges.
Neither changes membership without adequate physical evidence and review.
Missing MRMS is pending, no suitable USGS gauge is unknown, and IMERG is
deferred. See [the catalog guide](docs/provisional_episode_catalog.md).

```powershell
uv run geodemand catalog inspect --cohort-root C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full --episode-root C:\Work\Data\GeoDemand\artifacts\episode_policy_full --comparison-root C:\Work\Data\GeoDemand\artifacts\episode_comparison_full --mrms-root C:\Work\Data\GeoDemand\mrms --usgs-root C:\Work\Data\GeoDemand\usgs
uv run geodemand catalog build-provisional --cohort-root C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full --episode-root C:\Work\Data\GeoDemand\artifacts\episode_policy_full --comparison-root C:\Work\Data\GeoDemand\artifacts\episode_comparison_full --mrms-root C:\Work\Data\GeoDemand\mrms --usgs-root C:\Work\Data\GeoDemand\usgs --rules config\catalog_rules.yaml --output-dir C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog
uv run geodemand catalog validate --catalog-dir C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog
```

The full 0.7C run produced 37,053 provisional episodes and 88,515 unique
membership rows with zero split leakage. It retained 19,030 episodes
provisionally and queued 18,023 for review. MRMS episode evidence remains
pending, so no physical merge, split, or support status was assigned. Two full
builds matched across every generated file.
