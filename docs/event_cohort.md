# Candidate Event Cohort

Milestone 0.6A builds a reproducible candidate U.S. flood-event cohort from
spatially enriched Groundsource records. These records are candidates, not
confirmed flash-flood events. Physical verification, MRMS ingestion, search
demand analysis, clustering, and modeling happen later.

## Inputs

The cohort builder consumes corrected spatial-enrichment outputs:

- `events_enriched/`
- `event_state_overlaps/`
- `spatial_enrichment_summary.json`
- `boundary_manifest.json`
- `manifest.json`

The event input must contain U.S. state-union intersection fields and preserved
WKB geometry.

## Study Window

The default primary study window is 2022-01-01 through 2025-12-31, inclusive.
Both dates are configurable with `--start-date` and `--end-date`.

The provisional temporal split is deterministic:

- development: 2022-01-01 through 2023-12-31
- validation: 2024-01-01 through 2024-12-31
- test: 2025-01-01 through 2025-12-31

No random train-test split is performed.

## Domains

The primary domain is `conus`, defined as the contiguous 48 states plus
Washington, DC. Secondary domains are preserved separately:

- `alaska`
- `hawaii`
- `puerto_rico`
- `other_us_territory`
- `multi_domain`
- `unclassified`

Secondary-domain records are not dropped. They are terminal rows in
`secondary_domain_records/` and are represented in exclusion accounting as
`outside_primary_domain`.

## Eligibility

A record is eligible for the primary cohort when:

- `start_date` is present and parseable.
- `start_date` falls inside the configured window.
- `intersects_us_state_union` is true.
- `primary_state_code` is present.
- `study_domain` equals the configured primary domain, `conus` by default.
- event duration is not negative when an end date is available.

Country-unassigned, multistate, spatial-review, and antimeridian records are
not automatically excluded if they otherwise meet the criteria. Review flags
are preserved.

## Outputs

The builder writes:

- `event_cohort/`
- `eligible_event_records/`
- `secondary_domain_records/`
- `excluded_event_records/`
- `event_state_records/`
- `cohort_summary.json`
- `exclusion_reasons.csv`
- `state_year_counts.csv`
- `temporal_quality.csv`
- `overlap_diagnostics.csv`
- `manifest.json`

`event_record_id` is the source UUID. This milestone does not create a final
independent episode identifier.

## Row Accounting

The terminal accounting is:

```text
source U.S.-intersecting rows =
eligible primary rows + secondary-domain rows + other excluded rows
```

Secondary-domain records are not double-counted as other excluded rows.

## Temporal Quality

The cohort profile reports records by year/month, missing end dates, end dates
before start dates, same-day records, duration thresholds, and duration
quantiles. Event intervals are not expanded into daily rows.

## Overlap Diagnostics

Milestone 0.6A does not merge or cluster records. It reports possible duplicate
or related candidate records using bounded candidate generation:

- shared state
- year/date buckets
- STRtree spatial candidate search

Distances and areas are computed in EPSG:6933, not EPSG:4326. Diagnostics
report temporal rules, spatial rules, candidate-pair counts, and participating
record counts.

## Reproduction

```powershell
uv run geodemand cohort inspect --events C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\events_enriched --state-overlaps C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\event_state_overlaps
uv run geodemand cohort build --events C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\events_enriched --state-overlaps C:\Work\Data\GeoDemand\artifacts\spatial_enrichment_full\event_state_overlaps --output-dir C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort --start-date 2022-01-01 --end-date 2025-12-31 --primary-domain conus
```

Use `--max-rows` for bounded smoke runs. Scientific summaries are deterministic;
runtime metadata such as timestamps, processing seconds, throughput, and output
paths live in `manifest.json`.

## Limitations

The cohort is analysis-ready for downstream verification, but the records are
not yet confirmed events. No final clustering, urban classification, physical
precipitation verification, demographic feature generation, Google Trends
collection, or machine-learning modeling is performed in this milestone.
