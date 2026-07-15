# Candidate Episode Clustering

Milestone 0.6B builds provisional candidate flood episodes from the eligible
primary cohort. These episodes are sensitivity-analysis artifacts, not confirmed
flash-flood events.

## Inputs

The clustering commands consume:

- `eligible_event_records/`
- `event_state_records/`

from the candidate cohort output. Geometry must be WKB GeoParquet in EPSG:4326.
Area, intersection, IoU, distance, and STRtree candidate search use EPSG:6933.

## Policies

- `conservative`: event intervals overlap and geometries intersect.
- `balanced`: event date gap is at most 1 day and at least one spatial rule
  passes: geometry intersection, IoU >= 0.10, or representative-point distance
  <= 10 km.
- `broad`: event date gap is at most 2 days and at least one spatial rule
  passes: geometry intersection, IoU >= 0.05, or representative-point distance
  <= 25 km.

## Candidate Generation

The implementation avoids all-pairs comparison. It groups records by shared
state and neighboring start years, then uses Shapely STRtree distance queries
against projected EPSG:6933 geometries. Accepted event pairs are deduplicated
globally by sorted event IDs, so pairs discovered through multiple states appear
once with all shared state codes preserved.

## Episode Construction

Episodes are connected components of accepted edges. Singleton episodes are
valid. Episode identifiers are deterministic SHA-256 hashes of the policy
version, policy name, and sorted member event IDs.

The episode temporal split is assigned from `episode_start_date`:

- development: years through 2023
- validation: 2024
- test: 2025 and later

Episodes containing members from more than one candidate split are flagged in
`split_boundary_diagnostics.csv`.

## Outputs

Each `geodemand episodes build` run writes:

- `episodes_<policy>/episodes/`
- `episodes_<policy>/episode_membership/`
- `episodes_<policy>/episode_edges/`
- `episodes_<policy>/episode_summary.json`
- `episodes_<policy>/episode_size_distribution.csv`
- `episodes_<policy>/state_year_episode_counts.csv`
- `episodes_<policy>/bridge_diagnostics.csv`
- `episodes_<policy>/split_boundary_diagnostics.csv`
- `episodes_<policy>/manifest.json`

`geodemand episodes compare` writes two CSV files:

- `clustering_sensitivity.csv`: policy-level episode statistics only.
- `clustering_agreement.csv`: adjacent narrower-to-broader policy comparisons.

`clustering_agreement.csv` reports narrower episodes preserved exactly,
narrower episodes merged into larger broader episodes, apparent splits relative
to the broader policy, records retaining identical complete episode membership,
and `pairwise_membership_agreement`. The pairwise agreement statistic is the
fraction of event pairs whose same-episode or different-episode relationship is
unchanged between two policies. It is not adjusted Rand index.

## Reproduction

```powershell
uv run geodemand episodes inspect --events C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\eligible_event_records --event-states C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\event_state_records
uv run geodemand episodes build --events C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\eligible_event_records --event-states C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\event_state_records --output-dir C:\Work\Data\GeoDemand\artifacts\candidate_episodes --policy balanced
uv run geodemand episodes compare --events C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\eligible_event_records --event-states C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\event_state_records --output-dir C:\Work\Data\GeoDemand\artifacts\candidate_episode_sensitivity
```

Use `--max-rows` and `--batch-size` for bounded smoke runs. Generated episode
outputs should remain outside version control.

## Limitations

Connected components can chain events across accepted pair edges. Bridge,
duration, size, distance, state-count, and split-boundary diagnostics are
therefore review signals, not automatic rejection rules. Final episode
confirmation requires later MRMS or related physical validation.

## Catalog Use

Milestone 0.7C uses conservative episodes as base units and balanced components
only to identify possible undermerges. Broad components remain sensitivity
diagnostics. Clustering-only indicators create manual-review records and cannot
automatically split or merge membership.
