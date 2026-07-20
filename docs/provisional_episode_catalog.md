# Provisional Reported-Event Catalog

The catalog step converts conservative clustered episodes into deterministic candidate
reported flood episodes for Google Trends event-study planning.

```bash
geodemand catalog inspect --cohort-root PATH --episode-root PATH --comparison-root PATH
geodemand catalog build-provisional --cohort-root PATH --episode-root PATH --comparison-root PATH --rules config/catalog_rules.yaml --output-dir PATH
geodemand catalog review-queue --catalog-dir PATH
geodemand catalog validate --catalog-dir PATH
geodemand catalog summarize --catalog-dir PATH
```

Catalog decisions use:

- conservative episode membership
- balanced and broad clustering sensitivity
- spatial and temporal diagnostics
- episode size and compactness
- split-boundary and geometry-review flags
- deterministic decision history
- manual review requirements

Statuses use reported-event terminology:

- `retained_provisionally`
- `manual_review_required`
- `split_candidate`
- `merge_candidate`
- `insufficient_episode_evidence`
- `excluded_from_analysis`

The catalog preserves stable provisional IDs and exact membership accounting. Manual
review decisions are written as immutable records and validated before use.
