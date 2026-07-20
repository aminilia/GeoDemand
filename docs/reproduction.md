# Reproduction

All paths are explicit. Raw research data and generated full-size artifacts are not
part of the repository.

## Quality Gate

```powershell
python -m ruff check .
python -m ruff format --check .
python -m mypy src
python scripts/run_tests.py
python -m build
python scripts/validate_documented_cli.py
```

## Groundsource Audit

```powershell
geodemand data inspect-groundsource --input GROUND_SOURCE_PARQUET
geodemand data audit-groundsource --input GROUND_SOURCE_PARQUET --output-dir AUDIT_DIR
geodemand data validate-groundsource --input GROUND_SOURCE_PARQUET
```

Use `--max-rows`, `--max-row-groups`, and `--batch-size` for a bounded smoke audit.
The full default processes all rows.

## Spatial Enrichment, Cohort, And Episodes

```powershell
geodemand boundaries prepare --boundary-root BOUNDARY_ROOT --output-dir PREPARED_BOUNDARIES
geodemand data enrich-spatial --input GROUND_SOURCE_PARQUET --countries COUNTRIES_PARQUET --states STATES_PARQUET --output-dir SPATIAL_OUTPUT
geodemand cohort build --events ENRICHED_EVENTS --state-overlaps STATE_OVERLAPS --output-dir COHORT_OUTPUT --start-date 2022-01-01 --end-date 2025-12-31
geodemand episodes compare --events ELIGIBLE_EVENTS --event-states EVENT_STATES --output-dir EPISODE_COMPARISON
```

## Provisional Catalog

```powershell
geodemand catalog build-provisional --cohort-root COHORT_ROOT --episode-root EPISODE_ROOT --comparison-root COMPARISON_ROOT --rules config/catalog_rules.yaml --output-dir CATALOG_OUTPUT
geodemand catalog validate --catalog-dir CATALOG_OUTPUT
```

Physical evidence is optional. When supplied, it must be observed evidence with the
supported provenance schema. Synthetic fixtures are rejected by production catalog
construction.

## Google Trends Event Study

```powershell
geodemand trends phase-metrics --observations OBSERVATIONS --plan REQUEST_PLAN --terms config/trends_terms.yaml --rules config/trends_rules.yaml --output-root TRENDS_OUTPUT
geodemand trends concurrence --observations OBSERVATIONS --plan REQUEST_PLAN --terms config/trends_terms.yaml --rules config/trends_rules.yaml --phase-metrics PHASE_METRICS --output-root TRENDS_OUTPUT
geodemand trends select-controls --pilot PILOT_EPISODES --catalog PROVISIONAL_CATALOG --rules config/trends_rules.yaml --state-metadata data/reference/us_state_matching_metadata.csv --output-root TRENDS_OUTPUT
geodemand trends control-adjusted-metrics --phase-metrics PHASE_METRICS --controls CONTROL_SELECTION --output-root TRENDS_OUTPUT
geodemand trends attribute-peaks --phase-metrics PHASE_METRICS --concurrence CONCURRENCE --control-adjusted CONTROL_ADJUSTED --rules config/trends_rules.yaml --output-root TRENDS_OUTPUT
```

Run `python scripts/validate_documented_cli.py` after changing any command signature.
