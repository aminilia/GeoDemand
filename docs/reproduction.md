# Reproduction

GeoDemand-FF separates source code, local research data, generated artifacts, and raw
Google Trends exports. The repository should remain source-only.

## Milestone 1.0C-2 planning-only reproduction

Generate a cohort with `trends pilot-sample --episode-limit 10 --selection-version
1.0C-v1`, then run `trends map-geographies`. Generate the authoritative pilot plan with
`trends plan`, the existing control artifact, three repeatable `--batch-id` options,
`--planned-repeats 2`, `--execution-batch-id 1.0C-pilot-v1`,
`--controls-per-episode 1`, and `--execution-only`. For the 20-episode full-core plan,
select 20 episodes and request only `batch_1_flood_awareness` and
`batch_5_specific_recovery`.

Repeat these commands in separate empty roots to verify byte-identical selection, plan, and
summary files. They do not open a browser, fetch Trends data, import CSVs, or run
statistical validation.

## Environment

```bash
uv sync --locked --extra dev
uv run python -m ruff check .
uv run python -m ruff format --check .
uv run python -m mypy
uv run python scripts/run_tests.py
uv run python scripts/validate_documented_cli.py
```

Browser-assisted Google Trends export requires the optional browser extra:

```bash
uv sync --extra trends-browser
```

## Groundsource-To-Catalog

```bash
geodemand data inspect-groundsource --input PATH
geodemand data audit-groundsource --input PATH --output-dir PATH
geodemand boundaries prepare --boundary-root PATH --output-dir PATH
geodemand data enrich-spatial --input PATH --countries PATH --states PATH --output-dir PATH
geodemand cohort build --events PATH --state-overlaps PATH --output-dir PATH
geodemand episodes build --events PATH --event-states PATH --output-dir PATH --policy conservative
geodemand episodes compare --events PATH --event-states PATH --output-dir PATH
geodemand catalog build-provisional --cohort-root PATH --episode-root PATH --comparison-root PATH --rules config/catalog_rules.yaml --output-dir PATH
```

## Trends Event Study

```bash
geodemand trends pilot-sample --catalog-dir PATH --output-root PATH --rules config/trends_rules.yaml
geodemand trends map-geographies --pilot PATH --output-root PATH
geodemand trends plan --pilot PATH --geography PATH --terms config/trends_terms.yaml --rules config/trends_rules.yaml --output-root PATH --include-national
geodemand trends import-csv --csv PATH --sidecar PATH --output-root PATH
geodemand trends export-pytrends --plan PATH --output-root PATH
geodemand trends acquisition-inventory --data-root PATH --output-root PATH
geodemand trends acquisition-reproducibility --manual-root PATH --pytrends-root PATH --output-root PATH
geodemand trends validate-imports --observations PATH --plan PATH
geodemand trends metrics --observations PATH --plan PATH --rules config/trends_rules.yaml --output-root PATH
geodemand trends phase-metrics --observations PATH --plan PATH --terms config/trends_terms.yaml --rules config/trends_rules.yaml --output-root PATH
geodemand trends concurrence --observations PATH --plan PATH --terms config/trends_terms.yaml --rules config/trends_rules.yaml --output-root PATH
geodemand trends control-adjusted-metrics --phase-metrics PATH --controls PATH --output-root PATH
geodemand trends attribute-peaks --phase-metrics PATH --concurrence PATH --rules config/trends_rules.yaml --output-root PATH
geodemand analysis build-dataset --request-plan PATH --episode-metrics PATH --repeat-metrics PATH --phase-metrics PATH --output-root PATH
geodemand analysis describe-signal --dataset PATH --output-root PATH
```

The analysis build follows both `trends metrics` and `trends phase-metrics` and writes
only under `--output-root`. Its integrated Parquet table has one row per `request_id`,
`repeat_id`, and `concept_id`. Input schemas, planned concepts, and lineage values are
validated before output; missing phase-base joins and reverse orphan inputs are recorded
in deterministic join-summary and unmatched-key artifacts. Planned concepts are checked
individually for each applicable repeat, including repeats governed by a request-level
fallback plan. Empty upstream phase and integrated outputs preserve the same explicit
Arrow schemas as populated runs. The table is prepared for a later statistical-validation
milestone but performs no inferential analysis itself.

Run `analysis describe-signal` after `analysis build-dataset`. The input must be `.parquet`
or `.pq` with the analytical grain `request_id + repeat_id + concept_id` and the required
episode, geography, semantic-family, and request-role lineage. The command writes ten files:
seven typed Parquet tables, `descriptive_exclusions.csv`, `descriptive_summary.json`, and
`provenance.json`. Repeating a run with the same input and software produces byte-identical
outputs. Principal summaries first average finite repeats within `request_id + concept_id`,
then give each request/concept unit equal weight. Their schemas separately report raw
repeat rows, valid repeat values, request/concept units, and independent requests. Paired
and rank diagnostics require exactly two exports; groups with more than two are reported as
multi-repeat exclusions. The summaries are descriptive, and no
bootstrap, permutation, ICC, p-value, confidence-interval, or model output is produced.

## Synthetic Demonstration

```bash
uv run python scripts/synthetic_demo.py --output-dir demo-output
```

Repeated runs in the same locked execution environment produce identical scientific
artifacts and manifests. Cross-platform raw Parquet byte identity is not claimed; table
content hashes are used for portable comparisons.

## Data Handling

Do not commit raw Groundsource data, raw Google Trends exports, full generated audit
artifacts, canonical Parquet outputs, quarantine outputs, or rejected-record outputs.

## Final analysis

Use the exact `analysis finalize` contract in [analysis_1_1.md](analysis_1_1.md).
PyTrends export now fails closed; prior command examples are historical, not part of 1.1.
On deep Windows paths pass a short disposable `--basetemp` to scripts/run_tests.py.
