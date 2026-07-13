# GeoDemand-FF

GeoDemand-FF is a reproducible research scaffold for studying regional Google
search-demand surges following urban flash floods.

Milestone 0.5 provides package structure, data contracts, local GeoParquet
Groundsource inspection, WKB geometry auditing, tests, and project documentation.
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

## Repository Policy

This repository intentionally excludes research data and local outputs. Store
raw data under ignored folders such as `data/raw/` or outside the repository.
Keep credentials in local environment files only; this milestone does not need
credentials.
