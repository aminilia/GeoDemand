# GeoDemand-FF

GeoDemand-FF is a reproducible research scaffold for studying regional Google
search-demand surges following urban flash floods.

Milestone 0 provides package structure, data contracts, local Groundsource
ingestion, command-line tooling, tests, and project documentation. It does not
implement machine-learning models.

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
uv run pytest
```

## First Data Audit

Use a local Parquet file. Do not commit raw or downloaded research data.

```powershell
uv run geodemand data inspect-groundsource --input PATH
uv run geodemand data profile --input PATH --output REPORT_JSON
uv run geodemand data filter-groundsource --input PATH --output OUTPUT_DIR --country US
```

## Groundsource Required Fields

The local Groundsource Parquet file must contain:

- `event_id`
- `event_date`
- `country`
- `region_id`
- `region_name`
- `latitude`
- `longitude`
- `flood_severity`
- `source`

## Repository Policy

This repository intentionally excludes research data and local outputs. Store
raw data under ignored folders such as `data/raw/` or outside the repository.
Keep credentials in local environment files only; this milestone does not need
credentials.
