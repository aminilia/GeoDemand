# Architecture

## Milestone Boundary

Milestone 0 builds the reproducible foundation only: package structure, data
contracts, local ingestion, CLI commands, documentation, and verification. It
does not include machine-learning models, feature engineering, Google Trends
collection, or forecast evaluation.

## Source Layout

The project uses a `src/` layout so tests import the installed package rather
than accidentally importing files from the repository root. This reduces
packaging surprises when the project later gains workflows, notebooks, and
research scripts.

## Dependency Management

Dependencies live in `pyproject.toml` and are compatible with `uv`. Runtime
dependencies are limited to Typer, Pydantic, and Polars. Development tools are
kept in the `dev` optional dependency group so CI and local development use the
same commands.

## CLI

Typer provides the command-line interface because it gives typed command
parameters, useful help output, and a small surface area. The CLI delegates to
library functions in `geodemand.ingestion.groundsource` so ingestion logic is
testable without shelling out.

## Data Contracts

Pydantic models define stable research-facing records for flood events,
geographic regions, search-interest observations, and event-region
intersections. These contracts are separate from the Groundsource ingestion
schema because external source columns may change while internal research
records should remain stable.

## Groundsource Ingestion

Groundsource ingestion accepts only caller-provided local Parquet paths. It does
not hard-code URLs, paths, credentials, countries, or dates. Polars lazy scans
are used so schema validation, filtering, and output generation can scale to
larger files while keeping small-fixture tests fast.

## Output Format

Filtered Groundsource output is written as partitioned Parquet by `country` and
`event_date`. These partitions support repeatable data audits and future
regional or temporal joins without implying a modeling design.

## Profiling

The data profile report is JSON so it can be reviewed in pull requests, archived
with experiment metadata, or consumed by future automation. It includes schema,
row count, date range, country counts, missing counts, and numeric summaries for
the required severity and coordinate fields.

## Logging And Errors

The package uses standard-library structured JSON logging. User-facing errors
raise specific exceptions with actionable messages, and the CLI converts them
to non-zero exits without tracebacks for expected validation failures.

## Quality Gates

Ruff, MyPy, Pytest, pre-commit, and GitHub Actions are configured at project
start. This keeps formatting, linting, type checking, and unit tests part of the
research workflow before model code exists.
