# Architecture

## Milestone Boundary

Milestone 0.5 builds the reproducible foundation plus real Groundsource schema
discovery and data auditing. It does not include NOAA ingestion, Google Trends
collection, machine-learning models, event clustering, maps, feature
engineering, or forecast evaluation.

## Source Layout

The project uses a `src/` layout so tests import the installed package rather
than accidentally importing files from the repository root. This reduces
packaging surprises when the project later gains workflows, notebooks, and
research scripts.

## Dependency Management

Dependencies live in `pyproject.toml` and are compatible with `uv`. Runtime
dependencies include Typer, Pydantic, Polars, PyArrow, and Shapely. PyArrow is
used for Parquet metadata and record-batch scanning; Shapely is used for WKB
geometry decoding and representative-point extraction.

## CLI

Typer provides the command-line interface because it gives typed command
parameters, useful help output, and a small surface area. The CLI delegates to
library functions in `geodemand.ingestion.groundsource` so ingestion logic is
testable without shelling out.

## Data Contracts

Pydantic models define stable research-facing records for flood events,
geographic regions, search-interest observations, and event-region
intersections. Raw Groundsource records are represented separately from
canonical GeoDemand flood-event records because source columns may drift while
internal research contracts should remain stable.

## Groundsource Ingestion

Groundsource ingestion accepts only caller-provided local Parquet paths. The
confirmed source schema is GeoParquet 0.4.0 with `uuid`, `area_km2`, `geometry`,
`start_date`, `end_date`, and ignored `__index_level_0__`. It does not provide
source country, latitude, or longitude fields.

Inspection reads raw Arrow schema and Parquet key-value metadata before
validation. Auditing and canonical output scan record batches with PyArrow so
the 2.6 million source geometries do not need to fit into memory as a
GeoDataFrame. WKB is decoded in bounded batches with Shapely.

## Output Format

Canonical Groundsource output preserves original WKB geometry and GeoParquet
metadata. Representative coordinates are produced from
`representative_point().x` and `.y`; centroid is intentionally not used.
Country filtering requires a boundary dataset and later spatial enrichment.

## Profiling

The audit creates deterministic JSON and CSV outputs for schema, profile, field
quality, temporal coverage, geometry quality, and rejected-record summaries. The
manifest intentionally includes a UTC timestamp, checksum, file size, package
version, configuration, and row counts for reproducibility.

## Logging And Errors

The package uses standard-library structured JSON logging. User-facing errors
raise specific exceptions with actionable messages, and the CLI converts them
to non-zero exits without tracebacks for expected validation failures.

## Quality Gates

Ruff, MyPy, Pytest, pre-commit, and GitHub Actions are configured at project
start. This keeps formatting, linting, type checking, and unit tests part of the
research workflow before model code exists.
