# Architecture

## Milestone Boundary

Milestone 0.5 builds the reproducible foundation plus real Groundsource schema
discovery, data auditing, and global/U.S. spatial enrichment. It does not
include NOAA ingestion, Google Trends collection, machine-learning models,
event clustering, maps, feature engineering, or forecast evaluation.

## Source Layout

The project uses a `src/` layout so tests import the installed package rather
than accidentally importing files from the repository root. This reduces
packaging surprises when the project later gains workflows, notebooks, and
research scripts.

## Dependency Management

Dependencies live in `pyproject.toml` and are compatible with `uv`. Runtime
dependencies include Typer, Pydantic, Polars, PyArrow, Shapely, PyProj, and
Pyogrio. PyArrow is used for Parquet metadata and record-batch scanning; Shapely
is used for WKB geometry decoding, representative-point extraction, STRtree
queries, and topology operations. PyProj provides EPSG:6933 area calculations,
and Pyogrio reads the small Natural Earth and Census boundary archives.

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

## Boundary Preparation

Boundary preparation is separate from Groundsource ingestion because boundary
files are small, versioned reference datasets while Groundsource is the large
streamed event source. Natural Earth Admin 0 Countries 5.1.1 and Census
TIGER/Line 2025 states are validated, normalized to EPSG:4326, and written as
deterministic GeoParquet. The boundary manifest records source hashes, output
hashes, feature counts, CRS, geometry types, code fields, source versions,
Natural Earth `de_facto` worldview, and Census legal-boundary vintage.

## Spatial Enrichment

Spatial enrichment loads only the prepared boundary datasets into memory. The
2.6 million Groundsource rows are processed in bounded PyArrow record batches.
Each event retains original WKB geometry and receives country, U.S.
intersection, and state-overlap attributes.

Country assignment first uses `representative_point()` coverage. Ambiguous or
cross-border events use EPSG:6933 maximum-overlap ranking; offshore events with
a single country intersection use `single_intersection`; events with no country
intersection remain enriched with null country fields. State assignment runs
only for events whose complete geometry intersects the United States boundary.
All intersecting state overlaps are preserved, and the primary state is the
largest EPSG:6933 overlap with deterministic tie breaking.

Antimeridian, offshore, cross-border, multistate, and manual-review records are
not rejected by spatial enrichment. They remain terminal enriched rows with
review flags.

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
