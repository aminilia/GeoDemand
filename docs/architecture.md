# Architecture

## Current Boundary

The implemented system covers Groundsource schema discovery and bounded audit,
global/U.S. spatial enrichment, candidate U.S. cohort construction, nested episode
clustering, a provisional catalog, and a non-causal Google Trends event-study workflow.
It does not implement real MRMS or IMERG episode extraction, machine-learning models,
final event confirmation, universal urban classification, or forecast evaluation.

## Source Layout

The project uses a `src/` layout so tests import the installed package rather
than accidentally importing files from the repository root. This reduces
packaging surprises when the project later gains workflows, notebooks, and
research scripts.

## Dependency Management

Dependencies live in `pyproject.toml` and are compatible with `uv`. Runtime
dependencies include Typer, Pydantic, PyArrow, Shapely, PyProj, and
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

`geodemand.schemas` is the central versioned PyArrow schema registry for scientific
tables. It defines required fields and types without imposing per-row object validation
on multimillion-row pipelines. Physical evidence additionally requires explicit origin,
dataset, product, manifest, decoder, commit, and rule provenance. Pydantic is restricted
to control-plane configuration and Trends sidecar validation; it does not govern bulk
scientific rows.

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

## Candidate Cohort

The cohort builder consumes spatial-enrichment outputs rather than raw
Groundsource. It filters to U.S. state-union-intersecting records, classifies
study domains with explicit state-code sets, and writes candidate flood-event
records for the configured temporal window. The default primary domain is
CONUS plus Washington, DC; Alaska, Hawaii, Puerto Rico, and other U.S.
territories are preserved separately.

The builder uses source UUIDs as `event_record_id` and intentionally does not
create final independent episode IDs. It writes scoped state/year counts with
terminal category, study domain, primary exclusion reason, and an explicit
`not_applicable` candidate split for noneligible records. Temporal quality is
reported separately for all U.S.-intersecting source rows and for the eligible
primary cohort.

## Candidate Episode Clustering

Episode clustering consumes `eligible_event_records/` and `event_state_records/`
from the cohort builder. Three deterministic policies are supported:

- `conservative`: intervals overlap and geometries intersect.
- `balanced`: date gap is at most 1 day and records intersect, have IoU at
  least 0.10, or representative-point distance is at most 10 km.
- `broad`: date gap is at most 2 days and records intersect, have IoU at least
  0.05, or representative-point distance is at most 25 km.

Candidate generation uses temporal/state buckets and Shapely STRtree queries in
EPSG:6933 rather than all-pairs comparison. Accepted edges are deduplicated
globally by sorted event IDs, including pairs discovered through multiple
states. Episodes are connected components; singleton episodes are valid.

`episode_id` is deterministic: SHA-256 over the policy version, policy name, and
sorted member event IDs. Episodes inherit a deterministic temporal split from
episode start date, and split-boundary diagnostics flag components containing
members from multiple cohort splits. Runtime metadata is written to manifests;
scientific summaries use deterministic JSON ordering.

## MRMS Feasibility

MRMS support is implemented as an offline-testable subsystem with lazy optional
imports for `s3fs`, `xarray`, `cfgrib`, and `eccodes`. The ordinary test suite
uses mocked S3 listings and tiny synthetic metric fixtures; real NOAA access is
kept opt-in.

The subsystem separates deterministic sample selection, S3 inventory, and compressed
file caching. It inventories object keys before downloading, globally deduplicates
objects, validates cache files, and keeps volatile execution details in manifests.
Real GRIB decoding and episode extraction are pending; the production extraction command
fails explicitly and cannot fabricate metrics. Assessment can consume only separately
supplied observed evidence with complete provenance.

## Multi-Source Verification

NASA IMERG and USGS are separate offline-testable adapters. The implemented IMERG
inventory/fetch scaffold documents the required rate-to-half-hour accumulation
conversion, but real HDF5 geometry-aware extraction is pending and fails explicitly.
USGS provides nearby gauge observations as contextual response evidence, not direct
precipitation measurements or proof of hydrologic connectivity.

The integrated assessment combines component evidence through explicit
versioned rules written to `assessment_rules.json`. No gauge is represented as
unknown evidence rather than negative evidence, and no automated event relabeling
is performed.

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

## Provisional Catalog Layer

The catalog layer reads, but never overwrites, cohort and episode-policy
artifacts. Conservative episodes provide base membership; balanced membership
is joined as relationship evidence. Optional MRMS and USGS adapters are indexed
by source conservative episode ID. Deterministic rules emit catalog, evidence,
review, decision, split, summary, and manifest artifacts.

This separation prevents absent physical observations from becoming negative
labels and prevents sensitivity policies from silently rewriting analysis
units. Stable content-derived IDs and post-build validation enforce record
accounting and split isolation.

## Trends Feasibility Layer

The Trends layer separates request planning, immutable raw exports, canonical
observations, normalization diagnostics, response metrics, terminology
decisions, and episode feasibility. Backend identifiers share request and
observation contracts, so manual official CSV ingestion remains functional when
the official alpha API is unavailable. Volatile import time belongs in the
manifest; scientific tables use sidecar export dates and deterministic ordering.

Event-study analysis is isolated in `trends_event_study.py`. It consumes
immutable observations and explicit request plans, then writes phase,
concurrence, control-adjusted, attribution, and diagnostic outputs. Request
role, geography, geography level, batch, episode, concept, and terminology
version remain explicit at every stage. Cross-geography arithmetic is routed
through a scale validator that permits only within-series baseline-standardized
lifts; raw request-relative indices cannot enter state-minus-national or
treated-minus-control calculations.

The optional browser export adapter is isolated in `trends_browser.py`. It
consumes an existing plan, verifies one official Explore page, stages a download
atomically, validates it with the shared CSV parser, and writes an operational
Parquet manifest plus a browser lineage sidecar. Playwright loads dynamically,
so core and offline test installations do not require a browser runtime.
Selectors are centralized behind a session protocol; ordinary tests substitute
deterministic sessions and synthetic downloads.

The adapter is transport-only. It does not normalize values, interpret
interest, mutate downloaded bytes, or bypass `import-csv`. Volatile attempt
metadata stays in the manifest. Credentials, cookies, profile contents, and
account identifiers are outside the data model.

Control selection indexes provisional episode intervals by state before
applying the configured temporal buffer. This avoids repeated full-catalog
scans while retaining deterministic ranking. Population tier, broad climate class,
coastal status, and Trends-availability fields are versioned in
`data/reference/us_state_matching_metadata.csv`. Matched status requires metadata-backed
comparison; missing metadata is labeled `unmatched_fallback_control` rather than being
presented as a match.
