# Data Card

GeoDemand-FF currently uses reported flood footprints, administrative boundaries, and
Google Trends exports.

## Groundsource

The confirmed Groundsource source file is GeoParquet 0.4.0 with WKB `Polygon` and
`MultiPolygon` geometries in EPSG:4326. Required source fields are `uuid`, `geometry`,
and `start_date`; `area_km2` and `end_date` are optional. The pandas index artifact
`__index_level_0__` is ignored.

The audit records schema metadata, row counts, field quality, geometry quality,
temporal coverage, duplicate UUID diagnostics, rejected records, quarantined records,
and deterministic manifests. Invalid decodable geometries are quarantined by default.

## Boundaries

Natural Earth boundaries support country assignment. U.S. Census state and equivalent
boundaries support state assignment and state-union overlap checks. Boundary versions
and file hashes should be recorded in local manifests.

## Candidate Episodes

The cohort and episode catalog are derived from reported footprints after geospatial
audit, enrichment, date filtering, conservative clustering, balanced/broad sensitivity
checks, and manual-review rules. These are candidate reported flood episodes, not final
event labels.

## Google Trends

The primary 1.1 analysis admits only immutable official/manual or browser-assisted
Explore CSV exports with verified sidecars. Historical PyTrends artifacts are excluded;
the unsafe exporter is disabled. Values are request-relative indices. The event study uses within-request
phase metrics, repeat-aware concurrence, national diagnostics, and matched controls.

## Redistribution

The repository does not include downloaded research data or raw Google Trends exports.
See `DATA_LICENSES.md` for active source notes and redistribution constraints.
