# GeoDemand 1.1: frozen final analysis

The final command reuses existing official/manual CSV observations. No acquisition is
performed. The primary cohort is bounded to the existing full-core selection: at most
20 episodes, two panels, treated/control roles and two planned repeats. A complete plan
is mandatory; incomplete observations are retained in eligibility accounting.

```text
uv sync --locked --extra dev
uv run geodemand analysis finalize --catalog PATH --request-plan PATH --observations PATH --raw-root PATH --config config/analysis_1_1.yaml --output-root PATH
```

Supply the catalog `episodes.parquet`, authoritative execution-plan CSV/Parquet, canonical
observations Parquet and the directory containing immutable official CSVs and JSON
sidecars. The output root must be empty and separate from inputs. Paths to terminology,
phase rules and lockfile are resolved relative to the analysis configuration. These files
are distributed in the source release and are explicit inputs to an installed wheel.

## Admission and lineage

The command checks unique plan grain, a many-to-one catalog join, retained catalog status,
episode dates, complete panel/role/repeat design and pair IDs. It verifies raw hashes,
query definitions, backend, episode/geography/repeat lineage, export attempt and exact
observed values/partial flags against immutable CSVs. A numeric category of zero is valid.
Daily data are required; weekly/monthly series are rejected. Missing raw files, absent
hashes or unknown retrieval dates are excluded explicitly. Hash/value/lineage conflicts
fail before writing outputs. Unknown retrieval timestamps are never inferred from mtime.

Manual and browser-assisted official Explore exports are allowed acquisition labels.
PyTrends export is disabled; the legacy acquisition comparisons are unvalidated historical
diagnostics and do not enter 1.1. Different backends or query panels must not supply
duplicate primary episode/concept units. Raw files are never modified.

## Metric definition

Version `1.1-v1` freezes baseline onset −28 through −8, lead −7 through −1, immediate
onset through event end +2, early recovery end +3 through +7 and extended recovery end +8
through +28. All endpoints are inclusive. Baseline requires 14 valid days; each response
phase requires 80% of its expected daily observations. Missing/partial values are excluded.
Zero fraction at least 0.5 is low volume; all-zero series remain an explicit exclusion.
Baseline population SD must exceed 0.001. Undefined outcomes remain null.

Primary outcome is the response-window maximum minus baseline mean divided by baseline SD.
Response peak lag uses the earliest tied maximum in lead through end +28. Global-peak lag
is separate. Phase lifts reuse the existing phase engine with the same effective baseline.
Legacy 1.0 metrics remain unchanged and do not enter the final response calculation.

Repeat-level lineage is retained; eligible repeats are averaged within the same
episode/geography/concept/role/backend/panel/request. Weather in the recovery panel is
excluded from principal summaries to avoid duplicate context weighting. Acquisition
files/repeats are not independent episodes. Aggregate source_lineage retains all source
hashes, repeats and attempts. Paired differences are descriptive standardized contrasts,
not raw index subtraction or causal attribution. One control does not satisfy the legacy
two-control attribution rule.

## Conditional prediction and exactly two sensitivities

Require 20 independent event groups overall and per included concept, five grouped folds
and at least 12 training groups in each fold. An explicit shared_event_group/event_group_id
in the catalog overrides episode identity for fold grouping. This is a project admission
rule, not a sample-size power calculation. Existing chronological split labels are retained,
but reused; the report does not claim an untouched historical test set.

Fixed ridge alpha=1 and a training-mean predictor use episode-balanced weights. Numerical
features are log1p(reported union area), reported duration, and month sine/cosine; concept
uses training-only reference coding. Medians, scaling, category levels and constant/missing
feature exclusion are learned in each training fold. Outcome and search-derived quality
variables are never predictors. Fold and final descriptive coefficients are labeled separately.
MAE/RMSE are reported by fold and pooled, with each episode equally weighted. No tuning,
p-values, population claims or operational forecasting claims are made.

Sensitivities are baseline ending at −1 and complete two-repeat groups. They report
episode-balanced mean lifts and changes on shared eligible units. Unavailable checks stay
unavailable. No post-hoc outcome substitution, epsilon denominator or additional acquisition.

## Deliverables and stopping

The output root contains the two typed analytical Parquets, eligibility.csv, readiness.json,
model_predictions.csv, model_metrics.csv, model_coefficients.csv, sensitivity.csv,
provenance.json, report.md, four PNG/SVG figure pairs and three CSV tables. When prediction
is not estimable its prediction/coefficient CSVs retain headers and the metrics/readiness
files record reasons. Figure 4 becomes eligibility coverage. No synthetic predictions are
substituted for empirical results. All tables are embedded in the final report.

CLI stdout is JSON. Input conflicts return exit code 2; a completed descriptive report
returns zero with prediction_status=not_estimable when appropriate. With no verified real
observations status is empirical_blocked. Config data_origin=synthetic_fixture labels a
software smoke run and cannot be presented as an empirical completion.

All inputs, software source snapshot, lockfile, config and outputs are hashed. No execution
timestamp enters scientific outputs. Two runs with the same input paths and locked
environment should produce identical files in different empty output roots. Cross-platform
Parquet comparisons use canonical content hashes; graphics can vary by rendering stack.

Finish at a reproducible descriptive feasibility report if prediction is unsupported. No new
backend, model family, dashboard or external data source is part of this release.

## Validation

```text
uv run python -m ruff check .
uv run python -m ruff format --check .
uv run python -m mypy src
uv run python scripts/run_tests.py
uv run python scripts/validate_documented_cli.py
uv build
```

For a deeply nested Windows checkout, pass `--basetemp` with a short, disposable, previously
unused directory to `scripts/run_tests.py`. Pytest owns and may clear that chosen directory;
never point it at a research-data directory. The suite uses synthetic fixtures and does not
need the author's research store. Browser integration remains opt-in.
