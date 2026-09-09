# Milestone 1.0C acquisition design

## Historical status and scope

This document records the 1.0C planning contract. GeoDemand 1.1 now performs strict
plan-based analytical admission through `analysis finalize`; batch acquisition remains
outside the final task. The later PyTrends exporter is disabled and its historical
comparisons are explicitly unvalidated. See analysis_1_1.md.

## Original planning stages

Milestone 1.0C is split into three stages:

- **1.0C-1, historical audit:** inspected the existing planning, controlled-browser,
  sidecar, import, and validation contracts. Its machine-readable findings remain in
  `reports/acquisition_audit_1_0C.json`.
- **1.0C-2, implemented here:** deterministic nested episode selection and acquisition
  planning only.
- **1.0C-3, deferred:** browser acquisition, batch import, strict acquisition-completeness
  validation, duplicate-file-hash reporting, and observation rebuilding.

No browser acquisition, Trends download, CSV import, metric calculation, integrated-dataset
build, bootstrap, permutation test, ICC, p-value, or predictive model is part of 1.0C-2.

## Implemented episode selection

`trends pilot-sample` retains its ordinary 40-episode default and adds:

```text
--episode-limit <10|20|40>
--selection-version <str>
```

The command builds the 20-episode set first and the 10-episode set as a strict subset. It
preserves the established five-episode mini-pilot, then enforces the configured required
state continuity. With the current rules, the 20-set contains at least two Florida episodes
and the 10-set at least one. Remaining positions are filled deterministically by the least
represented year, region, and state, followed by source pilot rank and episode ID.

The selection artifacts are:

```text
planning/acquisition_episode_selection.csv
planning/acquisition_episode_selection.parquet
planning/acquisition_selection_summary.json
```

The versioned selection schema records actual insertion step, presentation rank, reason
code and detail, 10/20 membership, source rank, dates, state, and geography. The summary
records required and actual state counts, pass/fail status, counts by year/state/region and
reason, and selected episode IDs.

## Implemented execution planning

`trends plan` adds the following execution-only options:

```text
--batch-id <str>                 # repeatable
--planned-repeats <int>
--execution-batch-id <str>
--controls-per-episode <int>
--execution-only
```

Non-default execution options are rejected unless `--execution-only` is present. An
execution plan accepts only a selected 10- or 20-episode pilot carrying complete selection
lineage; a legacy 40-episode pilot is rejected with a structured domain error.

Each episode receives one `treated_state` request and exactly one existing rank-1
`control_state` request per selected panel. Missing, duplicated, invalid, or treated-equal
rank-1 controls fail planning. A deterministic episode-level `matched_pair_id` is shared by
both roles, every panel, and both repeats.

Execution-only outputs use distinct filenames so ordinary planning artifacts cannot be
overwritten:

```text
planning/acquisition_execution_plan.parquet
planning/acquisition_execution_plan.csv
planning/acquisition_execution_summary.json
planning/sidecars/*.json
```

Browser export, retry, status, and validation commands accept any valid supplied plan path;
the filename is not part of the plan contract. Export-status counts are scoped to
`request_id + planned_repeat_id` keys in that supplied plan.

## Panel design and exact counts

The 10-episode pilot uses:

- `batch_1_flood_awareness`
- `batch_5_specific_recovery`
- `batch_6_specific_products` (pilot-only)

```text
10 episodes × 2 roles × 3 panels × 2 repeats = 120 rows
```

This is 10 matched pairs and 60 logical request IDs.

The 20-episode full-core plan uses only:

- `batch_1_flood_awareness`
- `batch_5_specific_recovery`

```text
20 episodes × 2 roles × 2 panels × 2 repeats = 160 rows
```

This is 20 matched pairs and 80 logical request IDs. The product panel remains pilot-only
until coverage is reviewed. National retailer-brand batches remain available for separate
exploration but are excluded from both primary plans.

## Identity and lineage contracts

- `request_id` hashes the logical request definition and excludes repeat, execution batch,
  matched pair, selection, and attempt identity.
- `planned_repeat_id` is `repeat_1` or `repeat_2` for the current execution designs.
- `export_attempt_id` identifies a future browser attempt. A retry keeps its request and
  planned-repeat identity and receives a new attempt ID.
- `matched_pair_id` identifies the episode-level treated/control geography pair and is
  invariant across panels, repeats, attempts, and execution batches.
- `execution_batch_id`, `selection_version`, and `execution_selection_rank` are propagated
  lineage fields and do not change request hashes.

Both the selection and execution plans use explicit, versioned Arrow schemas. Identifiers
are strings, ranks and ordinals are integers, membership fields are booleans, control rank
is nullable only for treated rows, and column order is fixed for CSV and Parquet.

## Reproduction commands

Create the selected cohort first:

```text
geodemand trends pilot-sample --catalog-dir CATALOG --output-root PILOT_ROOT --rules config/trends_rules.yaml --episode-limit 10 --selection-version 1.0C-v1
geodemand trends map-geographies --pilot PILOT_ROOT/manifests/trends_pilot_episodes.parquet --output-root PILOT_ROOT
```

Create the 120-row pilot execution plan:

```text
geodemand trends plan --pilot PILOT_ROOT/manifests/trends_pilot_episodes.parquet --geography PILOT_ROOT/geography/geography_mapping.parquet --controls CONTROLS --terms config/trends_terms.yaml --rules config/trends_rules.yaml --output-root PILOT_ROOT --batch-id batch_1_flood_awareness --batch-id batch_5_specific_recovery --batch-id batch_6_specific_products --planned-repeats 2 --execution-batch-id 1.0C-pilot-v1 --controls-per-episode 1 --execution-only
```

For the 160-row full-core plan, select 20 episodes, omit
`batch_6_specific_products`, and use execution batch `1.0C-full-core-v1`.

## Deferred 1.0C-3 work

The following are not implemented or documented as available:

- browser acquisition and raw Trends exports;
- manifest-driven batch import;
- strict raw-file, repeat, panel, and matched-pair completeness validation;
- cross-export duplicate-hash classification and reporting;
- deterministic observation rebuilding from an import ledger;
- statistical inference or `analysis validate-signal`.
