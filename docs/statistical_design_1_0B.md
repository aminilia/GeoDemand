# Milestone 1.0B Statistical Design Audit

## 1. Dataset contract

This audit uses the corrected integrated artifact at
`C:\Work\Data\GeoDemand\trends\1.0A_final_analysis_corrected\integrated\analysis_dataset.parquet`
(SHA-256 `cc6f5b003faef6a3a9eb566e397ae6ae8ecec1ee77cda0dc4ca983d9457c579d`).
It contains 30 rows and 84 columns. The source provenance records six request-plan rows,
30 phase-response rows, 30 repeat-response rows, and 25 episode-response rows. The full
column contract and missingness audit is in `reports/analysis_audit/dataset_column_audit.csv`.

All required request-plan, repeat-metric, episode-metric, and phase-metric joins matched.
`join_summary.json` reports zero unmatched or orphan records, and `unmatched_keys.csv`
contains only its fixed header. The dataset contains five configured concepts, all using
terminology version `0.8A-v3`: `context_heavy_rain`, `context_weather`, `flash_flood`,
`flood`, and `flooding`.

## 2. Analytical grain

The declared and observed grain is `request_id + repeat_id + concept_id`. These identifiers
are strings, and there are zero duplicate-grain rows. The audit fails with
`audit_duplicate_analytical_grain` if a duplicate is present.

## 3. Available sample size

The dataset represents five independent request IDs, five episode IDs, five geographies
(`US-FL`, `US-KS`, `US-MS`, `US-WA`, and `US-WY`), five concepts, and two repeat-ID values.
Four requests have one export for each concept. Only the Florida request has two exports
for each concept. Consequently, there are five repeat-eligible request/concept groups, but
all five are concepts nested within one repeated request—not five independently repeated
requests. Coverage is reported in `group_coverage.csv` and `repeat_coverage.csv`.

## 4. Dependence and nesting structure

Rows sharing request ID are dependent: concepts share an event, geography, windows, and
request context, while repeat rows are repeated exports rather than new observations.
Concepts and repeats are nested within request; here each request also maps to one episode
and one geography. Request ID is therefore the available independent cluster. In a future
design with multiple requests per episode, episode may be the upper cluster for a
population estimand spanning episodes.

Inference must first aggregate repeats within request and concept. Treating 30 rows, the
five concepts, or the two Florida exports as independent replicates would be
pseudoreplication.

## 5. Viable metrics

Finite numeric fields are viable for deterministic descriptive reporting with explicit
missing and non-finite counts. Complete, interpretable response metrics include
`absolute_peak_lift`, `peak_lead_lag_days`, `zero_fraction`, `event_maximum`,
`post_maximum`, the raw phase means and maxima, and the four phase peak-lift fields. These
metrics have 30 finite rows representing five requests, episodes, and geographies.

Paired repeat diagnostics are available for nonconstant continuous metrics within the
five Florida request/concept pairs. This repeat eligibility is separate from, and does not
establish, population-inference eligibility. Exact support appears in
`metric_viability.csv`.

## 6. Ineligible metrics

No metric supports formal population inference in Batch 1 because at most five independent
request clusters are represented, below the proposed minimum of 20. Complete metrics are
therefore labeled `insufficient_independent_requests_5_below_20`, not as single-request
metrics. Counts for metrics with missing values are calculated from valid finite rows.

Standardized and robust fields are highly incomplete: standardized fields generally have
12 finite rows and robust fields six. `maximum_repeat_phase_lift_stddev` has only two finite
rows from one request. Valid-day counts and `repeat_count` are design diagnostics, while
boolean flags are flag-derived rather than primary response estimands. Missingness,
constant values, count/flag status, and cluster insufficiency are reported as separate
reasons; no value is imputed.

## 7. Repeat-reliability design

Only the Florida request is repeated. Its five concepts yield five paired groups for a
metric when both repeat values are finite. Report paired values, signed and absolute
difference, within-pair mean, and sample standard deviation. Relative difference requires
a pre-specified nonzero denominator; coefficient of variation is limited to nonnegative
ratio-scale metrics with a positive mean. Rank consistency may compare the five concept
rankings, with ties disclosed.

These diagnostics characterize one request's export variation and cannot establish
general repeat reliability. ICC is unsupported because there is only one independently
repeated request and the five concepts are not exchangeable subjects.

## 8. Bootstrap design

Bootstrap intervals are not defensible for current Batch 1. A row bootstrap would break
the dependence structure, and five independent request clusters are below the proposed
minimum of 20 per estimand.

For a future expanded dataset, pre-specify a request-level, fixed-concept estimand and
aggregate repeats first. Resample request clusters, or episodes carrying all nested
requests when multiple requests occur per episode. Use a 95% percentile interval, seed
`20260721`, and 10,000 iterations; warn below 30 clusters. Sparse, constant, or
incompletely identified clusters invalidate the interval. These are design proposals,
not implemented procedures.

## 9. Permutation-test design

No current permutation hypothesis has defensible exchangeability. The five requests cover
different episodes and geographies, all rows have the treated-state role, and there are no
matched treated/comparison pairs. Neither concept labels nor ordered temporal phases may
be permuted as though assigned at random.

A future paired test could use pre-specified treated-minus-comparison request-level
differences within independent matched episode/concept pairs, with pairwise sign swaps and
a two-sided mean-difference statistic. Require at least ten complete independent pairs,
seed `20260721`, and 10,000 permutations. Missing or reused controls, inconsistent windows,
or absent role labels invalidate the test.

## 10. Negative-control design

Available controls are limited to descriptive context concepts and baseline or anticipatory
window diagnostics. Comparison-related columns are present, but no comparison-role rows
form matched observations in this dataset. Additional upstream data are required for
explicit comparison geographies, unmatched event periods, and pre-specified negative
controls. Such controls must not be fabricated.

## 11. Multiple-testing strategy

No correction is currently applied because no inferential test is valid. Future tests
should use Benjamini-Hochberg within each pre-declared family, such as fixed-metric
treated/comparison contrasts across concepts. Magnitude, phase, timing, reliability, and
negative-control hypotheses should remain separate families.

## 12. Exclusion and warning rules

- Report null and non-finite counts by metric; never silently exclude or impute them.
- Derive metric-specific request, episode, and geography counts from finite rows only.
- Exclude a repeat pair only for the affected metric and retain the reason.
- Do not calculate ratios or coefficients of variation with invalid denominators.
- Do not treat valid-day counts or flags as primary continuous responses.
- Invalidate population inference below 20 independent request clusters per estimand.
- Invalidate matched inference for incomplete pairs, reused exchangeability units, lineage
  conflicts, or duplicate analytical-grain rows.

## 13. Limitations of Batch 1

Batch 1 has five independent request clusters, one per episode and geography. This is more
than the earlier Florida-only artifact but remains exploratory. Google Trends values are
relative indices, only Florida was exported twice, standardized/robust responses are
sparse, and matched comparison observations are absent. Complete lineage improves the
data contract but does not increase statistical independence.

## 14. Recommended scope for the first `validate-signal` implementation

The first implementation may cover contract validation, duplicate-grain failure,
deterministic descriptive summaries, explicit exclusion accounting, and paired Florida
repeat diagnostics with clear nesting labels. Bootstrap, permutation tests, ICC, and
population claims should remain disabled until their independent-cluster and design
requirements are met.

## 15. Implementation recommendation

**Conditional go for a narrow descriptive/repeat-validation command; no-go for formal
population inference on current Batch 1.** At least 20 independent request clusters per
estimand, explicit matched treated/comparison requests, and pre-specified negative controls
are required before inferential functionality. Request-plan and metric lineage are already
complete and are not an outstanding upstream addition.

The deterministic machine-readable recommendation is in
`reports/analysis_audit/statistical_design_summary.json`.
