# GeoDemand 1.1 release candidate notes

This source-only release candidate adds a final response report with strict raw-data admission,
consistent effective baselines, repeat/panel lineage, conditional episode-grouped ridge
prediction, and two prespecified descriptive sensitivities. Existing acquisition and
catalog workflows remain the foundation. No downloads occur during finalization.

PyTrends export fails closed pending validated query and immutable-repeat semantics.
Historical acquisition comparison artifacts are labeled unvalidated and are not used in
the final report. Manual imports reject hash mismatches and conflicting observation
identities while remaining idempotent for exact reimports. Runtime debris is excluded from
the source distribution and the inventory test is now synthetic.

Treated/control contrasts are reported only for pairs that satisfy the same eligibility
contract used by readiness: matched panel/concept, complete shared repeat support, distinct
requests, and distinct geographies. The observed release-candidate run has five eligible
treated event/concept units but zero estimable treated/control contrasts because its control
support is disjoint. Prediction is explicitly not estimable (fewer than 20 independent event
groups and insufficient per-concept coverage); the descriptive result is complete for the
available verified subset. Event response windows vary with event duration.

See analysis_1_1.md for exact input/output contracts and the run-specific report.md for
empirical completion status. The software version remains 0.1.0; milestone 1.1 is an
analysis/design version, not an automatically published package tag. No tag, release or
data publication is performed. The author's license and DATA_LICENSES.md remain in force.
