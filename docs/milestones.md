# Milestones

| Milestone | Active scope |
|---|---|
| 0 | Python package, CLI, schemas, Groundsource ingestion, tests, docs |
| 0.5 | Real Groundsource GeoParquet audit, geometry safeguards, spatial enrichment |
| 0.6 | Candidate U.S. cohort and deterministic episode clustering |
| 0.7 | Provisional reported-event catalog from clustering and spatial diagnostics |
| 0.8 | Google Trends terminology, planning, controlled export, and non-causal event study |
| 1.0A | Schema- and lineage-validated integration of request, episode, repeat, and phase metrics |
| 1.0B | Deterministic descriptive signal summaries and paired-repeat diagnostics |
| 1.0C-2 | Nested execution selection, matched-pair lineage, and planning-only dry runs |
| 1.0C-3 | Historical dual-backend experiment; PyTrends export disabled and comparisons unvalidated for 1.1 |
| 1.1 | Strict raw admission, final response report, conditional grouped ridge baseline and two sensitivities |

Milestone 1.0C-2 produces nested 10/20 episode selections, uses one rank-1 control per
episode, supports exact panel filtering and two planned repeats, and scopes export status to
the supplied plan. Expected sizes are 120 rows for the three-panel pilot and 160 rows for
the two-panel full-core plan. Browser acquisition, import, completeness validation, metric
calculation, and inference remain later work.

Milestone 1.0B currently extends the completed 1.0A integration with strictly descriptive
signal summaries. It includes explicit exclusion accounting, request-level repeat
aggregation and equally weighted request/concept principal summaries, paired diagnostics,
and non-inferential concept-rank agreement. Pairwise procedures require exactly two
exports; larger repeat sets require a separate multi-repeat design. Batch 1 has
five independent requests and only one repeated Florida request; it does not support formal
population inference or general repeat reliability. Bootstrap, permutation tests, ICC,
p-values and `analysis validate-signal` are not implemented. The separate 1.1 finalizer adds a conditional predictive baseline; it does not change the 1.0B descriptive contract.

The active project is a Groundsource-to-Google-Trends event-study pipeline. Candidate
episodes remain reported-event research units and require manual interpretation.
