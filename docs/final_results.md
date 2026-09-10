# GeoDemand 1.2 — final descriptive report

## Executive result

GeoDemand provides a reproducible reported-flood-episode and search-interest analysis pipeline. The completed study has two distinct evidence levels: an official-export subset whose raw files and lineage pass the pipeline's checks, and an exploratory archive whose acquisition provenance remains incomplete. They are not pooled, and their episode counts must not be added together.

The verified subset yields five eligible weather-context episode/concept units. It does not establish a flood-specific response. The exploratory archive yields 22 eligible flood episode/concept units with a median response-window peak of **4.56 baseline standard deviations** and median peak lag of **3 days after reported onset**. Across the 19 episodes eligible in both archived versions, every peak date agrees, while peak magnitudes differ. These are descriptive results, not causal effects, significance tests or predictive validation.

## Evidence and sample accounting

| Analysis | Available/planned sample | Eligible analysis | Evidence level |
|---|---|---|---|
| Official-only 1.1 | 20 planned episodes; observations verify for 5 | 5 weather-context units; 6 eligible repeat-level records | File hashes, sidecar lineage and values checked under frozen 1.1 admission |
| Exploratory flood archive | 40 episodes; one flood-awareness request per episode and version | 22 eligible in the partial-preserving version | Likely PyTrends; retrieval dates and attempt identities unresolved |
| Archived-version comparison | Same 40-episode archive | 19 shared eligible episodes; 3 eligibility changes | Within-archive version sensitivity, not independent repeat reliability |

The official plan contains 800 request/repeat/concept records: 770 lack observations and 794 are excluded in total, including the missing records. Thirty records are acquired and verified; six pass numerical rules and aggregate to five weather units. No control-state records are acquired in this supplied subset. There are zero eligible treated/control contrasts and prediction is not estimable. “Verified” describes the pipeline's provenance checks, not independent confirmation of the flood event or external-service authenticity.

The larger archive has 600 CSV files but only 200 distinct requests across five panels and 40 episodes. Two stores are exact copies; the third contains a different numerical version with partial flags. Duplicate copies are counted once. Different versions are not assumed to be intentional independent repeats. Weather appears in multiple panels and is not counted as additional episodes. Only 10 of the 22 eligible exploratory flood episodes belong to the frozen 20-episode full-core cohort; the supplement does not override the original prediction gate.

## Methods

Both analyses retain the same numerical response rules. Baseline: onset −28 through −8 inclusive, at least 14 valid days. Lead: −7 through −1. Immediate: onset through event end +2. Early recovery: end +3 through +7. Extended recovery: end +8 through +28. Each response phase requires 80% valid daily coverage. Zero fraction ≥0.5 and baseline population SD ≤0.001 exclude the normalized response. No threshold relaxation, denominator epsilon or outcome imputation is used.

Peak lift is the maximum in lead through event end +28 minus baseline mean, divided by baseline population SD. Earliest tied peaks define timing. A mean of peak lifts is not a mean increase in daily search interest. The onset-aligned curves display −28 through +28; peak metrics use the full event-end-anchored window. Longer events therefore have more opportunities to produce large maxima.

The official analysis respects partial flags and averages eligible repeats within requests. Its fixed ridge model and training-mean comparator remain gated off because fewer than 20 independent event groups and insufficient per-concept coverage are available. No fitted predictive result is substituted.

The supplement examines flood only in the flood-awareness panel. The partial-preserving store was chosen for its retained metadata after the audit, not preregistered before inspecting the data. All recorded partial flags are False; this does not authenticate acquisition history. The alternative version omits partial flags, so its results are conditional on accepting unknown partial status. Retrieval dates remain unknown. Logs and an adjacent PyTrends script undermine the inherited manual_csv label, but do not bind every file hash to a specific acquisition attempt.

## Results

### Verified official-only subset

Only context_weather is eligible. Its mean standardized peak lift is **3.305**, with median peak lag **19 days**. Flood-specific series fail sparse-signal or baseline-normalization rules in this subset. Missing or excluded series are not evidence of absent public response.

The baseline-ending-at-−1 sensitivity admits six units in five episodes, with overall mean peak lift 6.980 versus 3.305 in the primary result. That difference includes a changed eligible sample; the paired mean change on shared units is **+0.206**. Restricting to complete two-repeat groups leaves one episode/unit with mean lift 2.851 and zero change on the shared unit. One episode cannot establish general repeat reliability.

### Exploratory flood archive

| Subset | Episodes | Mean peak lift (baseline SD) | Median peak lift (baseline SD) | Median peak lag (days) |
|---|---:|---:|---:|---:|
| Partial-preserving, all eligible | 22 | 10.250 | 4.556 | 3 |
| Partial-preserving, shared only | 19 | 11.160 | 5.307 | 4 |
| Alternative, shared only | 19 | 11.576 | 4.919 | 4 |

For the 22 episodes, peak-lift quartiles are 2.975 and 14.365; peak lags range from −7 to +24 days. The mean exceeds the median substantially, so a few large normalized peaks influence the mean.

### Same-episode version sensitivity

Across the 19 shared eligible episodes, partial-preserving minus alternative peak lift has mean **-0.416** and median **0.000** baseline SD units. The mean absolute difference is **2.310**. Peak dates agree in **19 of 19** cases. Timing agreement does not imply equal magnitudes or independent replication.

Three episodes in Utah, Nevada and Missouri pass only in the partial-preserving version. Their alternative zero fractions are approximately 0.561, 0.678 and 0.526; their primary fractions are 0.456, 0.492 and 0.474. The cutoff remains 0.5. Full record-level exclusions and the paired comparison are retained in the local supplement.

## Limits and conclusion

The results demonstrate usable flood-search variation in a selected exploratory archive and a reproducible official-subset workflow. They do not establish that floods caused the peaks, estimate representative public demand, validate physical event severity, or show predictive skill. There are no valid control contrasts. Search indices are query-relative rather than counts. Sparse-series selection, variable response-window duration and unresolved acquisition provenance constrain interpretation. Equality of peak dates may reflect related archive history and must not be described as independently replicated reliability.

GeoDemand is complete for this bounded descriptive scope: five official-subset weather units, 22 exploratory flood units, and a comparison on 19 shared episodes. No combined “27 verified episodes” result is claimed. Further acquisition, prediction or population inference would be a new study, not a requirement for finishing this release candidate.

## Reproduction and deliverables

The local release package contains this integrated report, the unchanged official-only outputs, the exploratory report and tables, the audit manifests, the supplement builder, and a validation record. Each analysis retains its own provenance and eligibility accounting. Raw acquisition inputs remain at their original local paths; no raw CSV archive is bundled. Data-bearing analytical outputs are local-use deliverables subject to DATA_LICENSES.md, not an authorized public data release.

See docs/release_1_2.md in the source candidate for source/version conventions, docs/reproduction.md for reproduction, and the local validation record for executed checks. Milestone 1.2 does not change metric version 1.1-v1 or automatically assign a new package version or public tag.
