# GeoDemand 1.2 release candidate

This milestone integrates completed results and closes the descriptive study. Numerical
methods, eligibility thresholds and model admission remain unchanged. The official-only
analysis and exploratory likely-PyTrends archive keep separate provenance. See
[final_results.md](final_results.md) for the complete interpretation and exclusions.

## Scope and versioning

- Five eligible official-subset units concern weather context only.
- Twenty-two exploratory flood episodes pass numerical quality; nineteen are eligible
  in both archived versions. All nineteen share peak dates; magnitudes differ.
- No eligible treated/control contrasts or fitted predictive result are reported.
- Retrieval dates and acquisition-attempt identities remain unresolved for the archive.
- Source package version remains 0.1.0 and metric version remains 1.1-v1. Milestone 1.2
  identifies the closeout work; existing public tags are not moved or reused.

## Local candidate contents

Source distribution, wheel, commit patch/bundle, integrated Markdown report, separate
analysis outputs, audit tables, supplementary builder, and validation/checksum records.
Raw acquisition CSV stores are excluded. Local analytical files are not approved for
public redistribution merely because they are in the local package. Existing license
and data policies remain in force.

## Completion criteria

Check supported Python 3.11/3.12/3.13 runtimes, configured type checks, lint, formatting,
CLI documentation and package builds. Reproduce outputs and retain provenance. Review
the staged documentation and record a clean local commit. Publication, pushing and
tagging remain separate actions. Do not add new acquisition, model families or dashboards.
