# Data Directory

This directory documents expected local data organization. Do not commit raw,
downloaded, proprietary, or derived research datasets.

Suggested local layout:

```text
data/
  raw/          # ignored local source files
  interim/      # ignored intermediate outputs
  processed/    # ignored processed Parquet outputs
  external/     # ignored externally managed references
```

The Groundsource ingestion and audit commands accept explicit paths, so data can
also live outside the repository. Generated full-size audit outputs should be kept
outside version control or under ignored local output directories.

Boundary archives and prepared boundary outputs should remain external local inputs:

```text
C:\Work\Data\GeoDemand\boundaries\
C:\Work\Data\GeoDemand\artifacts\boundaries_prepared\
C:\Work\Data\GeoDemand\artifacts\groundsource_spatial\
C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\
C:\Work\Data\GeoDemand\artifacts\candidate_episodes\
C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog\
C:\Work\Data\GeoDemand\trends\
```

Candidate episode outputs, sensitivity tables, catalog tables, review exports,
summaries, manifests, Google Trends exports, sidecars, and derived feasibility
outputs are local research artifacts and should not be committed.

Preserve downloaded official Google Trends CSVs and their sidecars under the local
Trends raw area; never edit or overwrite them in place.
