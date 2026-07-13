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
also live outside the repository. Generated full-size audit outputs should be
kept outside version control or under ignored local output directories.

Groundsource itself is GeoParquet with WKB geometries. Country or state boundary
datasets for future spatial enrichment should be stored as external local inputs
and must not be committed unless their license explicitly permits redistribution.
