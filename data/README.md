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

The Groundsource ingestion commands accept explicit paths, so data can also live
outside the repository.
