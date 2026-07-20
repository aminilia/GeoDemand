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

Milestone 0.5E expects versioned boundary archives outside the repository:

```text
C:\Work\Data\GeoDemand\boundaries\
  natural_earth\5.1.1\ne_10m_admin_0_countries.zip
  census\2025\tl_2025_us_state.zip
```

Prepared boundary artifacts and spatial-enrichment outputs should also remain
outside version control. Typical local output folders are:

```text
C:\Work\Data\GeoDemand\artifacts\boundaries_prepared\
C:\Work\Data\GeoDemand\artifacts\groundsource_spatial\
C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full\
C:\Work\Data\GeoDemand\artifacts\candidate_episodes\
C:\Work\Data\GeoDemand\mrms\
C:\Work\Data\GeoDemand\imerg\
C:\Work\Data\GeoDemand\usgs\
C:\Work\Data\GeoDemand\multisource_verification\
```

Candidate episode outputs, sensitivity tables, and smoke-run artifacts are
derived research outputs and should not be committed.

MRMS compressed files, decoded temporary GRIB files, metric outputs, manifests,
and quicklooks are also local derived artifacts and must remain outside version
control.

IMERG downloads, USGS cached responses, multisource comparison outputs, and
quicklooks follow the same local-only rule.

The provisional catalog is also a generated local artifact:

```text
C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog\
```

Do not commit its Parquet tables, review exports, summaries, or manifests.

Google Trends exports and derived feasibility outputs belong under the ignored
external root `C:\Work\Data\GeoDemand\trends\`. Preserve downloaded official
CSVs and their sidecars under `raw/`; never edit or overwrite them in place.
The browser assistant writes operational attempt history under `manifests/` and
uses temporary downloads only until validation and atomic rename complete.
Persistent browser profiles and temporary browser downloads are local operator
state, not research data, and must remain outside version control.
