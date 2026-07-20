# MRMS Physical Verification

> Status: inventory and bounded fetch scaffolding are implemented. Production episode
> extraction intentionally fails with `real_mrms_extraction_not_implemented`; synthetic metric
> generators exist only in tests.

Milestone 0.7A adds targeted NOAA MRMS feasibility checks for provisional
candidate episodes. It validates access, inventory, caching, decoding hooks,
sample selection, and provisional precipitation-coherence metrics. It does not
perform national extraction, NOAA Atlas return-period analysis, Google Trends
ingestion, urban exposure analysis, final relabeling, or predictive modeling.

## Source

MRMS files are expected from the public NOAA AWS bucket:

```text
s3://noaa-mrms-pds
```

The code uses anonymous access and lists objects under:

```text
CONUS/<product-directory>/<YYYYMMDD>/*.grib2.gz
```

Milestone products are `MultiSensor_QPE_01H_Pass2`,
`RadarAccumulationQualityIndex_01H`, `MultiSensor_QPE_24H_Pass2`, and optional
`RadarOnly_QPE_15M`. Two-minute `PrecipRate` is intentionally out of scope.

## Workflow

`geodemand mrms sample` creates a deterministic verification sample from
conservative and balanced episode outputs. `inventory` lists S3 objects without
downloading, `fetch` caches compressed GRIB2 files with size and checksum
validation, `extract` writes provisional precipitation metrics, and `assess`
assigns review-only physical-coherence categories without changing episode
membership.

GRIB2 decoding uses `xarray` with the `cfgrib` engine and `indexpath=""` to
avoid persistent cfgrib index files. Missing/no-coverage precipitation values
are treated separately from true zero precipitation; ordinary negative values
raise an error.

## Reproduction

```powershell
uv run geodemand mrms selfcheck --working-root C:\Work\Data\GeoDemand\mrms
uv run geodemand mrms inspect-episodes --episode-root C:\Work\Data\GeoDemand\artifacts\episode_comparison_full
uv run geodemand mrms sample --episode-root EPISODE_OUTPUT_ROOT --output-root C:\Work\Data\GeoDemand\mrms
uv run geodemand mrms inventory --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --output-root C:\Work\Data\GeoDemand\mrms --max-episodes 3
uv run geodemand mrms fetch --download-plan C:\Work\Data\GeoDemand\mrms\inventory\mrms_download_plan.csv --working-root C:\Work\Data\GeoDemand\mrms --max-episodes 5
uv run geodemand mrms extract --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --file-manifest C:\Work\Data\GeoDemand\mrms\manifests\mrms_file_manifest.parquet --output-root C:\Work\Data\GeoDemand\mrms
uv run geodemand mrms assess --metrics-root C:\Work\Data\GeoDemand\mrms\metrics
```

The provided `episode_comparison_full` directory currently contains comparison
CSVs only. `mrms sample` requires policy episode and membership datasets before
a real sample can be generated.

## Catalog Integration

Catalog physical decisions require configured MRMS coverage and quality.
Multiple peaks, dry gaps, weak member correlation, and dispersed peak timing
can support split candidacy only in combination. Temporally coherent evidence
can support a balanced-policy merge candidate. Missing MRMS remains `pending`
and never counts against an episode.
