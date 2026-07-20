# NASA IMERG Verification

> Status: inventory and bounded fetch scaffolding are implemented. Production episode
> extraction intentionally fails with `real_extraction_not_implemented`; synthetic metric
> generators exist only in tests.

Milestone 0.7B adds an offline-testable NASA GPM IMERG Final scaffold for
candidate episode precipitation verification.

Install the optional client and decoder stack with
`uv pip install -e ".[imerg]"`. Ordinary offline tests do not require Earthdata
packages, credentials, or network access.

The target collection is `GPM_3IMERGHH` Version `07`, Final run. The primary
field is expected to be `Grid/precipitation`, but real HDF5 metadata must be
inspected before relying on a field path, fill value, scale factor, valid range,
longitude orientation, or quality variable.

IMERG precipitation is a rate in `mm/hr`. GeoDemand converts each half-hour to
accumulation with:

```text
half_hour_accumulation_mm = precipitation_rate_mm_per_hour * 0.5
```

Hourly IMERG values are formed only from paired valid half-hours. Missing
half-hours are not silently treated as zero.

Authentication supports `EARTHDATA_TOKEN`, `EARTHDATA_USERNAME` with
`EARTHDATA_PASSWORD`, user-managed `.netrc`, and interactive authentication only
when explicitly requested. Credentials are never written to manifests or logs.

Commands:

```powershell
uv run geodemand imerg selfcheck --working-root C:\Work\Data\GeoDemand\imerg
uv run geodemand imerg inventory --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --output-root C:\Work\Data\GeoDemand\imerg --max-episodes 5 --dry-run
uv run geodemand imerg fetch --download-plan C:\Work\Data\GeoDemand\imerg\inventory\imerg_download_plan.csv --output-root C:\Work\Data\GeoDemand\imerg --max-episodes 5
uv run geodemand imerg extract --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --file-manifest C:\Work\Data\GeoDemand\imerg\imerg_file_manifest.parquet --output-root C:\Work\Data\GeoDemand\imerg
```

IMERG and MRMS both estimate precipitation, but they have different spatial and
temporal support. Comparisons are descriptive physical-verification evidence,
not final truth labels.

Inventory uses the episode start minus 6 hours through episode end plus 6
hours. Fetching is bounded by episode and byte limits, uses atomic files, checks
content length, and records failures. Milestone completion still requires a
real authenticated granule decode; package imports alone are not a successful
NASA self-check.
