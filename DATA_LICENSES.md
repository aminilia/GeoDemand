# Data Licenses And Attribution

This repository does not commit raw research data, downloaded MRMS files,
boundary archives, Groundsource files, generated full audit artifacts, episode
outputs, or derived precipitation metrics.

## NOAA MRMS

Milestone 0.7A uses NOAA Multi-Radar/Multi-Sensor System files made available
through the public AWS bucket `s3://noaa-mrms-pds`. Users are responsible for
checking current NOAA terms, attribution expectations, and AWS public-dataset
availability before redistribution or publication.

Suggested attribution:

```text
NOAA Multi-Radar/Multi-Sensor System (MRMS) data accessed via the NOAA Open Data
Dissemination Program public AWS bucket noaa-mrms-pds.
```

## Groundsource And Boundaries

Groundsource, Natural Earth, and Census TIGER/Line inputs remain local external
inputs. Their licenses and redistribution constraints should be reviewed before
sharing derived datasets.

## NASA IMERG

NASA GPM IMERG Final data require Earthdata Login for download. Credentials must
remain local and must not be committed. Publications should cite NASA GPM IMERG
according to current NASA guidance for `GPM_3IMERGHH` Version 07.

## USGS Water Data

USGS water observations are accessed from official USGS Water Data services.
Users should preserve qualifier, approval, provisional, unit, parameter, and
monitoring-location metadata when sharing derived metrics.

## Provisional Catalog

The provisional catalog combines derived identifiers and metrics from the
sources above. Its creation does not grant redistribution rights for source
geometry, precipitation, or gauge data. Review all upstream terms before
publishing or redistributing catalog artifacts.

## Google Trends

Google Trends exports are subject to Google's Terms of Service. Reused data and
figures must be attributed to Google Trends according to the current official
export and citation guidance. Trends and Google Ads are distinct sources and
must not be conflated. Repository tests contain synthetic CSV structures only,
not copied Google Trends observations.
