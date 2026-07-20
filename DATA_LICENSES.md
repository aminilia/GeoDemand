# Data Licenses And Redistribution

This file records source and redistribution decisions for the current workflow. Verify
upstream terms again at publication time. Retrieval dates and exact input versions belong
in run manifests; repository tests and demonstrations use synthetic fixtures only.

| Source | Product/version | Official source | Terms and citation | Public repository policy |
|---|---|---|---|---|
| Groundsource | Confirmed local GeoParquet 0.4.0 snapshot; source file identifies its own release | Upstream release location must be recorded by the researcher | License and redistribution permission were not established in this repository. Cite the upstream dataset metadata supplied with the acquired file. | Do not redistribute raw geometry or copied records. Share code, aggregate audit findings, and derived artifacts only after rights review. |
| Natural Earth | Administrative boundaries, exact release recorded in boundary manifest | https://www.naturalearthdata.com/downloads/ | Natural Earth states that its raster and vector data are public domain; suggested citation: “Made with Natural Earth.” | Boundary inputs may be redistributed under those terms, but this repository keeps downloaded copies out to preserve a source-only archive. |
| U.S. Census Bureau | TIGER/Line state and equivalent boundaries, vintage recorded in boundary manifest | https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html | U.S. government work; Census requests source citation and provides accuracy/legal-boundary disclaimers in its technical documentation. | Raw downloads are omitted. Derived boundaries should retain vintage, disclaimer, and Census attribution. |
| NOAA | Multi-Radar/Multi-Sensor System (MRMS), product and timestamps recorded per manifest | https://registry.opendata.aws/noaa-mrms-pds/ | Cite NOAA MRMS and the NOAA Open Data Dissemination Program. Check product metadata and NOAA terms for each publication. | Do not commit GRIB files. Share small derived metrics only with provenance; real extraction is currently pending. |
| NASA | GPM IMERG Final `GPM_3IMERGHH_07`, Version 07 | https://disc.gsfc.nasa.gov/datasets/GPM_3IMERGHH_07/summary | Follow NASA GPM/GES DISC data-use and citation guidance; Earthdata authentication may be required. Record granule identifiers and retrieval date. | Do not commit HDF5/granules or credentials. Share derived metrics only with complete provenance; real extraction is currently pending. |
| USGS | Water Data for the Nation observations; API version/backend recorded in manifests | https://api.waterdata.usgs.gov/ | USGS identifies API data as U.S. government work in the public domain and requests citation. Preserve monitoring location, parameter, units, qualifiers, and approval/provisional status. | Raw responses remain local; compact derived response metrics may be shared with provenance and caveats. |
| Google Trends | Official Explore CSV exports; terminology and export-attempt versions in sidecars | https://support.google.com/trends/answer/4365533 | Subject to Google Terms of Service and current Google Trends export/citation guidance. Values are request-relative indices, not counts. | Do not commit raw exports unless redistribution permission is clear. Preserve untouched local CSVs and sidecars; public tests use synthetic structures. |

Groundsource license uncertainty is the strictest current redistribution constraint. A
public scientific data release needs a separate rights review even when the source-code
release is acceptable.
