# U.S. State Matching Metadata

## Version

- Metadata version: `2026-07-v2`
- Creation date: `2026-07-20`
- Coverage: 50 states and the District of Columbia
- Purpose: transparent candidate-control ranking for the Google Trends pilot

This table is an analyst-created research aid. It is not an official NOAA, Census,
or Google classification.

## Field Provenance

| Field | Source and vintage | Method | Authority |
|---|---|---|---|
| `state_code` | U.S. Census Bureau state abbreviations, 2020 Census geography | Direct transcription | Authoritative identifier |
| `census_region` | U.S. Census Bureau Census Regions and Divisions, 2020 geography | Direct region assignment | Authoritative Census grouping |
| `population_tier` | 2020 Census resident population totals | Analyst-ranked bins: 12 most populous `high`, 15 least populous `low`, remaining 24 `medium` | Analyst-derived |
| `climate_class` | Broad qualitative synthesis informed by state climate normals and dominant climate regimes | Analyst-assigned broad labels; transition labels combine major regimes and are not official NOAA classes | Analyst-derived |
| `coastal_state_flag` | State boundary adjacency to Atlantic, Pacific, Gulf of Mexico, or Great Lakes | Analyst-created binary flag; DC is false | Analyst-derived |
| `observed_trends_availability` | No completed state-level availability audit as of creation date | Preserved as `unknown` for every row | Unknown, not evidence of availability |

## Source URLs

- Census 2020 resident population totals:
  https://www.census.gov/data/tables/2020/dec/2020-apportionment-data.html
- Census Regions and Divisions reference map:
  https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf
- NOAA U.S. Climate Normals overview used only as broad contextual background:
  https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals

The climate labels are not a NOAA classification and must not be cited as one. They are
coarse matching covariates whose limitations should be retained in derived control
summaries.

## Trends Availability

All `observed_trends_availability` values are currently `unknown`. Control selection
therefore awards zero Trends-availability points, reports unknown candidate and selected
control counts, and labels otherwise complete matches as
`metadata_backed_availability_unknown`. A future empirical availability audit must create
a new metadata version rather than overwrite this file.
