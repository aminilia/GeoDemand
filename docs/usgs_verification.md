# USGS Gauge-Response Verification

Milestone 0.7B adds an offline-testable USGS gauge-response scaffold.

Install the optional client stack with `uv pip install -e ".[usgs]"`.
Ordinary offline tests use an adapter mock and require neither network access nor
an API key.

Primary parameter codes:

- `00060`: discharge
- `00065`: gage height

Continuous observations are preferred because flash-flood response can occur at
sub-daily time scales. Daily values may provide context but should not replace
continuous observations when available.

Gauge association is proximity-based only. GeoDemand does not claim a gauge is
downstream or hydrologically connected based solely on Euclidean distance.
Absence of a suitable gauge, or absence of a response at a nearby gauge, is not
automatic evidence that a Groundsource event is false.

Association categories:

- `inside_episode_geometry`
- `inside_25km_buffer`
- `nearby_within_50km`
- `fallback_within_100km`
- `no_suitable_gauge`
- `uncertain_association`

Commands:

```powershell
uv run geodemand usgs selfcheck --working-root C:\Work\Data\GeoDemand\usgs
uv run geodemand usgs discover --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --output-root C:\Work\Data\GeoDemand\usgs --max-episodes 5 --dry-run
uv run geodemand usgs fetch --request-plan C:\Work\Data\GeoDemand\usgs\inventory\usgs_request_plan.csv --output-root C:\Work\Data\GeoDemand\usgs --max-episodes 5
uv run geodemand usgs extract --observations C:\Work\Data\GeoDemand\usgs\cache\usgs_observations.parquet --associations C:\Work\Data\GeoDemand\usgs\inventory\usgs_episode_gauge_associations.parquet --output-root C:\Work\Data\GeoDemand\usgs
```

`USGS_API_KEY` is optional. The code records only whether a key was present, not
the key itself.

Normalized request responses are cached by monitoring location, parameter, and
UTC window. The precipitation window is expanded by 24 antecedent hours and 48
post-event hours for response assessment. A real continuous series must still
be retrieved before Milestone 0.7B can be called complete.

## Catalog Integration

Available gauge-response evidence can strengthen physical support or a review
decision, but cannot independently cause a split or merge. When no suitable
gauge exists, `usgs_hydrologic_response_supported` is null rather than false.
