# Multi-Source Verification

Milestone 0.7B combines candidate episode metadata, MRMS metrics, IMERG metrics,
MRMS/IMERG precipitation agreement, and USGS gauge-response evidence.

The pilot selector is deterministic for a fixed input and seed (`20260715`). It
selects quota cases for large episodes, balanced-policy merge candidates,
typical multi-member episodes, singletons, and split boundaries, then favors
uncovered years, seasons, states, and regions until the target is reached.

MRMS and IMERG are compared after harmonizing temporal support to common UTC
hours. IMERG half-hours are aggregated to hourly totals only when both
half-hours are valid. Comparisons use overlapping valid hours and report common
coverage.

USGS gauges measure hydrologic response at monitoring locations. Gauge evidence
is interpreted separately from precipitation evidence, and no suitable gauge is
reported as `unknown`, not `false`.

Integrated categories are rule-based review categories, not final labels:

- `strong_multisource_support`
- `precipitation_support_with_gauge_response`
- `precipitation_support_no_suitable_gauge`
- `precipitation_support_without_detected_gauge_response`
- `usgs_response_with_weak_precipitation_support`
- `conflicting_precipitation_products`
- `weak_multisource_signal`
- `insufficient_data`
- `manual_review`

Commands:

```powershell
uv run geodemand observations pilot-sample --sample C:\Work\Data\GeoDemand\mrms\manifests\verification_sample.parquet --output-dir C:\Work\Data\GeoDemand\multisource_verification
uv run geodemand observations compare-precipitation --mrms-metrics MRMS_METRICS --mrms-timeseries MRMS_TIMESERIES --imerg-metrics IMERG_METRICS --imerg-timeseries IMERG_TIMESERIES --sample PILOT_SAMPLE --output-dir C:\Work\Data\GeoDemand\multisource_verification
uv run geodemand observations assess --sample PILOT_SAMPLE --mrms-metrics MRMS_METRICS --imerg-metrics IMERG_METRICS --precipitation-comparison PRECIP_COMPARISON --usgs-summary USGS_SUMMARY --output-dir C:\Work\Data\GeoDemand\multisource_verification
uv run geodemand observations review-stubs --assessment C:\Work\Data\GeoDemand\multisource_verification\multisource_episode_assessment.parquet --output-dir C:\Work\Data\GeoDemand\multisource_verification
```

The `review-stubs` command writes deterministic text selections for manual review; it
does not claim to produce plots. Trends quicklooks are real deterministic SVGs. Real
multi-panel physical-evidence plots require decoded precipitation grids, and MRMS and
IMERG production extraction remains pending.
