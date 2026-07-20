# Google Trends Feasibility

Milestone 0.8A evaluates whether relative Google search interest can support a
defensible pilot around provisional flood episodes. It does not perform a
national extraction, estimate absolute search counts, confirm flood truth, or
train a predictive model.

## Access backends

`official_api` is reserved for Google's limited-access Trends API alpha. The
self-check does not invent an endpoint, credential variable, or authentication
flow. Without documented local access it returns
`unavailable_or_not_authorized` and performs no network request. Google states
that authorized alpha users can access a rolling five-year window, regular
daily through yearly aggregation, regions and subregions, and consistently
scaled data: <https://developers.google.com/search/apis/trends>.

`manual_csv` is the required official fallback. Google documents downloading
Explore charts as CSV and requires attribution when Trends data are reused:
<https://support.google.com/trends/answer/4365538>. Every unchanged CSV requires
a JSON or YAML sidecar with the complete request definition. The importer never
infers request metadata from a filename and preserves both files under `raw/`.

`classic_web_experimental` is planning-only and disabled by default. The project
does not depend on pytrends and does not automate login, CAPTCHA handling,
fingerprint imitation, proxy rotation, or rate-limit evasion.

## Scientific design

Explore values are request-specific relative search interest scaled from 0 to
100, not absolute query volume. Values from separate requests must not be
concatenated as though they share a common scale. Terms and topics remain
distinct concepts; spelling variants, plurals, and synonyms are not assumed to
be included.

The state or District of Columbia is the default geography. A state result does
not localize searches to an event footprint. Multistate episodes use the
catalog primary state and preserve secondary states. Metro/DMA identifiers stay
empty unless independently verified.

The versioned sampler configuration can require minimum representation for
scientifically important states. Required-state episodes are inserted through
same-year matched replacement, never by increasing the sample size. Matching
prioritizes coastal/inland class, singleton/multi-member class, footprint size,
season, and the explicitly labeled compact-footprint urban proxy. Added and
removed episode IDs, matched attributes, and any eligibility relaxation are
recorded in `trends_pilot_manifest.json`.

Daily windows contain 28 full baseline days, the episode interval, and at least
28 post-event days. Windows are extended to at least 60 days. Hourly and local
time data are excluded to avoid mixing them with UTC daily episode dates.

## National terminology strategy

Terminology dictionary `0.8A-v3` uses one national design; terms are not tuned
by state. Every concept is classified as `primary_broad`,
`secondary_specific`, `exploratory_low_volume`, `context`, or
`anchor_candidate`. Specific concepts from the first dictionary remain
available, but low expected volume is now explicit rather than interpreted as
evidence of no demand.

Every episode uses the same five ordered batches:

1. `weather`, `flood`, `flooding`, `flash flood`, `heavy rain`
2. `weather`, `water damage`, `cleanup`, `mold`, `insurance`
3. `weather`, `FEMA`, `emergency`, `evacuation`, `shelter`
4. `weather`, `outage`, `road closed`, `school closed`, `traffic`
5. `weather`, `flood cleanup`, `water removal`, `flood insurance`, `FEMA assistance`

The configured broad/specific comparisons are FEMA/FEMA assistance,
cleanup/flood cleanup, insurance/flood insurance, shelter/emergency shelter,
evacuation/flood evacuation, mold/mold removal, basement/wet basement,
outage/power outage, outage/no power, road closed/road closure, road
closed/flooded roads, school closed/school closure, roads closed/road closure,
and schools closed/school closure.
Their feasibility table reports shared usable and all-zero episodes, zero
fractions, baseline and event values, event-lift availability, repeat
stability, and deterministic preference flags.

The disruption family uses `outage`, `road closed`, `school closed`, and
`traffic` as broad primary terms. Formal phrases (`power outage`, `road
closure`, and `school closure`) plus `flooded roads` and `no power` remain
secondary comparisons. Plural and alternate phrases (`roads closed`, `schools
closed`, `road flooding`, and `electricity outage`) are exploratory. Standalone
`closure` is excluded because it is highly ambiguous.

Broad terms trade specificity for likely volume. An `outage` increase is not
proof of electricity loss because it may concern internet, cellular, or other
services. `traffic` is not flood-specific without supporting flood or weather
signals. `road closed` and `school closed` are treated as natural-language
alternatives to the formal noun phrases, not as different event outcomes.

## Normalization and suppression

The pipeline preserves raw within-request indices, calculates baseline
mean/median/standard deviation/MAD, guarded peak lifts, anchor ratios, shared
anchor batch diagnostics, and repeated-request stability. It does not calculate
ratios with zero denominators or standardized scores with unsafe variance.

Zero does not prove no searches. Observations retain low-volume, relative-zero,
missing, partial, parse-failure, and unknown-zero classifications. Diagnostics
report zero fractions, zero runs, isolated spikes, incomplete periods, all-zero
series, and unstable anchors. No arbitrary pseudocount is added.

`weather`, `rain`, `storm`, and `heavy rain` are context candidates. Weather is
the positive control in each standardized batch, but it is not a normalization
anchor: storm-related interest may rise during the event window. `news` and
`temperature` are anchor candidates whose nonzero coverage, baseline stability,
seasonal variability, event-period lift, repeated-request stability, zero
fraction, and geographic coverage must be measured. A candidate exceeding the
configured event-lift threshold is labeled `event_sensitive_anchor` and is not
used for ratio normalization. The pipeline never promotes an anchor by raw
interest magnitude alone.

## Event-study extension

Rule version `0.8A-v2` separates the longer baseline from four response phases:

- Anticipatory: seven days before episode start through the preceding day.
- Immediate: episode start through two days after episode end.
- Early recovery: days three through seven after episode end.
- Extended recovery: days eight through 28 after episode end.

Each phase reports mean, maximum, absolute peak lift, and within-series
baseline-standardized lift. The overall peak retains its date, phase, and lead
or lag from episode start. A pre-event peak is preserved as anticipatory
evidence rather than automatically dismissed.

The `behavioral_demand_proxy` family contains retailer brands (Walmart, Home
Depot, and Lowe's), preparation products (generator, bottled water, batteries,
and sandbags), and damage-mitigation products (sump pump, wet vacuum, and
dehumidifier). Each concept records proxy type, interpretation scope,
promotion sensitivity, seasonal sensitivity, and ambiguity. These are candidate
behavioral signals, never flood-specific outcomes. Heat, heat wave, snow,
tornado, and hurricane are optional diagnostic context terms for selected
ambiguous cases only.

Strong event concurrence means peaks are at most two days apart; moderate
concurrence means at most seven days. Correlations require at least 14 aligned
observations. National and matched-control requests are opt-in and limited to
the five-episode mini pilot. National comparisons use two batches containing
weather/flood plus Walmart, Home Depot, Lowe's, generator, and bottled water.
Separate files support role-specific inspection, while
`event_study_request_plan.parquet` combines all 50 optional treated, national,
and control definitions for import validation and downstream metrics.

State and national raw 0-to-100 values are never subtracted. For series `s` and
phase `p`, the comparison is:

`z(s,p) = (phase_mean(s,p) - baseline_mean(s)) / baseline_sd(s)`

`adjusted_lift(p) = treated_z(p) - median(control_z_i(p))`

The implementation rejects cross-geography subtraction unless the declared
scale is `within_series_baseline_standardized_lift`.

Controls are flood-free during the episode window plus a seven-day buffer and
cannot be states intersected by the same multistate episode. Ranking favors the
same Census region, then matching population tier, broad climate class, and
known Trends availability when metadata exist. Deterministic state-code order
breaks ties. These criteria do not establish causal exchangeability.

Peak attribution stores every component flag and uses
`likely_event_associated`, `possibly_event_associated`,
`likely_national_or_promotional`, `weather_related_not_flood_specific`,
`unrelated_or_ambiguous`, and `insufficient_evidence`. A retailer peak is not
direct flood-demand evidence, and a weather peak without flood-awareness
support may represent heat or another nonflood event.

## Real pilot workflow

```powershell
uv run geodemand trends official-api-selfcheck
uv run geodemand trends pilot-sample --catalog-dir C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog\build_a --output-root C:\Work\Data\GeoDemand\trends --rules config\trends_rules.yaml
uv run geodemand trends map-geographies --pilot C:\Work\Data\GeoDemand\trends\manifests\trends_pilot_episodes.parquet --output-root C:\Work\Data\GeoDemand\trends
uv run geodemand trends plan --pilot C:\Work\Data\GeoDemand\trends\manifests\trends_pilot_episodes.parquet --geography C:\Work\Data\GeoDemand\trends\geography\geography_mapping.parquet --terms config\trends_terms.yaml --rules config\trends_rules.yaml --output-root C:\Work\Data\GeoDemand\trends
uv run geodemand trends import-csv --csv EXPORT.csv --sidecar EXPORT.json --output-root C:\Work\Data\GeoDemand\trends
uv run geodemand trends validate-imports --observations C:\Work\Data\GeoDemand\trends\processed\trends_observations.parquet --plan C:\Work\Data\GeoDemand\trends\planning\terminology_mini_pilot_plan.parquet
uv run geodemand trends select-controls --pilot C:\Work\Data\GeoDemand\trends\manifests\trends_pilot_episodes.parquet --catalog C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog\build_a\episodes.parquet --rules config\trends_rules.yaml --output-root C:\Work\Data\GeoDemand\trends
uv run geodemand trends plan --pilot C:\Work\Data\GeoDemand\trends\manifests\trends_pilot_episodes.parquet --geography C:\Work\Data\GeoDemand\trends\geography\geography_mapping.parquet --terms config\trends_terms.yaml --rules config\trends_rules.yaml --output-root C:\Work\Data\GeoDemand\trends --include-behavioral-state --include-national --controls C:\Work\Data\GeoDemand\trends\controls\episode_control_states.parquet
uv run geodemand trends phase-metrics --observations OBSERVATIONS.parquet --plan EVENT_STUDY_PLAN.parquet --terms config\trends_terms.yaml --rules config\trends_rules.yaml --output-root C:\Work\Data\GeoDemand\trends
uv run geodemand trends concurrence --observations OBSERVATIONS.parquet --plan EVENT_STUDY_PLAN.parquet --terms config\trends_terms.yaml --rules config\trends_rules.yaml --phase-metrics PHASE_METRICS.parquet --output-root C:\Work\Data\GeoDemand\trends
uv run geodemand trends control-adjusted-metrics --phase-metrics PHASE_METRICS.parquet --controls C:\Work\Data\GeoDemand\trends\controls\episode_control_states.parquet --output-root C:\Work\Data\GeoDemand\trends
uv run geodemand trends attribute-peaks --phase-metrics PHASE_METRICS.parquet --concurrence CONCURRENCE.parquet --rules config\trends_rules.yaml --control-adjusted CONTROL_ADJUSTED.parquet --national-diagnostics NATIONAL_SPIKES.csv --output-root C:\Work\Data\GeoDemand\trends
```

Start with `terminology_mini_pilot_plan.csv`: five episodes, each using all five
standardized batches. The generated plan has 26 export rows: 25 unique request
definitions plus an exact repeat of Batch 1 for the Florida episode. It covers
Florida, Kansas, Mississippi, Washington, and Wyoming, all four years from 2022
through 2025, and both singleton and multi-member episodes. Do not proceed to
20 episodes until at least two concepts are usable, geography and suppression
are acceptable, and repeated-request behavior is defensible. Do not execute all
40 automatically.

## Current status and limitations

The regenerated full plan contains 200 rows: five identical ordered batches for
each of 40 episodes. The terminology table contains 47 concepts and the sidecar
templates record terminology version `0.8A-v3`. Determinism is checked by
repeating generation and hashing every planning artifact. Two identical
generations of all 235 files produced aggregate SHA-256
`c5c21910ff4448d0388213fa21a0df9b84fc17336a788dac6c46111d618ac8e1`.
These are implementation artifacts, not Trends evidence. Milestone acceptance
still requires at least
five real official exports, three states, three years, one repeat, suppression
and normalization results, terminology feasibility, and episode feasibility.
No synthetic fixture may satisfy those gates.

The opt-in local planning audit selected three controls for each of 40 pilot
episodes. For the five-episode mini pilot it generated 10 treated comparison,
10 national, and 30 control requests. Together with the 25 standard mini-pilot
definitions, the deduplicated total is 75. These are plans only; no additional
Google Trends data were downloaded.

The current pilot requires two Florida episodes. The eligible population
provided both without relaxation: a compact singleton from summer 2025 and a
multi-member moderate-footprint episode from spring 2023. They replaced two
matched Texas episodes, preserving every year and binary balance quota.
