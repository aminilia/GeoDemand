# Candidate U.S. Flood Cohort

The cohort step filters audited and spatially enriched Groundsource records into
candidate U.S. reported flood-event records for episode clustering.

```bash
geodemand cohort inspect --events PATH --state-overlaps PATH
geodemand cohort build --events PATH --state-overlaps PATH --output-dir PATH
```

The cohort records event identifiers, dates, geometry, representative coordinates,
state overlaps, review flags, and deterministic split assignments. Records remain
reported-footprint research units and may contain duplicate or spatially broad reports.

Downstream steps use this cohort for conservative, balanced, and broad clustering plus
Google Trends request planning.
