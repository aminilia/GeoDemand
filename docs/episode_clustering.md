# Episode Clustering

Episode clustering groups candidate reported flood records using deterministic spatial
and temporal policies.

```bash
geodemand episodes inspect --events PATH --event-states PATH
geodemand episodes build --events PATH --event-states PATH --output-dir PATH --policy conservative
geodemand episodes build --events PATH --event-states PATH --output-dir PATH --policy balanced
geodemand episodes build --events PATH --event-states PATH --output-dir PATH --policy broad
geodemand episodes compare --events PATH --event-states PATH --output-dir PATH
```

The conservative policy anchors the provisional catalog. Balanced and broad policies
provide sensitivity evidence for possible split and merge candidates. Nested-policy
agreement metrics are written separately from policy-level summaries so comparisons are
explicit and deterministic.

Clustering does not prove that grouped reports describe one event. It creates reproducible
candidate episodes for review and search-interest event-study analysis.
