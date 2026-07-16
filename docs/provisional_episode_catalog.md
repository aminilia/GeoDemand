# Provisional Physically Informed Episode Catalog

Milestone 0.7C creates a **provisional physically informed episode catalog**.
It is not a ground-truth, confirmed, or fully physically validated flood catalog.

## Evidence hierarchy

Conservative-policy connected components are the immutable starting units.
Their membership minimizes chaining risk and provides complete lineage.
Balanced-policy components identify conservative episodes that may belong to a
shared system, but never cause an automatic merge. Broad-policy output remains
diagnostic only.

MRMS is the primary physical evidence. Adequate coverage and quality are
required before physical split or merge candidacy. Missing MRMS is `pending`,
not negative evidence. USGS gauge response can strengthen a decision, but does
not independently split or merge episodes. `no_suitable_gauge` leaves
hydrologic support null. IMERG is explicitly `deferred` in this milestone.

## Deterministic rules and identifiers

Rules and thresholds are versioned in `config/catalog_rules.yaml`; every build
writes the resolved configuration. A provisional ID is SHA-256 over the rule
version and sorted final member event IDs. Every episode and membership row
retains its conservative and balanced lineage.

Clustering-only size, duration, state-count, distance, bridge, geometry, and
split-boundary signals produce manual review. They never split membership.
Balanced relationships without adequate MRMS also produce review, not merges.
MRMS-supported split candidacy requires adequate coverage and quality plus at
least two configured incoherence conditions. Merge candidacy requires all
related conservative episodes to share a split and have adequate, temporally
compatible MRMS evidence. Candidate status does not itself reassign members.

## Splits and review

The episode start date assigns development (2022-2023), validation (2024), or
test (2025). A balanced relationship spanning periods is flagged and remains
separate. Validation checks unique event membership, stable IDs, lineage,
valid non-empty geometry, one evidence row and split assignment per episode,
and membership split consistency.

The deterministic review queue contains no fabricated reviewer values. Edit a
copy of `manual_review_template.csv`, then use `catalog apply-decisions`.
Unknown IDs, duplicate decisions, decisions without reviewer/reason, and splits
that omit or duplicate members are rejected.

## Reproduction

```powershell
uv run geodemand catalog inspect --cohort-root C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full --episode-root C:\Work\Data\GeoDemand\artifacts\episode_policy_full --comparison-root C:\Work\Data\GeoDemand\artifacts\episode_comparison_full --mrms-root C:\Work\Data\GeoDemand\mrms --usgs-root C:\Work\Data\GeoDemand\usgs
uv run geodemand catalog build-provisional --cohort-root C:\Work\Data\GeoDemand\artifacts\candidate_event_cohort_full --episode-root C:\Work\Data\GeoDemand\artifacts\episode_policy_full --comparison-root C:\Work\Data\GeoDemand\artifacts\episode_comparison_full --mrms-root C:\Work\Data\GeoDemand\mrms --usgs-root C:\Work\Data\GeoDemand\usgs --rules config\catalog_rules.yaml --output-dir C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog
uv run geodemand catalog validate --catalog-dir C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog
uv run geodemand catalog summarize --catalog-dir C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog
uv run geodemand catalog review-queue --catalog-dir C:\Work\Data\GeoDemand\artifacts\provisional_episode_catalog
```

Outputs are local derived research artifacts and must not be committed. Final
validation still requires broader MRMS execution, scientific review of
candidates, and resolution of the review queue.

## Full-build result

The full Milestone 0.7C build consumed 88,515 eligible records and produced
37,053 provisional episodes with 88,515 unique membership rows. Validation was
balanced with zero duplicate memberships and zero cross-split leakage. There
were 19,030 retained provisional episodes and 18,023 manual-review episodes.
No episode received a physical merge, split, or support status because MRMS
episode metrics were not available. One conservative episode had available
USGS summary evidence; all 37,053 episodes record IMERG as deferred.

Review reasons are not mutually exclusive: 15,825 possible-undermerge, 4,173
bridge-chaining, 540 geometry-review, 68 large-cluster, 68 maximum-distance, 59
long-duration, 9 high-state-count, and 8 split-boundary flags. Two independent
full builds produced identical SHA-256 hashes for all 20 generated files.

Milestone 0.8A samples only retained, non-review, non-boundary episodes for
search-interest feasibility. This sampling choice reduces initial ambiguity but
does not turn provisional episodes into confirmed flood ground truth.
