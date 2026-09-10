# GeoDemand 1.2 local release validation

Validated on Windows on 2026-09-09 from the cleaned source base
`f4a8faeff823fe01f8db2807165d0f9321a82b58`, using uv.lock without changes.
The final documentation commit is recorded in the local package manifest.

| Runtime | Full offline suite | Configured mypy target 3.11 |
|---|---|---|
| CPython 3.11.15 | 250 passed, 2 skipped | Passed, 23 source files |
| CPython 3.12.13 | 250 passed, 2 skipped | Passed, 23 source files |
| CPython 3.13.13 | 250 passed, 2 skipped | Passed, 23 source files |

The earlier NumPy-stub type-check blocker was not reproduced in the locked isolated
environments. No lowering of supported Python versions or suppression of type errors
was necessary. Runtime tests were executed locally on Windows; Linux CI was not run
by this local release-preparation task.

Ruff lint passed, formatting passed for 52 files, and documented CLI validation passed
for 55 help paths and 89 documented commands. Git whitespace checks passed. Source
distribution and wheel builds passed. Scientific source, configuration and lockfile
were unchanged by milestone 1.2; the commit updates reporting and documentation.

Both prior official-only runs were hash-checked and a fresh two-run reproduction
produced 21 byte-identical files per run. Their analytical Parquet content hashes agree
with the earlier release-candidate outputs. The supplementary builder recomputed all
80 episode/version records, verified input hashes, and produced 12 byte-identical files
across two runs. The two supplementary PNG figures were visually inspected.

No raw research input was modified. Raw acquisition stores are excluded from release
packages. The local analytical bundle retains separate official-only and exploratory
provenance; it is not a public data release. A read-only GitHub check found main and
v0.1.0 at the cleaned revisions; no remote push or tag change was performed.

The final scientific endpoint is descriptive: five official-subset weather units,
22 exploratory flood units and 19 shared-version episodes. There is no eligible
control contrast or fitted prediction result. Counts are not pooled.
