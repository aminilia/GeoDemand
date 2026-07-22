# Controlled Google Trends Browser Export

The `playwright_export_assistant` backend is an experimental operator aid for
official Google Trends Explore CSV exports. It is not an official API, does not
use `pytrends`, and does not replace the canonical manual CSV importer. The
official limited-access API remains preferred when access is available.

## Safety boundary

The assistant uses one visible Chromium-family browser page and processes one
request at a time. Visible supervised mode is the safest mode. It does not
bypass CAPTCHA, rotate proxies, spoof fingerprints, create accounts, capture
credentials, or put cookies or account identifiers in research metadata. It
stops on CAPTCHA, account challenges, unexpected consent flows, and selector
contract failures. Headless mode is experimental and off by default.

A persistent profile is used only with `--profile-dir`. GeoDemand neither reads
profile contents nor writes them to sidecars. Keep profiles outside the
repository or under the ignored `.browser-profiles/` directory.

## Installation and selfcheck

```powershell
uv pip install -e ".[trends-browser]"
uv run python -m playwright install chromium

$ROOT = "C:\Work\Data\GeoDemand\trends"
# Supply either an ordinary plan or an execution-only acquisition plan.
$PLAN = "$ROOT\planning\acquisition_execution_plan.csv"

uv run geodemand trends browser-selfcheck `
  --plan $PLAN `
  --download-dir "$ROOT\raw" `
  --browser chromium `
  --request-id REQUEST_ID
```

Chrome and Edge are available through `--browser chrome` and `--browser edge`;
Chromium is the fallback. Selfcheck parses one planned URL, checks download
directory writability, launches visibly, opens Explore, and reports consent,
login, chart, and chart-download-control status. It does not download data.

## Bounded export

Begin with a dry run and an explicit small bound:

```powershell
uv run geodemand trends export-browser `
  --plan $PLAN `
  --output-root $ROOT `
  --mini-pilot `
  --geography US-FL `
  --batch-id batch_1_flood_awareness `
  --max-requests 1 `
  --supervised `
  --dry-run
```

Remove `--dry-run` only after reviewing the selection. Supervised mode displays
the request ID, geography, range, terms, and verification warnings, then accepts
`download`, `skip`, `retry`, `insufficient_data`, or `stop`. A mismatched page
can be downloaded only after explicit approval. Automatic mode skips it.

Filters may be combined: repeated `--request-id`, `--episode-id`,
`--geography`, `--batch-id`, `--mini-pilot`, and `--max-requests`. Both modes
use one page and a minimum 15-second delay. Repeated logical requests retain the
same request ID but use distinct repeat IDs, filenames, sidecars, and attempt
IDs.

## Download and lineage

The selector adapter finds the download control associated with the exact
`Interest over time` heading. It does not guess among subregion, related-topic,
or related-query downloads. A selector change produces clear manual review.

Downloads enter a unique temporary directory. The shared parser validates the
Interest-over-time structure, date column, ordered labels, row widths, partial
markers, duplicate dates, and values before atomic rename. A valid all-zero
series remains valid. Downloaded bytes are never rewritten. The JSON sidecar
records request lineage, terminology version, hashes, directly observed browser
metadata, and verification results. Authentication material is forbidden.

Attempts are stored at
`<output-root>/manifests/browser_export_manifest.parquet`. Volatile attempt IDs
and timestamps stay in this operational manifest, not deterministic scientific
summaries. An existing CSV is a cache hit only when its browser sidecar also
validates against the plan. Nothing is overwritten without `--overwrite`.

## Recovery and import

```powershell
uv run geodemand trends export-status --plan $PLAN --output-root $ROOT --mini-pilot
uv run geodemand trends export-validate --plan $PLAN --output-root $ROOT --mini-pilot
uv run geodemand trends export-retry --plan $PLAN --output-root $ROOT `
  --request-id REQUEST_ID --max-requests 1 --supervised

uv run geodemand trends import-csv `
  --csv "$ROOT\raw\REQUEST.csv" `
  --sidecar "$ROOT\raw\REQUEST.json" `
  --output-root $ROOT
```

Retry accepts only explicitly selected requests whose latest result is
retryable. CAPTCHA and account challenges are never automatically retried.
`--resume` skips request/repeat pairs already validated or recorded as cache
hits. `import-csv` reads browser sidecars through the same Pydantic contract and
contains no browser-specific scientific logic.

Google can change page structure, consent behavior, rate limits, and download
format without notice. Centralized selectors and offline mocks reduce accidental
downloads, but an operator must verify each small pilot before scaling.
