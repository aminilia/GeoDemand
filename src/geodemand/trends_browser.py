from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
import re
import shutil
import tempfile
import time
import uuid
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand.trends import TrendsError, read_request_plan_rows, validate_trends_csv

BrowserName = Literal["chromium", "chrome", "edge"]
ExportDecision = Literal["download", "skip", "retry", "insufficient_data", "stop"]
SessionFactory = Callable[[BrowserName, bool, Path | None], "BrowserSession"]
DecisionProvider = Callable[[Mapping[str, Any], "PageVerification"], ExportDecision]

BROWSER_AUTOMATION_VERSION = "0.8A-browser-v1"
DEFAULT_DELAY_SECONDS = 15.0
MAX_SERVICE_ERRORS = 2
REQUIRED_PLAN_COLUMNS = {
    "request_id",
    "episode_id",
    "geography",
    "timeframe",
    "target_concepts",
    "expected_output_filename",
    "sidecar_metadata_filename",
    "explore_url",
    "terminology_version",
    "request_status",
}
ALLOWED_STATUSES = {
    "planned",
    "opened",
    "awaiting_user_confirmation",
    "downloaded",
    "validated",
    "skipped_existing",
    "insufficient_data",
    "mismatched_page",
    "captcha_detected",
    "consent_required",
    "login_required",
    "browser_failure",
    "download_failure",
    "validation_failure",
    "manual_review",
    "retryable_failure",
}
COMPLETED_STATUSES = {"validated", "skipped_existing"}
RETRYABLE_STATUSES = {
    "browser_failure",
    "download_failure",
    "validation_failure",
    "retryable_failure",
}


@dataclass(frozen=True)
class PageVerification:
    status: str
    loaded_url: str
    chart_detected: bool
    download_control_detected: bool
    consent_present: bool = False
    login_required: bool = False
    captcha_present: bool = False
    insufficient_data: bool = False
    warnings: tuple[str, ...] = ()

    @property
    def verified(self) -> bool:
        return self.status == "verified" and not self.warnings


@dataclass
class ExportOptions:
    browser: BrowserName = "chromium"
    headless: bool = False
    profile_dir: Path | None = None
    max_requests: int = 1
    request_ids: set[str] = field(default_factory=set)
    episode_id: str | None = None
    geography: str | None = None
    batch_id: str | None = None
    delay_seconds: float = DEFAULT_DELAY_SECONDS
    supervised: bool = False
    resume: bool = False
    dry_run: bool = False
    stop_on_error: bool = False
    stop_on_captcha: bool = True
    overwrite: bool = False
    mini_pilot: bool = False


class BrowserSession(Protocol):
    def open(self, url: str) -> None: ...

    def verify(self, plan: Mapping[str, Any]) -> PageVerification: ...

    def download_interest_over_time(self, directory: Path) -> Path: ...

    def close(self) -> None: ...


class PlaywrightSelectors:
    interest_heading = re.compile(r"^Interest over time$", re.IGNORECASE)
    download_name = re.compile(r"download", re.IGNORECASE)
    captcha_markers = ("captcha", "recaptcha", "unusual traffic")
    consent_markers = ("before you continue", "consent.google", "privacy & terms")
    login_markers = ("accounts.google.com", "verify it's you", "account challenge")
    insufficient_markers = (
        "not enough search volume",
        "doesn't have enough data",
        "insufficient data",
    )
    error_markers = ("something went wrong", "429", "service unavailable", "try again later")


class PlaywrightBrowserSession:
    def __init__(self, browser: BrowserName, headless: bool, profile_dir: Path | None) -> None:
        if importlib.util.find_spec("playwright") is None:
            raise TrendsError(
                "Playwright is not installed. Install the trends-browser optional dependency."
            )
        sync_api = importlib.import_module("playwright.sync_api")
        self._playwright = sync_api.sync_playwright().start()
        browser_type = self._playwright.chromium
        channel = {"chromium": None, "chrome": "chrome", "edge": "msedge"}[browser]
        launch_options: dict[str, Any] = {"headless": headless}
        if channel:
            launch_options["channel"] = channel
        self._browser_handle: Any | None = None
        if profile_dir is not None:
            profile_dir.mkdir(parents=True, exist_ok=True)
            self._context = browser_type.launch_persistent_context(
                str(profile_dir), accept_downloads=True, **launch_options
            )
        else:
            self._browser_handle = browser_type.launch(**launch_options)
            self._context = self._browser_handle.new_context(accept_downloads=True)
        self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        self._download_button: Any | None = None

    def open(self, url: str) -> None:
        self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        self._page.wait_for_timeout(2_000)

    def verify(self, plan: Mapping[str, Any]) -> PageVerification:
        loaded_url = str(self._page.url)
        body = self._page.locator("body").inner_text(timeout=15_000).casefold()
        lowered_url = loaded_url.casefold()
        captcha = any(
            marker in body or marker in lowered_url
            for marker in PlaywrightSelectors.captcha_markers
        )
        consent = any(
            marker in body or marker in lowered_url
            for marker in PlaywrightSelectors.consent_markers
        )
        login = any(
            marker in body or marker in lowered_url for marker in PlaywrightSelectors.login_markers
        )
        insufficient = any(marker in body for marker in PlaywrightSelectors.insufficient_markers)
        service_error = any(marker in body for marker in PlaywrightSelectors.error_markers)
        warnings = list(verify_explore_url(str(plan["explore_url"]), loaded_url))
        heading = self._page.get_by_text(PlaywrightSelectors.interest_heading, exact=True)
        chart_detected = heading.count() == 1
        download_detected = False
        self._download_button = None
        if chart_detected:
            container = heading.locator(
                "xpath=ancestor::*[.//button[contains(translate(@aria-label, "
                "'DOWNLOAD','download'),'download')]][1]"
            )
            button = container.get_by_role("button", name=PlaywrightSelectors.download_name)
            if button.count() == 1:
                self._download_button = button
                download_detected = True
        if captcha:
            status = "captcha_detected"
        elif consent:
            status = "consent_required"
        elif login:
            status = "login_required"
        elif insufficient:
            status = "insufficient_data"
        elif service_error:
            status = "retryable_failure"
        elif not chart_detected or not download_detected:
            status = "selector_contract_failure"
        elif warnings:
            status = "mismatched_page"
        else:
            status = "verified"
        return PageVerification(
            status=status,
            loaded_url=loaded_url,
            chart_detected=chart_detected,
            download_control_detected=download_detected,
            consent_present=consent,
            login_required=login,
            captcha_present=captcha,
            insufficient_data=insufficient,
            warnings=tuple(sorted(warnings)),
        )

    def download_interest_over_time(self, directory: Path) -> Path:
        if self._download_button is None:
            raise TrendsError("selector_contract_failure: Interest over time download missing")
        directory.mkdir(parents=True, exist_ok=True)
        with self._page.expect_download(timeout=60_000) as event:
            self._download_button.click()
        download = event.value
        target = directory / (download.suggested_filename or "trends-export.csv")
        download.save_as(str(target))
        return target

    def close(self) -> None:
        try:
            self._context.close()
            if self._browser_handle is not None:
                self._browser_handle.close()
        finally:
            self._playwright.stop()


def browser_selfcheck(
    plan_path: Path,
    download_dir: Path,
    browser: BrowserName = "chromium",
    headless: bool = False,
    profile_dir: Path | None = None,
    request_id: str | None = None,
    session_factory: SessionFactory | None = None,
) -> dict[str, Any]:
    playwright_available = importlib.util.find_spec("playwright") is not None
    writable = _directory_writable(download_dir)
    plans = filter_request_plan(
        plan_path, request_ids={request_id} if request_id else set(), max_requests=1
    )
    if not plans:
        raise TrendsError("No request matched the browser selfcheck filters.")
    parsed = parse_explore_url(str(plans[0]["explore_url"]))
    result: dict[str, Any] = {
        "playwright_import": playwright_available,
        "browser": browser,
        "headless": headless,
        "visible_browser_requested": not headless,
        "download_directory_writable": writable,
        "explore_url_parsed": bool(parsed["terms"] and parsed["geography"]),
        "download_performed": False,
        "request_id": plans[0]["request_id"],
    }
    if not playwright_available and session_factory is None:
        return {**result, "selfcheck_status": "playwright_not_installed"}
    try:
        session = (session_factory or PlaywrightBrowserSession)(browser, headless, profile_dir)
    except Exception as exc:
        return {
            **result,
            "browser_executable_available": False,
            "selfcheck_status": "browser_failure",
            "failure_reason": str(exc),
        }
    try:
        session.open(str(plans[0]["explore_url"]))
        verification = session.verify(plans[0])
        result.update(
            {
                "browser_executable_available": True,
                "explore_accessible": bool(verification.loaded_url),
                "consent_present": verification.consent_present,
                "login_required": verification.login_required,
                "captcha_present": verification.captcha_present,
                "interest_over_time_detected": verification.chart_detected,
                "download_control_detected": verification.download_control_detected,
                "verification_status": verification.status,
                "verification_warnings": list(verification.warnings),
                "selfcheck_status": "passed"
                if verification.status == "verified"
                else "attention_required",
            }
        )
    except Exception as exc:
        result.update(
            {
                "browser_executable_available": False,
                "selfcheck_status": "browser_failure",
                "failure_reason": str(exc),
            }
        )
    finally:
        session.close()
    return result


def export_browser(  # noqa: PLR0912, PLR0915
    plan_path: Path,
    trends_root: Path,
    options: ExportOptions,
    session_factory: SessionFactory | None = None,
    decision_provider: DecisionProvider | None = None,
    eligible_keys: set[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    plans = filter_request_plan(
        plan_path,
        request_ids=options.request_ids,
        episode_id=options.episode_id,
        geography=options.geography,
        batch_id=options.batch_id,
        max_requests=options.max_requests,
        mini_pilot=options.mini_pilot,
    )
    if eligible_keys is not None:
        plans = [row for row in plans if _request_key(row) in eligible_keys]
    if not plans:
        raise TrendsError("No requests matched the export filters.")
    if options.dry_run:
        return _dry_run_summary(plans, options)
    raw_dir = trends_root / "raw"
    manifest_path = trends_root / "manifests" / "browser_export_manifest.parquet"
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest = _read_manifest(manifest_path)
    latest = _latest_attempts(manifest)
    session: BrowserSession | None = None
    records: list[dict[str, Any]] = []
    service_errors = 0
    try:
        for index, plan in enumerate(plans):
            key = _request_key(plan)
            if options.resume and latest.get(key, {}).get("status") in COMPLETED_STATUSES:
                continue
            attempt = _new_attempt(plan, options.browser, latest.get(key))
            expected_path = raw_dir / str(plan["expected_output_filename"])
            sidecar_path = raw_dir / str(plan["sidecar_metadata_filename"])
            labels = parse_explore_url(str(plan["explore_url"]))["terms"]
            if expected_path.exists() and not options.overwrite:
                try:
                    validation = validate_trends_csv(expected_path, labels)
                    if not sidecar_path.exists():
                        raise TrendsError("Existing CSV has no browser export sidecar.")
                    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
                    _validate_sidecar_lineage(plan, sidecar, validation)
                    attempt.update(
                        _completed_fields(
                            "skipped_existing",
                            expected_path,
                            str(validation["sha256"]),
                            cache_hit=True,
                            verification_status="cached_valid_file",
                        )
                    )
                except TrendsError as exc:
                    attempt.update(_failure_fields("validation_failure", "cache", str(exc)))
                records.append(attempt)
                _write_manifest(manifest_path, [*manifest, *records])
                continue
            if session is None:
                try:
                    session = (session_factory or PlaywrightBrowserSession)(
                        options.browser, options.headless, options.profile_dir
                    )
                except Exception as exc:
                    attempt.update(_failure_fields("browser_failure", "launch", str(exc)))
                    records.append(attempt)
                    break
            attempt["status"] = "opened"
            try:
                session.open(str(plan["explore_url"]))
                verification = session.verify(plan)
            except Exception as exc:
                attempt.update(_failure_fields("browser_failure", "open", str(exc)))
                records.append(attempt)
                if options.stop_on_error:
                    break
                continue
            attempt["verification_status"] = verification.status
            attempt["manual_review_flag"] = not verification.verified
            supervised_approved = False
            if verification.status in {
                "captcha_detected",
                "consent_required",
                "login_required",
                "insufficient_data",
                "mismatched_page",
            }:
                if verification.status == "mismatched_page" and options.supervised:
                    attempt["status"] = "awaiting_user_confirmation"
                    decision = (decision_provider or _console_decision)(plan, verification)
                    if decision == "download":
                        supervised_approved = True
                    else:
                        status = (
                            "insufficient_data"
                            if decision == "insufficient_data"
                            else "retryable_failure"
                            if decision == "retry"
                            else "manual_review"
                        )
                        attempt.update(_failure_fields(status, "supervision", f"user_{decision}"))
                        records.append(attempt)
                        if decision == "stop":
                            break
                        continue
                else:
                    attempt.update(
                        _failure_fields(
                            verification.status,
                            "page_verification",
                            ";".join(verification.warnings) or verification.status,
                        )
                    )
                    records.append(attempt)
                    if verification.status == "captcha_detected" and options.stop_on_captcha:
                        break
                    if verification.status in {"login_required", "consent_required"}:
                        break
                    continue
            if verification.status in {"retryable_failure", "selector_contract_failure"}:
                status = (
                    "retryable_failure"
                    if verification.status == "retryable_failure"
                    else "manual_review"
                )
                attempt.update(_failure_fields(status, "page_verification", verification.status))
                records.append(attempt)
                service_errors += int(status == "retryable_failure")
                if status == "manual_review" or service_errors >= MAX_SERVICE_ERRORS:
                    break
                continue
            if options.supervised and not supervised_approved:
                attempt["status"] = "awaiting_user_confirmation"
                decision = (decision_provider or _console_decision)(plan, verification)
                if decision == "stop":
                    attempt.update(_failure_fields("manual_review", "supervision", "user_stop"))
                    records.append(attempt)
                    break
                if decision == "skip":
                    attempt.update(_failure_fields("manual_review", "supervision", "user_skip"))
                    records.append(attempt)
                    continue
                if decision == "insufficient_data":
                    attempt.update(
                        _failure_fields(
                            "insufficient_data", "supervision", "user_marked_insufficient"
                        )
                    )
                    records.append(attempt)
                    continue
                if decision == "retry":
                    attempt.update(
                        _failure_fields("retryable_failure", "supervision", "user_requested_retry")
                    )
                    records.append(attempt)
                    continue
            with tempfile.TemporaryDirectory(prefix="geodemand-trends-", dir=raw_dir) as temporary:
                try:
                    downloaded = session.download_interest_over_time(Path(temporary))
                    attempt["status"] = "downloaded"
                    staged = raw_dir / f".{expected_path.name}.{attempt['export_attempt_id']}.part"
                    shutil.copyfile(downloaded, staged)
                    validation = validate_trends_csv(staged, labels)
                    if expected_path.exists() and options.overwrite:
                        expected_path.unlink()
                    os.replace(staged, expected_path)
                    _write_browser_sidecar(
                        plan,
                        plan_path,
                        expected_path,
                        sidecar_path,
                        attempt,
                        options.browser,
                        verification,
                        labels,
                    )
                    attempt.update(
                        _completed_fields(
                            "validated",
                            expected_path,
                            str(validation["sha256"]),
                            cache_hit=False,
                            verification_status="verified",
                        )
                    )
                except Exception as exc:
                    _cleanup_staged(raw_dir, expected_path.name)
                    stage = "validation" if isinstance(exc, TrendsError) else "download"
                    status = "validation_failure" if stage == "validation" else "download_failure"
                    attempt.update(_failure_fields(status, stage, str(exc)))
            records.append(attempt)
            _write_manifest(manifest_path, [*manifest, *records])
            if attempt["status"] not in COMPLETED_STATUSES and options.stop_on_error:
                break
            if index < len(plans) - 1 and options.delay_seconds > 0:
                time.sleep(options.delay_seconds)
    finally:
        if session is not None:
            session.close()
        _write_manifest(manifest_path, [*manifest, *records])
    return _export_summary(plans, records, manifest_path)


def export_status(plan_path: Path, trends_root: Path, mini_pilot: bool = False) -> dict[str, Any]:
    plans = filter_request_plan(plan_path, mini_pilot=mini_pilot)
    manifest_path = trends_root / "manifests" / "browser_export_manifest.parquet"
    manifest = _read_manifest(manifest_path)
    latest = _latest_attempts(manifest)
    statuses = Counter(str(row["status"]) for row in latest.values())
    bytes_downloaded = sum(
        int(row.get("byte_count") or 0)
        for row in latest.values()
        if row.get("status") in COMPLETED_STATUSES
    )
    return {
        "planned": len(plans),
        "completed": sum(statuses[item] for item in COMPLETED_STATUSES),
        "validated": statuses["validated"],
        "skipped_existing": statuses["skipped_existing"],
        "failed": sum(
            count
            for status, count in statuses.items()
            if status not in COMPLETED_STATUSES | {"planned", "opened"}
        ),
        "retryable": sum(statuses[item] for item in RETRYABLE_STATUSES),
        "manual_review": statuses["manual_review"] + statuses["mismatched_page"],
        "captcha_stopped": statuses["captcha_detected"],
        "bytes_downloaded": bytes_downloaded,
        "requests_by_geography": dict(
            sorted(Counter(str(row["geography"]) for row in plans).items())
        ),
        "requests_by_terminology_version": dict(
            sorted(Counter(str(row["terminology_version"]) for row in plans).items())
        ),
        "statuses": dict(sorted(statuses.items())),
        "manifest": str(manifest_path),
    }


def export_retry(
    plan_path: Path,
    trends_root: Path,
    request_ids: set[str],
    options: ExportOptions,
    session_factory: SessionFactory | None = None,
    decision_provider: DecisionProvider | None = None,
) -> dict[str, Any]:
    if not request_ids:
        raise TrendsError("export-retry requires at least one explicit request ID.")
    manifest_path = trends_root / "manifests" / "browser_export_manifest.parquet"
    latest = _latest_attempts(_read_manifest(manifest_path))
    eligible = {
        key
        for key, row in latest.items()
        if key[0] in request_ids and row.get("status") in RETRYABLE_STATUSES
    }
    if not eligible:
        raise TrendsError("No explicitly selected requests have retryable status.")
    options.request_ids = request_ids
    return export_browser(
        plan_path,
        trends_root,
        options,
        session_factory,
        decision_provider,
        eligible,
    )


def export_validate(
    plan_path: Path,
    trends_root: Path,
    request_ids: set[str] | None = None,
    mini_pilot: bool = False,
) -> dict[str, Any]:
    plans = filter_request_plan(plan_path, request_ids=request_ids or set(), mini_pilot=mini_pilot)
    raw_dir = trends_root / "raw"
    valid = 0
    missing = 0
    failures: list[dict[str, str]] = []
    for plan in plans:
        csv_path = raw_dir / str(plan["expected_output_filename"])
        sidecar_path = raw_dir / str(plan["sidecar_metadata_filename"])
        if not csv_path.exists() or not sidecar_path.exists():
            missing += 1
            continue
        try:
            labels = parse_explore_url(str(plan["explore_url"]))["terms"]
            validation = validate_trends_csv(csv_path, labels)
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            _validate_sidecar_lineage(plan, sidecar, validation)
            valid += 1
        except (TrendsError, ValueError, json.JSONDecodeError) as exc:
            failures.append({"request_id": str(plan["request_id"]), "reason": str(exc)})
    return {
        "request_count": len(plans),
        "validated": valid,
        "missing": missing,
        "validation_failures": failures,
        "valid": not failures and missing == 0,
    }


def filter_request_plan(  # noqa: PLR0913
    plan_path: Path,
    request_ids: set[str] | None = None,
    episode_id: str | None = None,
    geography: str | None = None,
    batch_id: str | None = None,
    max_requests: int | None = None,
    mini_pilot: bool = False,
) -> list[dict[str, Any]]:
    source = _mini_pilot_path(plan_path) if mini_pilot else plan_path
    rows = _read_plan(source)
    if not rows:
        raise TrendsError(f"Request plan is empty: {source}")
    missing = sorted(REQUIRED_PLAN_COLUMNS - set(rows[0]))
    if missing:
        raise TrendsError("Request plan is missing required columns: " + ", ".join(missing))
    selected = [
        row
        for row in rows
        if (not request_ids or str(row["request_id"]) in request_ids)
        and (episode_id is None or str(row["episode_id"]) == episode_id)
        and (geography is None or str(row["geography"]) == geography)
        and (batch_id is None or str(row.get("batch_id")) == batch_id)
    ]
    selected = sorted(
        selected,
        key=lambda row: (
            str(row["episode_id"]),
            str(row["geography"]),
            int(row.get("batch_index") or 0),
            str(row.get("planned_repeat_id") or "initial"),
            str(row["request_id"]),
        ),
    )
    return selected[:max_requests] if max_requests is not None else selected


def parse_explore_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    terms = query.get("q", [""])[0].split(",") if query.get("q") else []
    timeframe = query.get("date", [""])[0]
    return {
        "scheme": parsed.scheme,
        "host": parsed.netloc,
        "geography": query.get("geo", [""])[0],
        "timeframe": timeframe,
        "terms": terms,
        "category": query.get("cat", ["0"])[0],
        "search_property": query.get("gprop", ["web_search"])[0] or "web_search",
    }


def verify_explore_url(expected_url: str, actual_url: str) -> tuple[str, ...]:
    expected = parse_explore_url(expected_url)
    actual = parse_explore_url(actual_url)
    warnings = []
    for key in ("geography", "timeframe", "terms", "category", "search_property"):
        if expected[key] != actual[key]:
            warnings.append(f"{key}_mismatch")
    if "trends.google." not in str(actual["host"]):
        warnings.append("unexpected_host")
    return tuple(sorted(warnings))


def deterministic_manifest_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    volatile = {"export_attempt_id", "started_at", "completed_at"}
    return {key: row[key] for key in sorted(row) if key not in volatile}


def _write_browser_sidecar(  # noqa: PLR0913
    plan: Mapping[str, Any],
    plan_path: Path,
    csv_path: Path,
    sidecar_path: Path,
    attempt: Mapping[str, Any],
    browser: BrowserName,
    verification: PageVerification,
    labels: Sequence[str],
) -> None:
    template_path = plan_path.parent / "sidecars" / str(plan["sidecar_metadata_filename"])
    payload = (
        json.loads(template_path.read_text(encoding="utf-8"))
        if template_path.exists()
        else _minimal_sidecar(plan, labels)
    )
    now = datetime.now(UTC)
    payload.update(
        {
            "request_id": plan["request_id"],
            "repeat_id": plan.get("planned_repeat_id") or "initial",
            "export_attempt_id": attempt["export_attempt_id"],
            "backend": "playwright_export_assistant",
            "episode_id": plan["episode_id"],
            "requested_labels": list(labels),
            "geography": plan["geography"],
            "geography_level": plan.get("geography_level", "state"),
            "start_date": str(plan.get("request_start_date") or str(plan["timeframe"]).split()[0]),
            "end_date": str(plan.get("request_end_date") or str(plan["timeframe"]).split()[-1]),
            "category": plan.get("category", 0),
            "search_property": plan.get("search_property", "web_search"),
            "source_filename": csv_path.name,
            "export_date": now.date().isoformat(),
            "export_timestamp_utc": now.isoformat(),
            "terminology_version": plan["terminology_version"],
            "request_plan_sha256": _sha256(plan_path),
            "csv_sha256": _sha256(csv_path),
            "browser_type": browser,
            "browser_automation_version": (
                f"{BROWSER_AUTOMATION_VERSION};playwright={playwright_version() or 'unknown'}"
            ),
            "verification_status": verification.status,
            "verification_warnings": list(verification.warnings),
        }
    )
    forbidden = {
        "cookies",
        "authentication_tokens",
        "profile_contents",
        "google_account_identifier",
        "credentials",
        "password",
    }
    for key in forbidden:
        payload.pop(key, None)
    sidecar_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _minimal_sidecar(plan: Mapping[str, Any], labels: Sequence[str]) -> dict[str, Any]:
    concept_ids = [item for item in str(plan.get("concept_ids", "")).split(";") if item]
    if len(concept_ids) != len(labels):
        concept_ids = [f"query_{index}" for index in range(1, len(labels) + 1)]
    return {
        "requested_concepts": concept_ids,
        "requested_labels": list(labels),
        "query_types": ["term"] * len(labels),
        "semantic_families": ["unknown"] * len(labels),
        "anchor_concept_id": plan.get("anchor_concept"),
        "request_role": plan.get("request_role", "treated_state"),
        "treated_geography": plan.get("treated_geography", plan["geography"]),
        "batch_id": plan.get("batch_id"),
        "comparison_batch_id": plan.get("comparison_batch_id", plan.get("batch_id")),
        "notes": "Generated by the controlled visible-browser export assistant.",
    }


def _validate_sidecar_lineage(
    plan: Mapping[str, Any], sidecar: Mapping[str, Any], validation: Mapping[str, Any]
) -> None:
    timeframe = str(plan["timeframe"]).split()
    checks = {
        "request_id": str(plan["request_id"]),
        "repeat_id": str(plan.get("planned_repeat_id") or "initial"),
        "episode_id": str(plan["episode_id"]),
        "geography": str(plan["geography"]),
        "start_date": str(plan.get("request_start_date") or timeframe[0]),
        "end_date": str(plan.get("request_end_date") or timeframe[-1]),
        "category": str(plan.get("category", 0)),
        "search_property": str(plan.get("search_property", "web_search")),
        "terminology_version": str(plan["terminology_version"]),
        "csv_sha256": str(validation["sha256"]),
    }
    for key, expected in checks.items():
        if str(sidecar.get(key)) != expected:
            raise TrendsError(f"Sidecar {key} does not match the request plan or CSV.")
    if sidecar.get("backend") != "playwright_export_assistant":
        raise TrendsError("Sidecar backend is not playwright_export_assistant.")
    expected_labels = parse_explore_url(str(plan["explore_url"]))["terms"]
    if list(sidecar.get("requested_labels") or []) != expected_labels:
        raise TrendsError("Sidecar term ordering does not match the request plan.")


def _new_attempt(
    plan: Mapping[str, Any], browser: BrowserName, previous: Mapping[str, Any] | None
) -> dict[str, Any]:
    started = datetime.now(UTC).isoformat()
    return {
        "export_attempt_id": uuid.uuid4().hex,
        "request_id": str(plan["request_id"]),
        "repeat_id": str(plan.get("planned_repeat_id") or "initial"),
        "episode_id": str(plan["episode_id"]),
        "geography": str(plan["geography"]),
        "batch_id": str(plan.get("batch_id") or ""),
        "explore_url_hash": hashlib.sha256(str(plan["explore_url"]).encode()).hexdigest(),
        "expected_filename": str(plan["expected_output_filename"]),
        "actual_filename": None,
        "started_at": started,
        "completed_at": None,
        "status": "planned",
        "csv_sha256": None,
        "byte_count": 0,
        "browser": browser,
        "cache_hit": False,
        "verification_status": "not_started",
        "failure_stage": None,
        "failure_reason": None,
        "retry_count": int(previous.get("retry_count") or 0) + 1 if previous else 0,
        "manual_review_flag": False,
        "terminology_version": str(plan["terminology_version"]),
    }


def _completed_fields(
    status: str,
    path: Path,
    checksum: str,
    cache_hit: bool,
    verification_status: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "actual_filename": path.name,
        "completed_at": datetime.now(UTC).isoformat(),
        "csv_sha256": checksum,
        "byte_count": path.stat().st_size,
        "cache_hit": cache_hit,
        "verification_status": verification_status,
        "failure_stage": None,
        "failure_reason": None,
        "manual_review_flag": False,
    }


def _failure_fields(status: str, stage: str, reason: str) -> dict[str, Any]:
    if status not in ALLOWED_STATUSES and status != "selector_contract_failure":
        raise TrendsError(f"Unsupported browser export status: {status}")
    return {
        "status": status,
        "completed_at": datetime.now(UTC).isoformat(),
        "failure_stage": stage,
        "failure_reason": reason,
        "manual_review_flag": status
        in {"mismatched_page", "manual_review", "consent_required", "login_required"},
    }


def _export_summary(
    plans: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    manifest_path: Path,
) -> dict[str, Any]:
    statuses = Counter(str(row["status"]) for row in records)
    return {
        "selected_request_count": len(plans),
        "attempted_request_count": len(records),
        "statuses": dict(sorted(statuses.items())),
        "validated": statuses["validated"],
        "skipped_existing": statuses["skipped_existing"],
        "bytes_downloaded": sum(int(row.get("byte_count") or 0) for row in records),
        "manifest": str(manifest_path),
    }


def _dry_run_summary(plans: Sequence[Mapping[str, Any]], options: ExportOptions) -> dict[str, Any]:
    return {
        "dry_run": True,
        "browser_opened": False,
        "selected_request_count": len(plans),
        "request_ids": [str(row["request_id"]) for row in plans],
        "repeat_ids": [str(row.get("planned_repeat_id") or "initial") for row in plans],
        "browser": options.browser,
        "headless": options.headless,
        "supervised": options.supervised,
    }


def _console_decision(plan: Mapping[str, Any], verification: PageVerification) -> ExportDecision:
    terms = ", ".join(parse_explore_url(str(plan["explore_url"]))["terms"])
    print(f"Request {plan['request_id']} | {plan['geography']} | {plan['timeframe']} | {terms}")
    print(f"Verification: {verification.status}; warnings={list(verification.warnings)}")
    while True:
        decision = input("Decision [download/skip/retry/insufficient_data/stop]: ").strip()
        if decision in {"download", "skip", "retry", "insufficient_data", "stop"}:
            return cast(ExportDecision, decision)


def _directory_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path, delete=True):
            pass
    except OSError:
        return False
    return True


def _mini_pilot_path(plan_path: Path) -> Path:
    candidates = [
        plan_path.parent / "terminology_mini_pilot_plan.parquet",
        plan_path.parent / "terminology_mini_pilot_plan.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise TrendsError("Mini-pilot plan is missing beside the supplied request plan.")


def _read_plan(path: Path) -> list[dict[str, Any]]:
    return read_request_plan_rows(path, required_fields=REQUIRED_PLAN_COLUMNS)


def _read_manifest(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _write_manifest(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            str(row["request_id"]),
            str(row["repeat_id"]),
            str(row["started_at"]),
            str(row["export_attempt_id"]),
        ),
    )
    temporary = path.with_suffix(".tmp.parquet")
    pq.write_table(pa.Table.from_pylist(ordered), temporary, compression="zstd")
    os.replace(temporary, path)


def _latest_attempts(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    output: dict[tuple[str, str], dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: str(item.get("started_at") or "")):
        output[(str(row["request_id"]), str(row["repeat_id"]))] = dict(row)
    return output


def _request_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return str(row["request_id"]), str(row.get("planned_repeat_id") or "initial")


def _cleanup_staged(directory: Path, expected_name: str) -> None:
    for path in directory.glob(f".{expected_name}.*.part"):
        path.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def playwright_version() -> str | None:
    try:
        return importlib.metadata.version("playwright")
    except importlib.metadata.PackageNotFoundError:
        return None
