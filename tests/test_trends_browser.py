from __future__ import annotations

import csv
import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from geodemand.cli import app
from geodemand.trends import import_csv_export
from geodemand.trends_browser import (
    ExportOptions,
    PageVerification,
    browser_selfcheck,
    deterministic_manifest_fields,
    export_browser,
    export_retry,
    export_status,
    export_validate,
    filter_request_plan,
    parse_explore_url,
    verify_explore_url,
)


class MockSession:
    def __init__(
        self,
        verification: PageVerification | None = None,
        malformed: bool = False,
    ) -> None:
        self.verification = verification or _verified()
        self.malformed = malformed
        self.opened: list[str] = []
        self.closed = False
        self.download_count = 0

    def open(self, url: str) -> None:
        self.opened.append(url)

    def verify(self, plan: Mapping[str, Any]) -> PageVerification:
        return self.verification

    def download_interest_over_time(self, directory: Path) -> Path:
        self.download_count += 1
        output = directory / "multiTimeline.csv"
        if self.malformed:
            output.write_text("not,a,trends,export\n", encoding="utf-8")
        else:
            _write_export(output)
        return output

    def close(self) -> None:
        self.closed = True


def test_request_filters_and_mini_pilot_are_bounded(tmp_path: Path) -> None:
    plan = _plan(tmp_path, count=4)
    rows = filter_request_plan(
        plan,
        episode_id="episode-2",
        geography="US-FL",
        batch_id="batch-1",
        max_requests=1,
    )
    assert [row["request_id"] for row in rows] == ["request-2"]

    mini = plan.parent / "terminology_mini_pilot_plan.csv"
    _write_plan(mini, [_plan_row(9, repeat_id="repeat_2")])
    selected = filter_request_plan(plan, mini_pilot=True)
    assert [(row["request_id"], row["planned_repeat_id"]) for row in selected] == [
        ("request-9", "repeat_2")
    ]


def test_dry_run_preserves_visible_default_and_never_opens_browser(tmp_path: Path) -> None:
    plan = _plan(tmp_path, count=2)
    result = export_browser(
        plan,
        tmp_path / "trends",
        ExportOptions(max_requests=1, dry_run=True),
        session_factory=lambda *_: pytest.fail("dry-run launched a browser"),
    )
    assert result["browser_opened"] is False
    assert result["headless"] is False
    assert result["selected_request_count"] == 1


def test_url_metadata_and_term_order_verification() -> None:
    expected = _plan_row(1)["explore_url"]
    parsed = parse_explore_url(expected)
    assert parsed["terms"] == ["weather", "flood", "heavy rain"]
    assert parsed["geography"] == "US-FL"
    assert parsed["timeframe"] == "2024-01-01 2024-03-01"
    assert verify_explore_url(expected, expected) == ()

    mismatched = expected.replace("US-FL", "US-TX").replace(
        "weather%2Cflood%2Cheavy+rain", "flood%2Cweather%2Cheavy+rain"
    )
    assert verify_explore_url(expected, mismatched) == (
        "geography_mismatch",
        "terms_mismatch",
    )


def test_supervised_download_is_atomic_and_sidecar_is_safe(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    root = tmp_path / "trends"
    session = MockSession()
    result = export_browser(
        plan,
        root,
        ExportOptions(supervised=True, delay_seconds=0),
        session_factory=lambda *_: session,
        decision_provider=lambda *_: "download",
    )
    row = _manifest(root)[0]
    output = root / "raw" / "request-1.csv"
    sidecar_path = root / "raw" / "request-1.json"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))

    assert result["validated"] == 1
    assert row["csv_sha256"] == _sha256(output)
    assert row["byte_count"] == output.stat().st_size
    assert not list((root / "raw").glob("*.part"))
    assert sidecar["backend"] == "playwright_export_assistant"
    assert sidecar["repeat_id"] == "initial"
    assert sidecar["requested_labels"] == ["weather", "flood", "heavy rain"]
    assert sidecar["csv_sha256"] == row["csv_sha256"]
    assert not ({"cookies", "credentials", "password"} & set(sidecar))
    assert session.closed

    imported = import_csv_export(output, sidecar_path, tmp_path / "imported")
    assert imported["imported_rows"] == 6


def test_supervised_skip_does_not_download(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    session = MockSession()
    result = export_browser(
        plan,
        tmp_path / "trends",
        ExportOptions(supervised=True, delay_seconds=0),
        session_factory=lambda *_: session,
        decision_provider=lambda *_: "skip",
    )
    assert result["statuses"] == {"manual_review": 1}
    assert session.download_count == 0


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("captcha_detected", "captcha_detected"),
        ("consent_required", "consent_required"),
        ("login_required", "login_required"),
        ("selector_contract_failure", "manual_review"),
        ("mismatched_page", "mismatched_page"),
    ],
)
def test_page_failures_never_download(tmp_path: Path, status: str, expected: str) -> None:
    plan = _plan(tmp_path)
    session = MockSession(
        PageVerification(
            status=status,
            loaded_url="https://trends.google.com/trends/explore",
            chart_detected=status != "selector_contract_failure",
            download_control_detected=status != "selector_contract_failure",
            captcha_present=status == "captcha_detected",
            consent_present=status == "consent_required",
            login_required=status == "login_required",
            warnings=("geography_mismatch",) if status == "mismatched_page" else (),
        )
    )
    result = export_browser(
        plan,
        tmp_path / "trends",
        ExportOptions(delay_seconds=0),
        session_factory=lambda *_: session,
    )
    assert result["statuses"] == {expected: 1}
    assert session.download_count == 0


def test_all_zero_csv_is_valid_but_malformed_csv_is_quarantined_as_failure(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    valid_root = tmp_path / "valid"
    result = export_browser(
        plan,
        valid_root,
        ExportOptions(delay_seconds=0),
        session_factory=lambda *_: MockSession(),
    )
    assert result["validated"] == 1

    invalid_root = tmp_path / "invalid"
    invalid = export_browser(
        plan,
        invalid_root,
        ExportOptions(delay_seconds=0),
        session_factory=lambda *_: MockSession(malformed=True),
    )
    assert invalid["statuses"] == {"validation_failure": 1}
    assert not (invalid_root / "raw" / "request-1.csv").exists()
    assert not list((invalid_root / "raw").glob("*.part"))


def test_existing_valid_export_is_a_cache_hit_without_browser(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    root = tmp_path / "trends"
    first = export_browser(
        plan,
        root,
        ExportOptions(delay_seconds=0),
        session_factory=lambda *_: MockSession(),
    )
    original_hash = _sha256(root / "raw" / "request-1.csv")
    second = export_browser(
        plan,
        root,
        ExportOptions(delay_seconds=0),
        session_factory=lambda *_: pytest.fail("cache hit launched a browser"),
    )
    assert first["validated"] == 1
    assert second["skipped_existing"] == 1
    assert _manifest(root)[-1]["cache_hit"] is True
    assert _sha256(root / "raw" / "request-1.csv") == original_hash


def test_repeat_ids_are_distinct_and_resume_skips_completed(tmp_path: Path) -> None:
    plan = tmp_path / "planning" / "plan.csv"
    first = _plan_row(1, repeat_id="repeat_1")
    second = _plan_row(1, repeat_id="repeat_2")
    second["expected_output_filename"] = "request-1-repeat-2.csv"
    second["sidecar_metadata_filename"] = "request-1-repeat-2.json"
    _write_plan(plan, [first, second])
    root = tmp_path / "trends"
    session = MockSession()
    result = export_browser(
        plan,
        root,
        ExportOptions(max_requests=2, delay_seconds=0),
        session_factory=lambda *_: session,
    )
    assert result["validated"] == 2
    assert {row["repeat_id"] for row in _manifest(root)} == {"repeat_1", "repeat_2"}
    assert len({row["export_attempt_id"] for row in _manifest(root)}) == 2

    resumed = export_browser(
        plan,
        root,
        ExportOptions(max_requests=2, resume=True, delay_seconds=0),
        session_factory=lambda *_: pytest.fail("resume launched a browser"),
    )
    assert resumed["attempted_request_count"] == 0


def test_status_retry_validate_and_deterministic_fields(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    root = tmp_path / "trends"
    export_browser(
        plan,
        root,
        ExportOptions(delay_seconds=0),
        session_factory=lambda *_: MockSession(malformed=True),
    )
    status = export_status(plan, root)
    assert status["retryable"] == 1

    retried = export_retry(
        plan,
        root,
        {"request-1"},
        ExportOptions(delay_seconds=0),
        session_factory=lambda *_: MockSession(),
    )
    assert retried["validated"] == 1
    assert export_validate(plan, root)["valid"] is True

    attempts = _manifest(root)
    left = deterministic_manifest_fields(attempts[-1])
    volatile_copy = dict(attempts[-1])
    volatile_copy.update(export_attempt_id="different", started_at="later", completed_at="later")
    assert deterministic_manifest_fields(volatile_copy) == left


def test_selfcheck_and_cli_help_are_available_without_real_browser(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    session = MockSession()
    result = browser_selfcheck(
        plan,
        tmp_path / "downloads",
        session_factory=lambda *_: session,
    )
    assert result["visible_browser_requested"] is True
    assert result["download_performed"] is False
    assert result["selfcheck_status"] == "passed"
    assert session.download_count == 0

    runner = CliRunner()
    for command in (
        "browser-selfcheck",
        "export-browser",
        "export-status",
        "export-retry",
        "export-validate",
    ):
        cli = runner.invoke(app, ["trends", command, "--help"])
        assert cli.exit_code == 0, cli.output


@pytest.mark.trends_browser_integration
@pytest.mark.skipif(
    os.environ.get("GEODEMAND_TRENDS_BROWSER_INTEGRATION") != "1",
    reason="opt-in visible browser integration test",
)
def test_opt_in_browser_selfcheck() -> None:
    plan = Path(os.environ["GEODEMAND_TRENDS_PLAN"])
    download_dir = Path(os.environ["GEODEMAND_TRENDS_DOWNLOAD_DIR"])
    result = browser_selfcheck(plan, download_dir)
    assert result["selfcheck_status"] == "passed", result


@pytest.mark.trends_browser_download_integration
@pytest.mark.skipif(
    os.environ.get("GEODEMAND_TRENDS_BROWSER_DOWNLOAD_INTEGRATION") != "1",
    reason="opt-in supervised browser download integration test",
)
def test_opt_in_supervised_download() -> None:
    plan = Path(os.environ["GEODEMAND_TRENDS_PLAN"])
    output_root = Path(os.environ["GEODEMAND_TRENDS_OUTPUT_ROOT"])
    request_id = os.environ["GEODEMAND_TRENDS_REQUEST_ID"]
    result = export_browser(
        plan,
        output_root,
        ExportOptions(
            max_requests=1,
            request_ids={request_id},
            supervised=True,
        ),
        decision_provider=lambda *_: "download",
    )
    assert result["validated"] == 1, result


def _verified() -> PageVerification:
    return PageVerification(
        status="verified",
        loaded_url=_plan_row(1)["explore_url"],
        chart_detected=True,
        download_control_detected=True,
    )


def _plan(tmp_path: Path, count: int = 1) -> Path:
    path = tmp_path / "planning" / "plan.csv"
    _write_plan(path, [_plan_row(index) for index in range(1, count + 1)])
    return path


def _plan_row(index: int, repeat_id: str = "initial") -> dict[str, Any]:
    return {
        "request_id": f"request-{index}",
        "episode_id": f"episode-{index}",
        "geography": "US-FL",
        "geography_level": "state",
        "timeframe": "2024-01-01 2024-03-01",
        "request_start_date": "2024-01-01",
        "request_end_date": "2024-03-01",
        "target_concepts": "flood;heavy_rain",
        "concept_ids": "weather;flood;heavy_rain",
        "anchor_concept": "weather",
        "batch_id": "batch-1",
        "batch_index": 1,
        "category": 0,
        "search_property": "web_search",
        "expected_output_filename": f"request-{index}.csv",
        "sidecar_metadata_filename": f"request-{index}.json",
        "explore_url": (
            "https://trends.google.com/trends/explore?"
            "date=2024-01-01+2024-03-01&geo=US-FL&cat=0&"
            "q=weather%2Cflood%2Cheavy+rain"
        ),
        "terminology_version": "0.8A-v3",
        "request_status": "planned",
        "planned_repeat_id": repeat_id,
    }


def _write_plan(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_export(path: Path) -> None:
    path.write_text(
        "Category: All categories\n\n"
        "Day,weather: (US-FL),flood: (US-FL),heavy rain: (US-FL)\n"
        "2024-01-01,0,0,0\n"
        "2024-01-02,0,0,0\n",
        encoding="utf-8",
    )


def _manifest(root: Path) -> list[dict[str, Any]]:
    return pq.read_table(root / "manifests" / "browser_export_manifest.parquet").to_pylist()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
