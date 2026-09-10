"""Strict admission and versioned final analysis of existing official CSV observations."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import platform
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from geodemand import __version__
from geodemand.analysis_prediction import (
    COEFFICIENT_FIELDS,
    METRIC_FIELDS,
    PREDICTION_FIELDS,
    predict,
)
from geodemand.reproducibility import canonical_table_content_hash
from geodemand.trends import (
    TrendsSidecar,
    _load_sidecar,
    _parse_export,
    _parse_export_date,
    _parse_interest,
    read_request_plan_rows,
)
from geodemand.trends_event_study import _phase_metric_row, response_phase_windows

VERSION = "1.1-v1"
PAIR_SIZE = 2
MAX_INTEREST = 100
PHASES = ("anticipatory", "immediate", "early_recovery", "extended_recovery")
LINEAGE = (
    "request_id",
    "episode_id",
    "geography",
    "geography_level",
    "request_role",
    "batch_id",
    "terminology_version",
    "matched_pair_id",
    "execution_batch_id",
    "selection_version",
)
STRINGS = (
    *LINEAGE,
    "backend",
    "repeat_id",
    "export_attempt_id",
    "query_text",
    "query_type",
    "source_file",
    "source_sha256",
    "sidecar_sha256",
    "retrieval_date",
    "event_group",
    "candidate_split",
    "reason",
    "event_start_date",
    "event_end_date",
    "baseline_start_date",
    "baseline_end_date",
    "response_start_date",
    "response_end_date",
    "peak_date",
    "global_peak_date",
    "repeat_ids",
    "metric_version",
    "data_origin",
    "acquisition_backends",
    "source_lineage",
    "expected_output_filename",
    "sidecar_metadata_filename",
    "missing_dates",
    "partial_dates",
    "category",
    "search_property",
    *(f"{p}_{bound}_date" for p in PHASES for bound in ("start", "end")),
)
NUMBERS = (
    "standardized_peak_lift",
    "baseline_mean",
    "baseline_standard_deviation",
    "peak_lag_days",
    "global_peak_lag_days",
    "zero_fraction",
    "baseline_valid_day_count",
    "repeat_count",
    "repeat_sd",
    "duration_days",
    "union_area_km2",
    "season_sin",
    "season_cos",
    "partial_day_count",
    "execution_selection_rank",
    *(f"{p}_standardized_lift" for p in PHASES),
    *(f"{p}_valid_day_count" for p in PHASES),
)
BOOLS = (
    "acquired",
    "verified",
    "eligible",
    "primary_unit",
    "complete_pair",
    "complete_two_repeats",
)
SCHEMA = pa.schema(
    [
        *[pa.field(k, pa.string()) for k in STRINGS],
        *[pa.field(k, pa.float64()) for k in NUMBERS],
        *[pa.field(k, pa.bool_()) for k in BOOLS],
        pa.field("concept_id", pa.string()),
    ]
)
ELIGIBILITY_FIELDS = (
    *LINEAGE,
    "repeat_id",
    "concept_id",
    "acquired",
    "verified",
    "eligible",
    "complete_pair",
    "reason",
    "expected_output_filename",
    "sidecar_metadata_filename",
    "missing_dates",
    "partial_dates",
)
SENSITIVITY_FIELDS = (
    "check",
    "eligible_episodes",
    "eligible_units",
    "mean_lift",
    "primary_mean_lift",
    "paired_mean_change",
    "reason",
)


class FinalizeError(ValueError):
    """An input cannot safely enter the final analysis."""


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _date(value: Any) -> date:
    return date.fromisoformat(str(value)[:10])


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def _config(path: Path) -> dict[str, Any]:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    expected = {
        "version": VERSION,
        "maximum_episodes": 20,
        "minimum_prediction_episodes": 20,
        "baseline_start": -28,
        "baseline_end": -8,
        "folds": 5,
        "ridge_alpha": 1.0,
        "minimum_training_episodes": 12,
        "minimum_baseline_days": 14,
        "minimum_phase_coverage": 0.8,
        "maximum_zero_fraction": 0.5,
        "minimum_baseline_sd": 0.001,
        "primary_outcome": "standardized_peak_lift",
        "seed": 20260721,
        "context_panel": "batch_1_flood_awareness",
        "panels": ["batch_1_flood_awareness", "batch_5_specific_recovery"],
        "repeats": ["repeat_1", "repeat_2"],
        "primary_backends": ["manual_csv", "playwright_export_assistant"],
        "sensitivities": ["baseline_end_minus_1", "complete_two_repeats"],
    }
    if not isinstance(config, dict) or any(config.get(k) != v for k, v in expected.items()):
        raise FinalizeError(
            "Analysis settings differ from the frozen 1.1-v1 design; version the design explicitly."
        )
    if config.get("data_origin") not in {"observed", "synthetic_fixture"}:
        raise FinalizeError(
            "data_origin must explicitly distinguish observations and synthetic fixtures."
        )
    for key in ("terms", "rules", "lockfile"):
        config[key] = (path.parent / config[key]).resolve()
        if not config[key].is_file():
            raise FinalizeError(f"Missing {key}: {config[key]}")
    return dict(config)


def _plan(  # noqa: PLR0912
    path: Path, config: Mapping[str, Any], catalog: list[dict[str, Any]], terms: Mapping[str, Any]
) -> list[dict[str, Any]]:
    fields = {
        *LINEAGE,
        "concept_ids",
        "planned_repeat_id",
        "event_start_date",
        "event_end_date",
        "request_start_date",
        "request_end_date",
        "category",
        "search_property",
        "backend",
    }
    rows = read_request_plan_rows(path, required_fields=fields)
    ids = [str(r["provisional_episode_id"]) for r in catalog]
    if len(ids) != len(set(ids)):
        raise FinalizeError("Catalog episode join is not many-to-one: duplicate episode IDs.")
    by_id = dict(zip(ids, catalog, strict=True))
    episodes = {str(r["episode_id"]) for r in rows}
    if not episodes or len(episodes) > int(config["maximum_episodes"]):
        raise FinalizeError("Plan must contain 1–20 episodes in the existing full-core design.")
    panels = {b["batch_id"]: b["concepts"] for b in terms["standardized_batches"]}
    keys: set[tuple[str, str]] = set()
    combos: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    pair_ids: dict[str, set[str]] = defaultdict(set)
    logical: dict[str, tuple[str, ...]] = {}
    for row in rows:
        for field_name in fields:
            if row.get(field_name) is None or not str(row[field_name]).strip():
                raise FinalizeError(f"Empty plan lineage: {field_name}")
        episode = str(row["episode_id"])
        if episode not in by_id:
            raise FinalizeError(f"Catalog missing planned episode: {episode}")
        item = by_id[episode]
        if str(item.get("provisional_catalog_status")) != "retained_provisionally":
            raise FinalizeError(f"Episode not retained provisionally: {episode}")
        if (_date(item["start_date"]), _date(item["end_date"])) != (
            _date(row["event_start_date"]),
            _date(row["event_end_date"]),
        ):
            raise FinalizeError(f"Catalog/plan dates conflict: {episode}")
        if (
            row["batch_id"] not in config["panels"]
            or row["planned_repeat_id"] not in config["repeats"]
        ):
            raise FinalizeError("Plan is outside the two-panel/two-repeat full-core scope.")
        if str(row["concept_ids"]).split(";") != panels[row["batch_id"]]:
            raise FinalizeError("Plan concept panel differs from versioned terminology.")
        if (
            row["terminology_version"] != terms["dictionary_version"]
            or row["backend"] not in config["primary_backends"]
        ):
            raise FinalizeError("Plan terminology/backend differs from the primary design.")
        if row["request_role"] not in {"treated_state", "control_state"}:
            raise FinalizeError("Plan request role is outside full-core scope.")
        key = (str(row["request_id"]), str(row["planned_repeat_id"]))
        if key in keys:
            raise FinalizeError(f"Duplicate planned request/repeat: {key}")
        keys.add(key)
        signature = tuple(str(row[k]) for k in sorted(fields - {"planned_repeat_id"}))
        if key[0] in logical and logical[key[0]] != signature:
            raise FinalizeError("Logical request changes between planned repeats.")
        logical[key[0]] = signature
        combo = (str(row["request_role"]), str(row["batch_id"]), key[1])
        if combo in combos[episode]:
            raise FinalizeError("Multiple requests for the same episode/role/panel/repeat.")
        combos[episode].add(combo)
        pair_ids[episode].add(str(row["matched_pair_id"]))
    expected = {
        (role, panel, repeat)
        for role in ("treated_state", "control_state")
        for panel in config["panels"]
        for repeat in config["repeats"]
    }
    if any(v != expected for v in combos.values()) or any(len(v) != 1 for v in pair_ids.values()):
        raise FinalizeError(
            "Authoritative plan lacks a complete role/panel/repeat design or coherent pair ID."
        )
    if len({next(iter(v)) for v in pair_ids.values()}) != len(episodes):
        raise FinalizeError("Matched pair ID reused across episodes.")
    return sorted(rows, key=lambda r: (r["request_id"], r["planned_repeat_id"]))


def _raw_file(root: Path, name: str) -> Path:
    if not name or Path(name).name != name or "/" in name or "\\" in name:
        raise FinalizeError("Source filenames must be basenames within raw-root.")
    return root / name


def _verify_export(  # noqa: PLR0912, PLR0915
    rows: list[dict[str, Any]],
    plan: Mapping[str, Any],
    root: Path,
    terms: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[str, dict[str, str]]:
    files = {str(r.get("source_file", "")) for r in rows}
    if len(files) != 1:
        raise FinalizeError(
            "Multiple raw sources for one request/repeat; resolve attempt identity."
        )
    raw = _raw_file(root, next(iter(files)))
    sidecar_path = raw.with_suffix(".json")
    if not raw.is_file() or not sidecar_path.is_file():
        return "missing_raw_or_sidecar", {}
    payload = _load_sidecar(sidecar_path)
    if payload.get("csv_sha256") and payload["csv_sha256"] != sha(raw):
        raise FinalizeError(f"Raw hash mismatch: {raw.name}")
    if (
        not payload.get("csv_sha256")
        or not payload.get("export_date")
        or "REQUIRED" in str(payload.get("export_date"))
    ):
        return "unverified_hash_or_retrieval_date", {}
    sidecar = TrendsSidecar.model_validate(payload)
    if sidecar.source_filename != raw.name:
        raise FinalizeError("Sidecar filename mismatch.")
    expected = {
        k: str(plan[k])
        for k in LINEAGE
        if k not in {"matched_pair_id", "execution_batch_id", "selection_version"}
    }
    expected["repeat_id"] = str(plan["planned_repeat_id"])
    for field, expected_value in expected.items():
        if str(getattr(sidecar, field)) != expected_value:
            raise FinalizeError(f"Sidecar/plan lineage conflict: {raw.name}:{field}")
    for field in ("matched_pair_id", "execution_batch_id", "selection_version"):
        if payload.get(field) and str(payload[field]) != str(plan[field]):
            raise FinalizeError(f"Sidecar/plan lineage conflict: {field}")
    if sidecar.backend not in config["primary_backends"]:
        return "nonprimary_backend", {}
    if (sidecar.start_date, sidecar.end_date) != (
        _date(plan["request_start_date"]),
        _date(plan["request_end_date"]),
    ):
        raise FinalizeError("Sidecar/plan query date mismatch.")
    if (
        str(sidecar.category) != str(plan["category"])
        or sidecar.search_property != plan["search_property"]
    ):
        raise FinalizeError("Sidecar/plan query settings mismatch.")
    concepts = str(plan["concept_ids"]).split(";")
    if (
        concepts != sidecar.requested_concepts
        or sidecar.requested_labels != [terms[c]["query_text"] for c in concepts]
        or sidecar.query_types != [terms[c]["query_type"] for c in concepts]
    ):
        raise FinalizeError("Sidecar query panel does not match versioned terminology.")
    header, raw_rows = _parse_export(raw)
    if header[0].lower() not in {"date", "day"} or len(header) != len(concepts) + 1:
        raise FinalizeError(
            "Final analysis requires daily official CSV series; "
            "weekly/monthly data are inadmissible."
        )
    for index, label in enumerate(sidecar.requested_labels, 1):
        if label.casefold() not in header[index].casefold():
            raise FinalizeError("Raw CSV label does not match sidecar.")
    parsed: dict[tuple[str, date], tuple[float | None, bool]] = {}
    for raw_row in raw_rows:
        if len(raw_row[0]) != len("2024-01-01"):
            raise FinalizeError("Raw daily dates must use YYYY-MM-DD, not intervals or timestamps.")
        day = _parse_export_date(raw_row[0])
        if not sidecar.start_date <= day <= sidecar.end_date:
            raise FinalizeError("Raw observation outside query window.")
        for index, concept in enumerate(concepts):
            key = (concept, day)
            if key in parsed:
                raise FinalizeError("Duplicate raw date/concept.")
            value, partial, _ = _parse_interest(raw_row[index + 1])
            if value is not None and not math.isfinite(value):
                raise FinalizeError("Nonfinite raw interest.")
            parsed[key] = (value, partial)
    for row in rows:
        for field, expected_value in expected.items():
            if str(row.get(field)) != expected_value:
                raise FinalizeError(f"Observation/plan lineage conflict: {field}")
        concept = str(row["concept_id"])
        if (
            row.get("backend") != sidecar.backend
            or row.get("query_text") != terms[concept]["query_text"]
            or row.get("query_type") != terms[concept]["query_type"]
        ):
            raise FinalizeError("Observation query/backend disagrees with raw provenance.")
        if str(row.get("export_attempt_id") or "") != str(sidecar.export_attempt_id or ""):
            raise FinalizeError("Observation export attempt disagrees with sidecar.")
        if parsed.get((concept, _date(row["date"]))) != (
            row.get("interest"),
            row.get("is_partial"),
        ):
            raise FinalizeError("Observation value/partial flag differs from immutable CSV.")
    return "", {
        "source_file": raw.name,
        "source_sha256": sha(raw),
        "sidecar_sha256": sha(sidecar_path),
        "retrieval_date": sidecar.export_date.isoformat(),
        "backend": sidecar.backend,
        "export_attempt_id": sidecar.export_attempt_id or "",
    }


def response_metrics(
    series: Mapping[date, float],
    plan: Mapping[str, Any],
    term: Mapping[str, Any],
    config: Mapping[str, Any],
    rules: Mapping[str, Any],
    baseline_end: int = -8,
) -> dict[str, Any]:
    """Reuse phase calculation; add strict admission, effective windows and response timing."""
    start, end = _date(plan["event_start_date"]), _date(plan["event_end_date"])
    effective = {
        **plan,
        "baseline_start_date": (start - timedelta(days=28)).isoformat(),
        "baseline_end_date": (start + timedelta(days=baseline_end)).isoformat(),
        "post_start_date": (end + timedelta(days=1)).isoformat(),
        "post_end_date": (end + timedelta(days=28)).isoformat(),
    }
    metric = _phase_metric_row(
        str(plan["request_id"]),
        str(plan["planned_repeat_id"]),
        "",
        str(term["concept_id"]),
        series,
        effective,
        term,
        rules,
        baseline_end_override=_date(effective["baseline_end_date"]),
    )
    windows = response_phase_windows(start, end, rules)
    for phase, (left, right) in windows.items():
        metric[f"{phase}_start_date"] = str(left)
        metric[f"{phase}_end_date"] = str(right)
    reasons = []
    if metric["baseline_valid_day_count"] < config["minimum_baseline_days"]:
        reasons.append("insufficient_baseline")
    for phase, (left, right) in windows.items():
        if metric[f"{phase}_valid_day_count"] < math.ceil(
            len(_days(left, right)) * config["minimum_phase_coverage"]
        ):
            reasons.append(f"insufficient_{phase}_coverage")
    used = {
        d: v
        for d, v in series.items()
        if start - timedelta(days=28) <= d <= end + timedelta(days=28)
    }
    fraction = sum(v == 0 for v in used.values()) / len(used) if used else 1.0
    if not used:
        reasons.append("no_valid_observations")
    elif fraction == 1:
        reasons.append("all_zero")
    elif fraction >= config["maximum_zero_fraction"]:
        reasons.append("low_volume")
    sd = metric["baseline_standard_deviation"]
    if sd is None or sd <= config["minimum_baseline_sd"]:
        reasons.append("undefined_normalization")
    response = {
        d: v for d, v in used.items() if start - timedelta(days=7) <= d <= end + timedelta(days=28)
    }
    peak = min(response, key=lambda d: (-response[d], d)) if response else None
    metric.update(
        {
            "zero_fraction": fraction,
            "reason": ";".join(reasons),
            "eligible": not reasons,
            "peak_date": peak.isoformat() if peak else None,
            "peak_lag_days": (peak - start).days if peak else None,
            "global_peak_lag_days": metric["peak_lead_lag_days"],
            "response_start_date": (start - timedelta(days=7)).isoformat(),
            "response_end_date": (end + timedelta(days=28)).isoformat(),
            "metric_version": VERSION,
        }
    )
    if sd is None or sd <= config["minimum_baseline_sd"]:
        metric["standardized_peak_lift"] = None
    return metric


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[
            tuple(
                str(row.get(k, ""))
                for k in (
                    "episode_id",
                    "geography",
                    "concept_id",
                    "request_role",
                    "backend",
                    "batch_id",
                    "request_id",
                )
            )
        ].append(row)
    result = []
    for _, group in sorted(groups.items()):
        valid = [r for r in group if r["eligible"]]
        row = dict(group[0])
        row.update(
            eligible=bool(valid),
            repeat_id="",
            repeat_ids=";".join(sorted(r["repeat_id"] for r in valid)),
            repeat_count=len(valid),
            complete_two_repeats=len(valid) == PAIR_SIZE,
            complete_pair=all(r.get("complete_pair", False) for r in valid) if valid else False,
            reason="" if valid else ";".join(sorted({r["reason"] for r in group})),
            acquired=any(r["acquired"] for r in group),
            verified=any(r["verified"] for r in group),
            acquisition_backends=";".join(sorted({str(r["backend"]) for r in valid})),
            source_lineage=json.dumps(
                [
                    {
                        k: r.get(k)
                        for k in (
                            "repeat_id",
                            "source_file",
                            "source_sha256",
                            "sidecar_sha256",
                            "export_attempt_id",
                            "retrieval_date",
                        )
                    }
                    for r in group
                ],
                sort_keys=True,
            ),
        )
        for field in (
            "source_file",
            "source_sha256",
            "sidecar_sha256",
            "export_attempt_id",
            "retrieval_date",
        ):
            row[field] = ""  # Aggregate provenance is the complete source_lineage list.
        row["peak_date"] = row["global_peak_date"] = ""
        for field in (
            "standardized_peak_lift",
            "peak_lag_days",
            "global_peak_lag_days",
            "baseline_mean",
            "baseline_standard_deviation",
            "zero_fraction",
            "baseline_valid_day_count",
            "partial_day_count",
            *(f"{p}_valid_day_count" for p in PHASES),
            *(f"{p}_standardized_lift" for p in PHASES),
        ):
            values = [float(r[field]) for r in valid if r.get(field) is not None]
            row[field] = statistics.fmean(values) if values else None
        row["repeat_sd"] = (
            statistics.pstdev([r["standardized_peak_lift"] for r in valid])
            if len(valid) > 1
            else None
        )
        result.append(row)
    return result


def eligible_treated_control_pairs(
    rows: Sequence[Mapping[str, Any]],
) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    """Return contrasts satisfying the same admission contract as pairs."""
    grouped: dict[tuple[str, str, str], dict[str, list[Mapping[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        if not row.get("eligible") or not row.get("primary_unit"):
            continue
        role = str(row.get("request_role", ""))
        if role in {"treated_state", "control_state"}:
            key = (str(row["matched_pair_id"]), str(row["batch_id"]), str(row["concept_id"]))
            grouped[key][role].append(row)
    output: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for key in sorted(grouped):
        treated = grouped[key].get("treated_state", [])
        control = grouped[key].get("control_state", [])
        if len(treated) != 1 or len(control) != 1:
            continue
        left, right = treated[0], control[0]
        if not left.get("complete_pair") or not right.get("complete_pair"):
            continue
        if str(left.get("request_id")) == str(right.get("request_id")):
            continue
        if str(left.get("geography")) == str(right.get("geography")):
            continue
        output.append((left, right))
    return output


def _summary_sensitivity(
    name: str, primary: list[dict[str, Any]], other: list[dict[str, Any]]
) -> dict[str, Any]:
    def units(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
        return {
            (r["episode_id"], r["concept_id"]): r
            for r in rows
            if r["eligible"] and r["primary_unit"] and r["request_role"] == "treated_state"
        }

    first, second = units(primary), units(other)
    shared = sorted(first.keys() & second.keys())

    def mean_by_episode(values: list[tuple[str, float]]) -> float | None:
        grouped: dict[str, list[float]] = defaultdict(list)
        for episode, value in values:
            grouped[episode].append(value)
        return statistics.fmean(statistics.fmean(v) for v in grouped.values()) if grouped else None

    return {
        "check": name,
        "eligible_episodes": len({k[0] for k in second}),
        "eligible_units": len(second),
        "mean_lift": mean_by_episode(
            [(k[0], v["standardized_peak_lift"]) for k, v in second.items()]
        ),
        "primary_mean_lift": mean_by_episode(
            [(k[0], v["standardized_peak_lift"]) for k, v in first.items()]
        ),
        "paired_mean_change": mean_by_episode(
            [
                (k[0], second[k]["standardized_peak_lift"] - first[k]["standardized_peak_lift"])
                for k in shared
            ]
        ),
        "reason": "" if shared else "no_eligible_paired_units",
    }


def finalize(
    *,
    catalog_path: Path,
    request_plan_path: Path,
    observations_path: Path,
    raw_root: Path,
    config_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    try:
        return _finalize(
            catalog_path, request_plan_path, observations_path, raw_root, config_path, output_root
        )
    except (OSError, ValueError, KeyError, TypeError, pa.ArrowException, yaml.YAMLError) as exc:
        if isinstance(exc, FinalizeError):
            raise
        raise FinalizeError(f"Final analysis input/output error: {exc}") from exc


def _finalize(  # noqa: PLR0912, PLR0915
    catalog_path: Path,
    request_plan_path: Path,
    observations_path: Path,
    raw_root: Path,
    config_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    from geodemand.analysis_report import build_report  # noqa: PLC0415

    config = _config(config_path)
    terms_payload = yaml.safe_load(config["terms"].read_text(encoding="utf-8"))
    terms = {r["concept_id"]: r for r in terms_payload["concepts"]}
    rules = yaml.safe_load(config["rules"].read_text(encoding="utf-8"))
    expected_windows = {
        "anticipatory_days": 7,
        "immediate_post_end_days": 2,
        "early_recovery_start_days": 3,
        "early_recovery_end_days": 7,
        "extended_recovery_start_days": 8,
        "extended_recovery_end_days": 28,
    }
    if any(rules["event_study"].get(k) != v for k, v in expected_windows.items()):
        raise FinalizeError("Phase rules differ from frozen 1.1 windows.")
    if rules["normalization"]["near_zero_standard_deviation"] != config["minimum_baseline_sd"]:
        raise FinalizeError("Normalization threshold conflicts with 1.1 design.")
    catalog = pq.read_table(catalog_path).to_pylist()
    plans = _plan(request_plan_path, config, catalog, terms_payload)
    cats = {str(r["provisional_episode_id"]): r for r in catalog}
    observations = pq.read_table(observations_path).to_pylist()
    plan_index = {(r["request_id"], r["planned_repeat_id"]): r for r in plans}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    seen = set()
    for row in observations:
        if config["data_origin"] == "observed" and row.get("data_origin") == "synthetic_fixture":
            raise FinalizeError("Synthetic observations cannot enter an observed-data report.")
        key = (str(row["request_id"]), str(row["repeat_id"]))
        if key not in plan_index or row["concept_id"] not in str(
            plan_index[key]["concept_ids"]
        ).split(";"):
            raise FinalizeError(f"Observation outside supplied plan: {key}:{row['concept_id']}")
        identity = (*key, str(row["concept_id"]), _date(row["date"]))
        if identity in seen:
            raise FinalizeError(f"Duplicate observation: {identity}")
        seen.add(identity)
        if not isinstance(row.get("is_partial"), bool):
            raise FinalizeError("Observation partial flag must be boolean.")
        value = row.get("interest")
        if value is not None and (
            not math.isfinite(float(value)) or not 0 <= float(value) <= MAX_INTEREST
        ):
            raise FinalizeError("Invalid/nonfinite interest outside [0,100].")
        grouped[key].append(row)
    repeat_rows: list[dict[str, Any]] = []
    alternative: list[dict[str, Any]] = []
    series_by_key: dict[tuple[str, str, str], dict[date, float]] = {}
    for plan in plans:
        key = (str(plan["request_id"]), str(plan["planned_repeat_id"]))
        source = grouped.get(key, [])
        reason, provenance = (
            _verify_export(source, plan, raw_root, terms, config)
            if source
            else ("missing_request_or_repeat", {})
        )
        cat = cats[str(plan["episode_id"])]
        start = _date(plan["event_start_date"])
        for concept in str(plan["concept_ids"]).split(";"):
            points = [r for r in source if r["concept_id"] == concept]
            series = (
                {
                    _date(r["date"]): float(r["interest"])
                    for r in points
                    if r["interest"] is not None and not r["is_partial"]
                }
                if not reason
                else {}
            )
            row = {
                **{k: str(plan[k]) for k in LINEAGE},
                **provenance,
                "backend": provenance.get("backend", str(plan["backend"])),
                "repeat_id": key[1],
                "concept_id": concept,
                "query_text": terms[concept]["query_text"],
                "query_type": terms[concept]["query_type"],
                "duration_days": cat.get("duration_days"),
                "union_area_km2": cat.get("union_area_km2"),
                "event_group": str(
                    cat.get("shared_event_group") or cat.get("event_group_id") or plan["episode_id"]
                ),
                "candidate_split": str(cat.get("candidate_split", "unknown")),
                "season_sin": math.sin(2 * math.pi * start.month / 12),
                "season_cos": math.cos(2 * math.pi * start.month / 12),
                "acquired": bool(points),
                "verified": bool(points) and not reason,
                "primary_unit": concept != "context_weather"
                or plan["batch_id"] == config["context_panel"],
                "data_origin": config["data_origin"],
                "partial_day_count": sum(r["is_partial"] for r in points),
                "expected_output_filename": str(plan.get("expected_output_filename") or ""),
                "sidecar_metadata_filename": str(plan.get("sidecar_metadata_filename") or ""),
                "execution_selection_rank": (
                    float(plan["execution_selection_rank"])
                    if plan.get("execution_selection_rank") is not None
                    else None
                ),
                "category": str(plan["category"]),
                "search_property": str(plan["search_property"]),
                "missing_dates": ";".join(
                    str(d)
                    for d in _days(
                        start - timedelta(days=28),
                        _date(plan["event_end_date"]) + timedelta(days=28),
                    )
                    if d not in series
                ),
                "partial_dates": ";".join(
                    sorted(str(_date(r["date"])) for r in points if r["is_partial"])
                ),
            }
            for field in ("duration_days", "union_area_km2"):
                if row[field] is not None and (
                    not math.isfinite(float(row[field])) or float(row[field]) < 0
                ):
                    raise FinalizeError(f"Invalid reported-event descriptor: {field}")
            metric = response_metrics(series, plan, terms[concept], config, rules)
            row.update({k: v for k, v in metric.items() if k in SCHEMA.names})
            row.update(provenance)
            if reason or not points:
                row.update(eligible=False, reason=reason or "missing_concept")
            repeat_rows.append(row)
            changed = {
                **row,
                **{
                    k: v
                    for k, v in response_metrics(
                        series, plan, terms[concept], config, rules, -1
                    ).items()
                    if k in SCHEMA.names
                },
            }
            if reason or not points:
                changed.update(eligible=False, reason=reason or "missing_concept")
            changed.update(provenance)
            alternative.append(changed)
            series_by_key[(*key, concept)] = series
    for rows in (repeat_rows, alternative):
        pair_groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            pair_groups[
                tuple(
                    str(row[k]) for k in ("matched_pair_id", "batch_id", "concept_id", "repeat_id")
                )
            ].append(row)
        for pair in pair_groups.values():
            complete = (
                len(pair) == PAIR_SIZE
                and all(r["eligible"] for r in pair)
                and len({r["geography"] for r in pair}) == PAIR_SIZE
            )
            for row in pair:
                row["complete_pair"] = complete
    event_rows = aggregate(repeat_rows)
    admitted_keys = [
        (r["episode_id"], r["geography"], r["concept_id"], r["request_role"])
        for r in event_rows
        if r["eligible"] and r["primary_unit"]
    ]
    if len(admitted_keys) != len(set(admitted_keys)):
        raise FinalizeError(
            "Multiple primary backend/panel units; select one source per episode/concept."
        )
    predictions, metrics, coefficients, reasons = predict(event_rows, config)
    sensitivities = [
        _summary_sensitivity("baseline_end_minus_1", event_rows, aggregate(alternative)),
        _summary_sensitivity(
            "complete_two_repeats", event_rows, [r for r in event_rows if r["complete_two_repeats"]]
        ),
    ]
    planned_episodes = {r["episode_id"] for r in plans}
    primary = [
        r
        for r in event_rows
        if r["eligible"] and r["primary_unit"] and r["request_role"] == "treated_state"
    ]
    contrasts = eligible_treated_control_pairs(event_rows)
    verified = [r for r in repeat_rows if r["verified"]]
    readiness = {
        "analysis_version": VERSION,
        "data_origin": config["data_origin"],
        "status": "synthetic_smoke_only"
        if config["data_origin"] == "synthetic_fixture"
        else "descriptive_complete"
        if verified
        else "empirical_blocked",
        "prediction_status": "not_estimable" if reasons else "estimated",
        "prediction_reasons": reasons,
        "planned_episode_count": len(planned_episodes),
        "observed_episode_count": len({r["episode_id"] for r in repeat_rows if r["acquired"]}),
        "verified_episode_count": len({r["episode_id"] for r in verified}),
        "eligible_episode_count": len({r["episode_id"] for r in primary}),
        "eligible_independent_event_groups": len({r["event_group"] for r in primary}),
        "planned_request_repeat_concept_count": len(repeat_rows),
        "eligible_primary_unit_count": len(primary),
        "eligible_treated_control_contrast_count": len(contrasts),
        "eligible_treated_control_contrast_episode_count": len(
            {str(t["episode_id"]) for t, _ in contrasts}
        ),
        "missing_count": sum(not r["acquired"] for r in repeat_rows),
        "excluded_count": sum(not r["eligible"] for r in repeat_rows),
        "complete_plan": all(r["verified"] for r in repeat_rows),
        "complete_usable_plan": all(r["eligible"] for r in repeat_rows),
        "missing_pair_count": len(
            {
                (r["matched_pair_id"], r["batch_id"], r["concept_id"], r["repeat_id"])
                for r in repeat_rows
                if not r["complete_pair"]
            }
        ),
        "unverified_count": sum(r["acquired"] and not r["verified"] for r in repeat_rows),
        "real_data_claim": config["data_origin"] == "observed" and bool(verified),
        "eligible_primary_concepts": sorted({r["concept_id"] for r in primary}),
        "unavailable_descriptors": [
            field
            for field in ("duration_days", "union_area_km2")
            if not any(r.get(field) is not None for r in primary)
        ],
    }
    for path in (
        catalog_path,
        request_plan_path,
        observations_path,
        config_path,
        config["terms"],
        config["rules"],
        config["lockfile"],
    ):
        if path.resolve().is_relative_to(output_root.resolve()):
            raise FinalizeError("Output root must not contain any input file.")
    if raw_root.resolve().is_relative_to(
        output_root.resolve()
    ) or output_root.resolve().is_relative_to(raw_root.resolve()):
        raise FinalizeError("Output root and raw root must be separate.")
    output_root.mkdir(parents=True, exist_ok=True)
    if any(output_root.iterdir()):
        raise FinalizeError(
            "Use an empty output root to avoid stale or overwritten scientific results."
        )
    for name, rows in (
        ("event_demand_repeat_level", repeat_rows),
        ("event_demand_event_level", event_rows),
    ):
        pq.write_table(
            pa.Table.from_pylist(rows, schema=SCHEMA),
            output_root / f"{name}.parquet",
            compression="zstd",
        )
    write_csv(output_root / "eligibility.csv", repeat_rows, ELIGIBILITY_FIELDS)
    write_json(output_root / "readiness.json", readiness)
    write_csv(output_root / "model_predictions.csv", predictions, PREDICTION_FIELDS)
    write_csv(output_root / "model_metrics.csv", metrics, METRIC_FIELDS)
    write_csv(output_root / "model_coefficients.csv", coefficients, COEFFICIENT_FIELDS)
    write_csv(output_root / "sensitivity.csv", sensitivities, SENSITIVITY_FIELDS)
    inputs = {
        name: {"path": str(path.resolve()), "sha256": sha(path)}
        for name, path in (
            ("catalog", catalog_path),
            ("request_plan", request_plan_path),
            ("observations", observations_path),
            ("config", config_path),
            ("terms", config["terms"]),
            ("rules", config["rules"]),
            ("lockfile", config["lockfile"]),
        )
    }
    command = (
        f'geodemand analysis finalize --catalog "{catalog_path.resolve()}" '
        f'--request-plan "{request_plan_path.resolve()}" '
        f'--observations "{observations_path.resolve()}" --raw-root "{raw_root.resolve()}" '
        f'--config "{config_path.resolve()}" --output-root NEW_EMPTY_OUTPUT_ROOT'
    )
    build_report(
        output_root,
        readiness,
        repeat_rows,
        event_rows,
        series_by_key,
        predictions,
        metrics,
        sensitivities,
        command,
    )
    source_root = Path(__file__).parent
    source_hashes = {
        str(p.relative_to(source_root)): sha(p) for p in sorted(source_root.rglob("*.py"))
    }
    outputs = {
        str(p.relative_to(output_root)).replace("\\", "/"): {
            "sha256": sha(p),
            **(
                {"content_sha256": canonical_table_content_hash(p)}
                if p.suffix == ".parquet"
                else {}
            ),
        }
        for p in sorted(output_root.rglob("*"))
        if p.is_file()
    }
    run_provenance = {
        "raw_root": str(raw_root.resolve()),
        "version": VERSION,
        "package_version": __version__,
        "python_version": platform.python_version(),
        "data_origin": config["data_origin"],
        "seed": config["seed"],
        "inputs": inputs,
        "source_snapshot_sha256": hashlib.sha256(
            json.dumps(source_hashes, sort_keys=True).encode()
        ).hexdigest(),
        "source_files": source_hashes,
        "outputs": outputs,
        "raw_sources": sorted(
            [
                {
                    k: r.get(k)
                    for k in (
                        "source_file",
                        "source_sha256",
                        "sidecar_sha256",
                        "retrieval_date",
                        "backend",
                        "request_id",
                        "repeat_id",
                    )
                }
                for r in verified
            ],
            key=lambda r: (str(r["request_id"]), str(r["repeat_id"])),
        ),
    }
    write_json(output_root / "provenance.json", run_provenance)
    return {**readiness, "outputs": {p.name: str(p) for p in sorted(output_root.iterdir())}}
