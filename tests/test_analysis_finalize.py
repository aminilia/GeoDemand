from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from typer.testing import CliRunner

from geodemand.analysis_finalize import (
    FinalizeError,
    _config,
    aggregate,
    finalize,
    response_metrics,
)
from geodemand.analysis_prediction import _design, grouped_folds, predict
from geodemand.analysis_report import curve_data
from geodemand.cli import app
from geodemand.trends import TrendsError, import_csv_export

ROOT = Path(__file__).parents[1]


def final_inputs(tmp_path: Path, episodes: int = 1) -> dict[str, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = yaml.safe_load((ROOT / "config/analysis_1_1.yaml").read_text())
    config.update(
        data_origin="synthetic_fixture",
        terms=str(ROOT / "config/trends_terms.yaml"),
        rules=str(ROOT / "config/trends_rules.yaml"),
        lockfile=str(ROOT / "uv.lock"),
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    terms = yaml.safe_load((ROOT / "config/trends_terms.yaml").read_text())
    term_by_id = {r["concept_id"]: r for r in terms["concepts"]}
    plans, cats, observations = [], [], []
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    for index in range(episodes):
        episode = f"episode-{index:02}"
        start = date(2024, 1, 29) + timedelta(days=index * 10)
        cats.append(
            {
                "provisional_episode_id": episode,
                "start_date": str(start),
                "end_date": str(start),
                "provisional_catalog_status": "retained_provisionally",
                "duration_days": 0,
                "union_area_km2": 20 + index,
                "candidate_split": "validation",
            }
        )
        for role in ("treated_state", "control_state"):
            for batch in terms["standardized_batches"]:
                if batch["batch_id"] not in config["panels"]:
                    continue
                concepts = batch["concepts"]
                request = f"{episode}-{role}-{batch['batch_id']}"
                for repeat in config["repeats"]:
                    raw = raw_root / f"{request}-{repeat}.csv"
                    labels = [term_by_id[c]["query_text"] for c in concepts]
                    geo = "US-FL" if role == "treated_state" else "US-GA"
                    plan = {
                        "request_id": request,
                        "episode_id": episode,
                        "geography": geo,
                        "geography_level": "state",
                        "request_role": role,
                        "batch_id": batch["batch_id"],
                        "terminology_version": terms["dictionary_version"],
                        "matched_pair_id": f"pair-{index}",
                        "execution_batch_id": "smoke-full-core",
                        "selection_version": "synthetic-v1",
                        "concept_ids": ";".join(concepts),
                        "planned_repeat_id": repeat,
                        "event_start_date": str(start),
                        "event_end_date": str(start),
                        "request_start_date": str(start - timedelta(days=28)),
                        "request_end_date": str(start + timedelta(days=28)),
                        "category": 0,
                        "search_property": "web_search",
                        "backend": "manual_csv",
                    }
                    plans.append(plan)
                    values = []
                    for lag in range(-28, 29):
                        value = 10 + (lag % 3) + (5 + index if lag == 1 else 0)
                        day = start + timedelta(days=lag)
                        values.append([str(day), *[value] * len(concepts)])
                        for concept in concepts:
                            observations.append(
                                {
                                    "request_id": request,
                                    "repeat_id": repeat,
                                    "episode_id": episode,
                                    "geography": geo,
                                    "geography_level": "state",
                                    "request_role": role,
                                    "batch_id": batch["batch_id"],
                                    "terminology_version": terms["dictionary_version"],
                                    "concept_id": concept,
                                    "date": day,
                                    "interest": float(value),
                                    "is_partial": False,
                                    "backend": "manual_csv",
                                    "source_file": raw.name,
                                    "query_text": term_by_id[concept]["query_text"],
                                    "query_type": "term",
                                    "export_attempt_id": f"{request}-{repeat}",
                                }
                            )
                    with raw.open("w", newline="", encoding="utf-8") as stream:
                        stream.write("Category: All categories\n\n")
                        writer = csv.writer(stream, lineterminator="\n")
                        writer.writerow(["Day", *labels])
                        writer.writerows(values)
                    sidecar = {
                        k: plan[k]
                        for k in (
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
                            "category",
                            "search_property",
                            "backend",
                        )
                    }
                    sidecar.update(
                        requested_concepts=concepts,
                        requested_labels=labels,
                        query_types=["term"] * len(concepts),
                        semantic_families=[term_by_id[c]["semantic_family"] for c in concepts],
                        start_date=plan["request_start_date"],
                        end_date=plan["request_end_date"],
                        repeat_id=repeat,
                        export_attempt_id=f"{request}-{repeat}",
                        export_date="2026-01-01",
                        source_filename=raw.name,
                        csv_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),
                    )
                    raw.with_suffix(".json").write_text(json.dumps(sidecar), encoding="utf-8")
    paths = {
        "catalog_path": tmp_path / "catalog.parquet",
        "request_plan_path": tmp_path / "plan.parquet",
        "observations_path": tmp_path / "observations.parquet",
        "raw_root": raw_root,
        "config_path": config_path,
    }
    for key, rows in (
        ("catalog_path", cats),
        ("request_plan_path", plans),
        ("observations_path", observations),
    ):
        pq.write_table(pa.Table.from_pylist(rows), paths[key])
    return paths


def test_finalize_complete_smoke_and_determinism(tmp_path: Path) -> None:
    inputs = final_inputs(tmp_path / "inputs")
    first, second = tmp_path / "first", tmp_path / "second"
    result = finalize(**inputs, output_root=first)
    assert result["status"] == "synthetic_smoke_only"
    assert result["prediction_status"] == "not_estimable"
    assert result["complete_plan"] is True
    assert result["eligible_primary_unit_count"] == 9
    finalize(**inputs, output_root=second)
    assert {p.relative_to(first): p.read_bytes() for p in first.rglob("*") if p.is_file()} == {
        p.relative_to(second): p.read_bytes() for p in second.rglob("*") if p.is_file()
    }
    rows = pq.read_table(first / "event_demand_repeat_level.parquet").to_pylist()
    assert len(rows) == 40
    assert all(r["complete_pair"] for r in rows)
    assert all(r["export_attempt_id"] for r in rows)
    assert len(list((first / "figures").glob("*.png"))) == 4
    assert len(list((first / "figures").glob("*.svg"))) == 4


def test_missing_repeat_concept_pair_and_no_data(tmp_path: Path) -> None:
    inputs = final_inputs(tmp_path / "inputs")
    table = pq.read_table(inputs["observations_path"])
    rows = [
        r
        for r in table.to_pylist()
        if r["request_role"] == "treated_state"
        and r["repeat_id"] == "repeat_1"
        and r["concept_id"] != "flood"
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), inputs["observations_path"])
    result = finalize(**inputs, output_root=tmp_path / "out")
    assert result["missing_count"] > 0
    assert result["missing_pair_count"] == 20
    reasons = (tmp_path / "out/eligibility.csv").read_text()
    assert "missing_concept" in reasons and "missing_request_or_repeat" in reasons
    pq.write_table(table.slice(0, 0), inputs["observations_path"])
    result = finalize(**inputs, output_root=tmp_path / "empty")
    assert result["verified_episode_count"] == 0
    assert result["eligible_episode_count"] == 0


@pytest.mark.parametrize(
    "change", ["hash", "value", "duplicate", "backend", "query", "weekly", "catalog"]
)
def test_finalize_rejects_corrupt_inputs(tmp_path: Path, change: str) -> None:
    inputs = final_inputs(tmp_path / "inputs")
    rows = pq.read_table(inputs["observations_path"]).to_pylist()
    raw = inputs["raw_root"] / rows[0]["source_file"]
    if change == "hash":
        raw.write_text(raw.read_text() + "\n")
    elif change == "weekly":
        raw.write_text(raw.read_text().replace("Day,", "Week,"))
        sidecar = json.loads(raw.with_suffix(".json").read_text())
        sidecar["csv_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
        raw.with_suffix(".json").write_text(json.dumps(sidecar))
    elif change == "catalog":
        cats = pq.read_table(inputs["catalog_path"]).to_pylist()
        pq.write_table(pa.Table.from_pylist(cats * 2), inputs["catalog_path"])
    else:
        if change == "duplicate":
            rows.append(rows[0])
        if change == "value":
            rows[0]["interest"] = 99.0
        if change == "backend":
            rows[0]["backend"] = "pytrends"
        if change == "query":
            rows[0]["query_text"] = "different"
        pq.write_table(pa.Table.from_pylist(rows), inputs["observations_path"])
    with pytest.raises(FinalizeError):
        finalize(**inputs, output_root=tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_effective_baseline_and_response_peak_are_consistent(tmp_path: Path) -> None:
    inputs = final_inputs(tmp_path / "inputs")
    plan = pq.read_table(inputs["request_plan_path"]).to_pylist()[0]
    config = _config(inputs["config_path"])
    rules = yaml.safe_load(config["rules"].read_text())
    start = date.fromisoformat(plan["event_start_date"])
    # Seven cycles of [10,11,12] baseline => mean 11, population SD sqrt(2/3).
    series = {start + timedelta(days=lag): float(10 + ((lag + 28) % 3)) for lag in range(-28, 29)}
    series[start + timedelta(days=1)] = 20
    series[start + timedelta(days=2)] = 20
    metric = response_metrics(series, plan, {"concept_id": "flood"}, config, rules)
    assert metric["baseline_mean"] == 11
    assert metric["standardized_peak_lift"] == pytest.approx(9 / (2 / 3) ** 0.5)
    assert metric["peak_lag_days"] == 1
    assert metric["baseline_end_date"] == str(start - timedelta(days=8))
    series[start - timedelta(days=20)] = 90
    metric = response_metrics(series, plan, {"concept_id": "flood"}, config, rules)
    assert metric["global_peak_lag_days"] == -20 and metric["peak_lag_days"] == 1
    zero = response_metrics(
        dict.fromkeys(series, 0.0), plan, {"concept_id": "flood"}, config, rules
    )
    assert zero["standardized_peak_lift"] is None and "all_zero" in zero["reason"]
    sparse = response_metrics({start: 10}, plan, {"concept_id": "flood"}, config, rules)
    assert not sparse["eligible"] and "insufficient_baseline" in sparse["reason"]


def test_import_exact_reimport_and_conflict(tmp_path: Path) -> None:
    inputs = final_inputs(tmp_path / "inputs")
    raw = next(inputs["raw_root"].glob("*.csv"))
    sidecar = raw.with_suffix(".json")
    first = import_csv_export(raw, sidecar, tmp_path / "imports")
    assert first["imported_rows"] > 0
    assert import_csv_export(raw, sidecar, tmp_path / "imports")["imported_rows"] == 0
    replacement = raw.parent / "replacement.csv"
    replacement.write_text(raw.read_text().replace(",10", ",19"))
    meta = json.loads(sidecar.read_text())
    meta.update(
        source_filename=replacement.name,
        csv_sha256=hashlib.sha256(replacement.read_bytes()).hexdigest(),
    )
    replacement.with_suffix(".json").write_text(json.dumps(meta))
    with pytest.raises(TrendsError, match="Conflicting"):
        import_csv_export(replacement, replacement.with_suffix(".json"), tmp_path / "imports")


def test_official_preamble_less_than_one_and_partial_exclusion(tmp_path: Path) -> None:
    inputs = final_inputs(tmp_path / "inputs")
    rows = pq.read_table(inputs["observations_path"]).to_pylist()
    first = rows[0]
    raw = inputs["raw_root"] / first["source_file"]
    lines = raw.read_text().splitlines()
    cells = lines[3].split(",")
    cells[1] = "10 (partial)"
    cells[2] = "<1"
    lines[3] = ",".join(cells)
    raw.write_text("\n".join(lines) + "\n")
    meta_path = raw.with_suffix(".json")
    meta = json.loads(meta_path.read_text())
    meta["csv_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
    meta_path.write_text(json.dumps(meta))
    rows[0].update(interest=10.0, is_partial=True)
    rows[1]["interest"] = 0.0
    pq.write_table(pa.Table.from_pylist(rows), inputs["observations_path"])
    finalize(**inputs, output_root=tmp_path / "out")
    result = pq.read_table(tmp_path / "out/event_demand_repeat_level.parquet").to_pylist()
    selected = next(
        r
        for r in result
        if r["request_id"] == first["request_id"]
        and r["repeat_id"] == first["repeat_id"]
        and r["concept_id"] == first["concept_id"]
    )
    assert selected["verified"] and selected["baseline_valid_day_count"] == 20
    assert selected["partial_day_count"] == 1 and selected["partial_dates"]


def test_grouped_model_training_only_and_admission() -> None:
    rows: list[dict[str, Any]] = [
        {
            "episode_id": f"e{i}",
            "event_group": f"g{i}",
            "concept_id": "flood",
            "eligible": True,
            "primary_unit": True,
            "request_role": "treated_state",
            "standardized_peak_lift": float(i),
            "union_area_km2": i + 1,
            "duration_days": i % 3,
            "season_sin": 0,
            "season_cos": 1,
        }
        for i in range(20)
    ]
    config = {
        "minimum_prediction_episodes": 20,
        "minimum_training_episodes": 12,
        "folds": 5,
        "seed": 42,
        "ridge_alpha": 1.0,
    }
    predictions, metrics, coefficients, reasons = predict(rows, config)
    assert not reasons and len(predictions) == 20
    assert len(metrics) == 12 and coefficients
    folds = grouped_folds(rows, 5, 42)
    for r in predictions:
        train = [x["standardized_peak_lift"] for x in rows if folds[x["event_group"]] != r["fold"]]
        assert r["naive_prediction"] == pytest.approx(sum(train) / len(train))
    assert predict(rows[:19], config)[3]
    train, test = rows[:15], [{**rows[15], "union_area_km2": 1e12}]
    x, _, specs = _design(train, test, "0")
    x2, _, specs2 = _design(train, [{**test[0], "union_area_km2": None}], "0")
    np.testing.assert_array_equal(x, x2)
    assert specs == specs2
    rows[1]["event_group"] = rows[0]["event_group"]
    assert predict(rows, config)[3]  # shared-event groups reduce independent count


def test_repeat_weighting_and_panel_separation() -> None:
    base = {
        "episode_id": "e",
        "geography": "US-FL",
        "concept_id": "weather",
        "request_role": "treated_state",
        "backend": "manual_csv",
        "batch_id": "a",
        "request_id": "r",
        "repeat_id": "repeat_1",
        "eligible": True,
        "acquired": True,
        "verified": True,
        "reason": "",
        "primary_unit": True,
        "standardized_peak_lift": 2.0,
        "peak_lag_days": 1,
        "event_start_date": "2024-01-01",
        "baseline_mean": 0,
        "baseline_standard_deviation": 1,
    }
    rows = [
        base,
        {**base, "repeat_id": "repeat_2", "standardized_peak_lift": 4.0},
        {
            **base,
            "batch_id": "b",
            "request_id": "s",
            "primary_unit": False,
            "standardized_peak_lift": 99.0,
        },
    ]
    result = aggregate(rows)
    assert len(result) == 2 and result[0]["standardized_peak_lift"] == 3
    series = {
        ("r", "repeat_1", "weather"): {date(2024, 1, 1): 2},
        ("r", "repeat_2", "weather"): {date(2024, 1, 1): 4},
    }
    assert curve_data(rows, series)["weather"][0] == [3]


def test_cli_reports_json_failure(tmp_path: Path) -> None:
    inputs = final_inputs(tmp_path / "inputs")
    cfg = yaml.safe_load(inputs["config_path"].read_text())
    cfg["minimum_prediction_episodes"] = 1
    inputs["config_path"].write_text(yaml.safe_dump(cfg))
    args = ["analysis", "finalize", "--output-root", str(tmp_path / "out")]
    for flag, key in (
        ("--catalog", "catalog_path"),
        ("--request-plan", "request_plan_path"),
        ("--observations", "observations_path"),
        ("--raw-root", "raw_root"),
        ("--config", "config_path"),
    ):
        args.extend([flag, str(inputs[key])])
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 2 and json.loads(result.stdout)["status"] == "error"
