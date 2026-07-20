from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from typer.testing import CliRunner

from geodemand.cli import app
from geodemand.trends import (
    TrendsError,
    assess_episodes,
    calculate_metrics,
    evaluate_terms,
    import_csv_export,
    load_terms_config,
    map_geographies,
    official_api_selfcheck,
    pilot_sample,
    plan_requests,
    validate_imports,
    write_quicklooks,
)

ROOT = Path(__file__).parents[1]
RULES = ROOT / "config" / "trends_rules.yaml"
TERMS = ROOT / "config" / "trends_terms.yaml"


def test_pilot_mapping_and_plan_are_deterministic(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"
    first_pilot = pilot_sample(catalog, first, RULES)
    second_pilot = pilot_sample(catalog, second, RULES)
    first_geo = map_geographies(first_pilot["pilot"], first)
    second_geo = map_geographies(second_pilot["pilot"], second)
    first_plan = plan_requests(first_pilot["pilot"], first_geo["mapping"], TERMS, RULES, first)
    second_plan = plan_requests(second_pilot["pilot"], second_geo["mapping"], TERMS, RULES, second)

    pilot_rows = _rows(first_pilot["pilot"])
    assert len(pilot_rows) == 40
    assert {str(row["start_date"])[:4] for row in pilot_rows} == {"2022", "2023", "2024", "2025"}
    assert all(count == 10 for count in _year_counts(pilot_rows).values())
    assert sum(row["primary_state"] == "FL" for row in pilot_rows) >= 2
    assert _sha(first_pilot["pilot"]) == _sha(second_pilot["pilot"])
    assert _sha(first_geo["mapping"]) == _sha(second_geo["mapping"])
    assert _sha(first_plan["parquet"]) == _sha(second_plan["parquet"])
    plan_rows = _rows(first_plan["parquet"])
    assert len(plan_rows) == 200
    assert max(int(row["group_count"]) for row in plan_rows) == 5
    mini_rows = _rows(first_plan["mini_pilot_parquet"])
    assert len(mini_rows) == 26
    assert len({str(row["request_id"]) for row in mini_rows}) == 25
    assert len({str(row["episode_id"]) for row in mini_rows}) == 5
    assert len({str(row["geography"]) for row in mini_rows}) >= 3
    assert len({str(row["event_start_date"])[:4] for row in mini_rows}) >= 3
    florida_batch_one = [
        row
        for row in mini_rows
        if row["geography"] == "US-FL" and row["batch_id"] == "batch_1_flood_awareness"
    ]
    assert len(florida_batch_one) == 2
    assert len({str(row["request_id"]) for row in florida_batch_one}) == 1
    assert len({str(row["explore_url"]) for row in florida_batch_one}) == 1
    assert {str(row["planned_repeat_id"]) for row in florida_batch_one} == {
        "repeat_1",
        "repeat_2",
    }
    assert (
        min(
            (
                date.fromisoformat(str(row["request_end_date"]))
                - date.fromisoformat(str(row["request_start_date"]))
            ).days
            + 1
            for row in plan_rows
        )
        >= 60
    )


def test_required_state_matched_replacement_preserves_size_and_years(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    base_rules = _rules(tmp_path / "base-rules.yaml", [], 1)
    baseline = _rows(pilot_sample(catalog, tmp_path / "baseline", base_rules)["pilot"])
    baseline_counts = _state_counts(baseline)
    eligible_states = ["CA", "TX", "PA", "FL", "IL", "WA", "CO", "GA", "NY", "AZ", "NC", "MO"]
    required_state = min(eligible_states, key=lambda state: baseline_counts.get(state, 0))
    assert baseline_counts.get(required_state, 0) < 4

    required_rules = _rules(tmp_path / "required-rules.yaml", [required_state], 4)
    paths = pilot_sample(catalog, tmp_path / "required", required_rules)
    selected = _rows(paths["pilot"])
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    replacements = manifest["required_state_inclusion"]["replacements"]

    assert len(selected) == 40
    assert all(count == 10 for count in _year_counts(selected).values())
    assert _state_counts(selected)[required_state] == 4
    assert len(replacements) == 4 - baseline_counts.get(required_state, 0)
    assert all("year" in row["matched_attributes"] for row in replacements)
    assert all(row["added_episode_id"] != row["removed_episode_id"] for row in replacements)


def test_required_state_without_eligible_episodes_fails_clearly(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    rules = _rules(tmp_path / "missing-state-rules.yaml", ["DC"], 1)
    with pytest.raises(TrendsError, match="Required state DC has only 0 eligible episodes"):
        pilot_sample(catalog, tmp_path / "output", rules)


def test_multistate_mapping_uses_primary_state(tmp_path: Path) -> None:
    pilot = tmp_path / "pilot.parquet"
    _write(pilot, [{"provisional_episode_id": "e1", "primary_state": "CA", "states": "CA;NV"}])
    paths = map_geographies(pilot, tmp_path / "out")
    row = _rows(paths["mapping"])[0]
    assert row["trends_geography_identifier"] == "US-CA"
    assert row["multi_state_flag"] is True
    assert row["secondary_states"] == "NV"


def test_manual_csv_import_metrics_repeats_and_quicklooks(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    output = tmp_path / "trends"
    pilot = pilot_sample(catalog, output, RULES)["pilot"]
    geography = map_geographies(pilot, output)["mapping"]
    plan = plan_requests(pilot, geography, TERMS, RULES, output)["parquet"]
    request = _rows(plan)[0]
    concepts = str(request["concept_ids"]).split(";")
    labels = ["weather", "flood", "flooding", "flash flood", "heavy rain"]
    families = ["context", *["flood_awareness"] * 3, "context"]

    for repeat, offset in (("repeat-1", 0), ("repeat-2", 1)):
        csv_path = tmp_path / f"{repeat}.csv"
        sidecar = tmp_path / f"{repeat}.json"
        _export(csv_path, request, labels, offset)
        _sidecar(sidecar, csv_path.name, request, concepts, labels, families, repeat)
        result = import_csv_export(csv_path, sidecar, output)
        assert result["imported_rows"] == 300
        assert (output / "raw" / csv_path.name).exists()

    observations = output / "processed" / "trends_observations.parquet"
    validation = validate_imports(observations, plan)
    assert validation["repeat_count"] == 2
    artifacts = calculate_metrics(observations, plan, RULES, output)
    metrics = _rows(artifacts["metrics"])
    flood = next(row for row in metrics if row["concept_id"] == "flood")
    assert flood["baseline_valid_day_count"] == 28
    assert flood["absolute_peak_lift"] is not None
    assert flood["anchor_ratio_peak"] is None
    assert flood["repeat_stability_flag"] is True
    assert "reported_zero" not in str(flood["metric_quality_reasons"])

    term_paths = evaluate_terms(artifacts["metrics"], plan, TERMS, RULES, output)
    assert len(_rows(term_paths["feasibility"])) == 62
    pair_rows = _rows(term_paths["broad_specific_parquet"])
    assert len(pair_rows) == 14
    assert {
        "usable_episode_count",
        "all_zero_episode_count",
        "broad_median_zero_fraction",
        "specific_median_zero_fraction",
        "broad_median_baseline_value",
        "specific_median_baseline_value",
        "broad_median_event_maximum",
        "specific_median_event_maximum",
        "broad_repeat_stability",
        "specific_repeat_stability",
        "event_lift_availability",
        "broad_term_preferred",
        "specific_term_preferred",
        "retain_both",
        "insufficient_evidence",
    }.issubset(pair_rows[0])
    assessment = assess_episodes(pilot, geography, plan, artifacts["metrics"], output)
    statuses = {row["episode_trends_status"] for row in _rows(assessment["feasibility"])}
    assert "feasible_with_state_level_limitations" in statuses
    quicklooks = write_quicklooks(observations, artifacts["metrics"], plan, output)
    assert [path.name for path in quicklooks] == [
        "strong_response.svg",
        "low_volume.svg",
        "all_zero.svg",
        "unstable_repeat.svg",
        "anchor_normalization.svg",
        "awareness_recovery.svg",
        "state_geography_limit.svg",
    ]


def test_zero_anchor_and_all_zero_are_preserved(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    output = tmp_path / "trends"
    pilot = pilot_sample(catalog, output, RULES)["pilot"]
    geography = map_geographies(pilot, output)["mapping"]
    plan = plan_requests(pilot, geography, TERMS, RULES, output)["parquet"]
    request = _rows(plan)[0]
    csv_path = tmp_path / "zeros.csv"
    sidecar = tmp_path / "zeros.json"
    labels = ["weather", "flood"]
    concepts = ["anchor_weather", "flood"]
    _zero_export(csv_path, request, labels)
    _sidecar(
        sidecar,
        csv_path.name,
        request,
        concepts,
        labels,
        ["anchor_control", "flood_awareness"],
        "zero-repeat",
    )
    import_csv_export(csv_path, sidecar, output)
    observations = output / "processed" / "trends_observations.parquet"
    assert all(float(row["interest"]) == 0 for row in _rows(observations))
    artifacts = calculate_metrics(observations, plan, RULES, output)
    flood = next(row for row in _rows(artifacts["metrics"]) if row["concept_id"] == "flood")
    assert flood["metric_quality_status"] == "all_zero"
    assert flood["ratio_peak_lift"] is None
    assert flood["anchor_ratio_peak"] is None


def test_sidecar_and_csv_validation_errors(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("not,interest\n1,2\n", encoding="utf-8")
    with pytest.raises(TrendsError, match="sidecar"):
        import_csv_export(csv_path, tmp_path / "missing.json", tmp_path / "out")

    duplicate = tmp_path / "duplicate.csv"
    duplicate.write_text("Day,flood\n2024-01-01,1\n2024-01-01,2\n", encoding="utf-8")
    sidecar = tmp_path / "duplicate.json"
    payload = {
        "request_id": "r1",
        "backend": "manual_csv",
        "requested_concepts": ["flood"],
        "requested_labels": ["flood"],
        "query_types": ["topic"],
        "semantic_families": ["flood_awareness"],
        "geography": "US-CA",
        "geography_level": "state",
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "category": 0,
        "search_property": "web_search",
        "export_date": "2024-02-01",
        "source_filename": duplicate.name,
        "episode_id": "e1",
    }
    sidecar.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(TrendsError, match="Duplicate date"):
        import_csv_export(duplicate, sidecar, tmp_path / "out")


def test_official_api_unavailable_and_cli_help() -> None:
    status = official_api_selfcheck()
    assert status["official_api_status"] == "unavailable_or_not_authorized"
    assert status["network_attempted"] is False
    assert "pytrends" not in (ROOT / "pyproject.toml").read_text(encoding="utf-8").lower()
    result = CliRunner().invoke(app, ["trends", "--help"])
    assert result.exit_code == 0
    assert "official-api-selfcheck" in result.stdout
    assert "import-csv" in result.stdout
    assert "select-controls" in result.stdout
    assert "phase-metrics" in result.stdout
    assert "concurrence" in result.stdout
    assert "control-adjusted-metrics" in result.stdout
    assert "attribute-peaks" in result.stdout


def test_standardized_batches_and_weather_context_contract(tmp_path: Path) -> None:
    terms = load_terms_config(TERMS)
    expected = [
        ["context_weather", "flood", "flooding", "flash_flood", "context_heavy_rain"],
        ["context_weather", "water_damage", "cleanup", "mold", "insurance"],
        ["context_weather", "fema", "emergency", "evacuation", "shelter"],
        ["context_weather", "outage", "road_closed", "school_closed", "traffic"],
        [
            "context_weather",
            "flood_cleanup",
            "water_removal",
            "flood_insurance",
            "fema_assistance",
        ],
    ]
    assert [row["concepts"] for row in terms["standardized_batches"]] == expected
    assert len(terms["broad_specific_pairs"]) == 14
    weather = next(row for row in terms["concepts"] if row["concept_id"] == "context_weather")
    assert weather["terminology_tier"] == "context"
    assert weather["context_candidate"] is True
    assert weather["anchor_candidate"] is False

    catalog = _catalog(tmp_path)
    output = tmp_path / "plan"
    pilot = pilot_sample(catalog, output, RULES)["pilot"]
    geography = map_geographies(pilot, output)["mapping"]
    plan = _rows(plan_requests(pilot, geography, TERMS, RULES, output)["parquet"])
    assert all(row["anchor_concept"] is None for row in plan)
    for rows in _group_rows(plan, "episode_id").values():
        assert [str(row["concept_ids"]).split(";") for row in rows] == expected


def test_disruption_tiers_pairs_and_ambiguous_closure_exclusion() -> None:
    terms = load_terms_config(TERMS)
    concepts = {row["concept_id"]: row for row in terms["concepts"]}
    assert terms["dictionary_version"] == "0.8A-v3"
    assert {
        concept_id
        for concept_id, row in concepts.items()
        if row["semantic_family"] == "disruption" and row["terminology_tier"] == "primary_broad"
    } == {"outage", "road_closed", "school_closed", "traffic"}
    assert concepts["power_outage"]["terminology_tier"] == "secondary_specific"
    assert concepts["road_closure"]["terminology_tier"] == "secondary_specific"
    assert concepts["school_closure"]["terminology_tier"] == "secondary_specific"
    assert not any(
        row["query_text"] == "closure" and row["terminology_tier"] == "primary_broad"
        for row in concepts.values()
    )
    pairs = {
        (row["broad_concept_id"], row["specific_concept_id"])
        for row in terms["broad_specific_pairs"]
    }
    assert {
        ("outage", "power_outage"),
        ("outage", "no_power"),
        ("road_closed", "road_closure"),
        ("road_closed", "flooded_roads"),
        ("school_closed", "school_closure"),
        ("roads_closed", "road_closure"),
        ("schools_closed", "school_closure"),
    }.issubset(pairs)


def test_terminology_config_rejects_invalid_tier(tmp_path: Path) -> None:
    payload = yaml.safe_load(TERMS.read_text(encoding="utf-8"))
    payload["concepts"][0]["terminology_tier"] = "state_customized"
    path = tmp_path / "invalid-terms.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(TrendsError, match="Invalid terminology tier"):
        load_terms_config(path)


def test_event_sensitive_anchor_is_not_accepted_as_normalizer(tmp_path: Path) -> None:
    metrics = tmp_path / "metrics.parquet"
    plan = tmp_path / "plan.parquet"
    _write(
        metrics,
        [
            {
                "request_id": "r1",
                "provisional_episode_id": "e1",
                "concept_id": "anchor_news",
                "metric_quality_status": "usable",
                "event_maximum": 20.0,
                "baseline_mean": 10.0,
                "baseline_median": 10.0,
                "baseline_standard_deviation": 1.0,
                "zero_fraction": 0.0,
                "repeat_stability_flag": True,
            }
        ],
    )
    _write(
        plan,
        [
            {
                "request_id": "r1",
                "episode_id": "e1",
                "event_start_date": "2024-06-01",
                "geography": "US-FL",
                "concept_ids": "anchor_news",
            }
        ],
    )
    paths = evaluate_terms(metrics, plan, TERMS, RULES, tmp_path / "output")
    row = next(item for item in _rows(paths["feasibility"]) if item["concept_id"] == "anchor_news")
    assert row["classification"] == "event_sensitive_anchor"
    assert row["anchor_compatibility"] is False


def test_import_preserves_legacy_and_current_terminology_versions(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    output = tmp_path / "trends"
    pilot = pilot_sample(catalog, output, RULES)["pilot"]
    geography = map_geographies(pilot, output)["mapping"]
    request = _rows(plan_requests(pilot, geography, TERMS, RULES, output)["parquet"])[0]
    concepts = str(request["concept_ids"]).split(";")
    labels = ["weather", "flood", "flooding", "flash flood", "heavy rain"]
    families = ["context", *["flood_awareness"] * 3, "context"]
    for repeat, version in (
        ("legacy", None),
        ("prior", "0.8A-v2"),
        ("current", "0.8A-v3"),
    ):
        csv_path = tmp_path / f"{repeat}.csv"
        sidecar = tmp_path / f"{repeat}.json"
        _export(csv_path, request, labels, 0)
        _sidecar(sidecar, csv_path.name, request, concepts, labels, families, repeat)
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
        if version is not None:
            payload["terminology_version"] = version
        sidecar.write_text(json.dumps(payload), encoding="utf-8")
        import_csv_export(csv_path, sidecar, output)
    observations = output / "processed" / "trends_observations.parquet"
    versions = {str(row["terminology_version"]) for row in _rows(observations)}
    assert versions == {"unknown_legacy", "0.8A-v2", "0.8A-v3"}


def _catalog(tmp_path: Path) -> Path:
    catalog = tmp_path / "catalog"
    rows = []
    states = ["CA", "TX", "PA", "FL", "IL", "WA", "CO", "GA", "NY", "AZ", "NC", "MO"]
    for year in range(2022, 2026):
        for index in range(12):
            start = date(year, (index % 12) + 1, 10)
            rows.append(
                {
                    "provisional_episode_id": hashlib.sha256(
                        f"{year}-{index}".encode()
                    ).hexdigest(),
                    "start_date": start.isoformat(),
                    "end_date": start.isoformat(),
                    "candidate_split": "development"
                    if year < 2024
                    else "validation"
                    if year == 2024
                    else "test",
                    "primary_state": states[index],
                    "states": states[index],
                    "representative_latitude": 35.0 + index / 10,
                    "representative_longitude": -120.0 + index,
                    "member_count": 1 if index % 2 == 0 else 3,
                    "duration_days": 0,
                    "union_area_km2": 5.0 if index % 3 == 0 else 40.0,
                    "provisional_catalog_status": "retained_provisionally",
                    "manual_review_flag": False,
                    "split_boundary_flag": False,
                }
            )
    _write(catalog / "episodes.parquet", rows)
    return catalog


def _export(path: Path, request: dict[str, object], labels: list[str], offset: int) -> None:
    start = date.fromisoformat(str(request["request_start_date"]))
    end = date.fromisoformat(str(request["request_end_date"]))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["Category: All categories"])
        writer.writerow(["Day", *labels])
        day = start
        while day <= end:
            event = date.fromisoformat(str(request["event_start_date"]))
            spike = 60 if day == event else 0
            anchor = f"{50 + offset} (partial)" if day == end else 50 + offset
            writer.writerow(
                [
                    day.isoformat(),
                    anchor,
                    10 + spike + offset,
                    8 + spike // 2,
                    3 + spike // 3,
                    "<1" if day.day % 9 == 0 else 4 + spike // 4,
                ]
            )
            day += timedelta(days=1)


def _zero_export(path: Path, request: dict[str, object], labels: list[str]) -> None:
    start = date.fromisoformat(str(request["request_start_date"]))
    end = date.fromisoformat(str(request["request_end_date"]))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["Day", *labels])
        while start <= end:
            writer.writerow([start.isoformat(), 0, 0])
            start += timedelta(days=1)


def _sidecar(
    path: Path,
    filename: str,
    request: dict[str, object],
    concepts: list[str],
    labels: list[str],
    families: list[str],
    repeat: str,
) -> None:
    payload = {
        "request_id": request["request_id"],
        "backend": "manual_csv",
        "requested_concepts": concepts,
        "requested_labels": labels,
        "query_types": ["term"] * len(concepts),
        "semantic_families": families,
        "geography": request["geography"],
        "geography_level": "state",
        "start_date": request["request_start_date"],
        "end_date": request["request_end_date"],
        "category": 0,
        "search_property": "web_search",
        "export_date": "2026-07-15",
        "source_filename": filename,
        "episode_id": request["episode_id"],
        "anchor_concept_id": None,
        "repeat_id": repeat,
        "notes": "synthetic offline fixture",
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _year_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        year = str(row["start_date"])[:4]
        counts[year] = counts.get(year, 0) + 1
    return counts


def _state_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        state = str(row["primary_state"])
        counts[state] = counts.get(state, 0) + 1
    return counts


def _group_rows(rows: list[dict[str, object]], key: str) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(str(row[key]), []).append(row)
    return grouped


def _rules(path: Path, required_states: list[str], minimum: int) -> Path:
    payload = yaml.safe_load(RULES.read_text(encoding="utf-8"))
    payload["required_states"] = required_states
    payload["minimum_required_state_episodes"] = minimum
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
    return path
