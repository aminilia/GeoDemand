from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Polygon

from geodemand.catalog import build_provisional_catalog, discover_catalog_inputs
from geodemand.episodes import build_episodes, compare_episode_policies
from geodemand.trends_event_study import calculate_phase_metrics

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the deterministic synthetic GeoDemand demo.")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "demo-output")
    args = parser.parse_args()
    run_demo(args.output_dir)
    return 0


def run_demo(output_dir: Path) -> dict[str, str]:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    cohort = output_dir / "candidate_cohort"
    events = cohort / "eligible_event_records"
    states = cohort / "event_state_records"
    events.mkdir(parents=True)
    states.mkdir(parents=True)
    event_rows = [_event("synthetic-1", 0.0), _event("synthetic-2", 0.15)]
    _write(events / "part-00000.parquet", event_rows)
    _write(states / "part-00000.parquet", [_state(row["event_record_id"]) for row in event_rows])
    _json(
        cohort / "cohort_summary.json",
        {"data_origin": "synthetic_fixture", "eligible_primary_rows": len(event_rows)},
    )

    episode_root = output_dir / "episode_policies"
    for policy in ("conservative", "balanced"):
        build_episodes(events, states, episode_root / policy, policy)  # type: ignore[arg-type]
    comparison = output_dir / "episode_comparison"
    compare_episode_policies(events, states, comparison)
    catalog = output_dir / "provisional_catalog"
    build_provisional_catalog(
        discover_catalog_inputs(cohort, episode_root, comparison),
        catalog,
        ROOT / "config" / "catalog_rules.yaml",
    )

    trends = output_dir / "trends"
    plan = trends / "synthetic_request_plan.parquet"
    observations = trends / "synthetic_imported_observations.parquet"
    _write(plan, [_plan_row()])
    _write(observations, _trend_rows())
    phase = calculate_phase_metrics(
        observations,
        plan,
        ROOT / "config" / "trends_terms.yaml",
        ROOT / "config" / "trends_rules.yaml",
        trends,
    )
    plot = trends / "synthetic_interest.svg"
    plot.write_text(_svg(), encoding="utf-8")
    manifest = {
        "data_origin": "synthetic_fixture",
        "candidate_rows": len(event_rows),
        "artifacts": {
            "catalog": _sha256(catalog / "episodes.parquet"),
            "phase_metrics": _sha256(phase["phase_metrics"]),
            "plot": _sha256(plot),
        },
    }
    _json(output_dir / "demo_manifest.json", manifest)
    return manifest["artifacts"]


def _event(event_id: str, x: float) -> dict[str, Any]:
    geometry = Polygon([(x, 30.0), (x + 0.2, 30.0), (x + 0.2, 30.2), (x, 30.2)])
    return {
        "event_record_id": event_id,
        "source_uuid": event_id,
        "start_date": date(2024, 6, 10),
        "end_date": date(2024, 6, 11),
        "duration_days": 1,
        "primary_state_code": "FL",
        "primary_state_name": "Florida",
        "state_candidate_count": 1,
        "multi_state_event": False,
        "intersecting_state_codes": "FL",
        "event_area_km2": 1.0,
        "us_overlap_area_km2": 1.0,
        "representative_longitude": geometry.representative_point().x,
        "representative_latitude": geometry.representative_point().y,
        "study_domain": "conus",
        "spatial_review_required": False,
        "spatial_review_reason": "",
        "candidate_split": "validation",
        "geometry": geometry.wkb,
    }


def _state(event_id: object) -> dict[str, Any]:
    return {
        "event_record_id": event_id,
        "state_code": "FL",
        "state_name": "Florida",
        "start_date": date(2024, 6, 10),
        "end_date": date(2024, 6, 11),
        "state_overlap_area_km2": 1.0,
        "event_area_fraction": 1.0,
        "us_overlap_fraction": 1.0,
        "primary_state": True,
    }


def _plan_row() -> dict[str, Any]:
    return {
        "request_id": "synthetic-request",
        "episode_id": "synthetic-episode",
        "geography": "US-FL",
        "geography_level": "state",
        "batch_id": "synthetic_batch",
        "comparison_batch_id": "synthetic_batch",
        "request_role": "treated_state_comparison",
        "treated_geography": "US-FL",
        "event_start_date": date(2024, 6, 10),
        "event_end_date": date(2024, 6, 11),
        "baseline_start_date": date(2024, 5, 1),
        "baseline_end_date": date(2024, 6, 9),
        "post_start_date": date(2024, 6, 12),
        "post_end_date": date(2024, 7, 11),
        "concept_ids": "flood;walmart",
        "query_terms": "flood;Walmart",
        "terminology_version": "0.8A-v3",
    }


def _trend_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = date(2024, 5, 1)
    for offset in range(72):
        day = start + timedelta(days=offset)
        for concept, peak in (("flood", 80.0), ("walmart", 55.0)):
            rows.append(
                {
                    "request_id": "synthetic-request",
                    "repeat_id": "repeat-1",
                    "export_attempt_id": "synthetic-attempt-1",
                    "episode_id": "synthetic-episode",
                    "geography": "US-FL",
                    "concept_id": concept,
                    "query_text": concept,
                    "terminology_version": "0.8A-v3",
                    "date": day,
                    "interest": peak if day == date(2024, 6, 10) else 5.0,
                    "is_partial": False,
                }
            )
    return rows


def _svg() -> str:
    return """<svg xmlns="http://www.w3.org/2000/svg" width="640" height="240"
viewBox="0 0 640 240">
<rect width="640" height="240" fill="white"/>
<text x="24" y="28" font-family="sans-serif" font-size="16">
Synthetic Google Trends interest</text>
<polyline points="24,200 280,190 340,55 400,185 616,195" fill="none"
stroke="#1565c0" stroke-width="3"/>
<line x1="340" y1="40" x2="340" y2="205" stroke="#d32f2f" stroke-dasharray="5 4"/>
<text x="350" y="58" font-family="sans-serif" font-size="12">synthetic event</text>
</svg>
"""


def _write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")


def _json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
