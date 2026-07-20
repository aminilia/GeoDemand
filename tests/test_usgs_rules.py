from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand.usgs import extract_usgs

RULES = Path(__file__).parents[1] / "config" / "usgs_response_rules.yaml"


def test_missing_window_edges_reduce_coverage(tmp_path: Path) -> None:
    rows = _observations("g1", rise=6.0)
    for label, subset in (("missing-start", rows[23:]), ("missing-end", rows[:-23])):
        metrics = _extract(tmp_path / label, subset, [_association("g1")])
        assert metrics[0]["adequate_temporal_coverage"] is False
        assert metrics[0]["response_detected"] is False


def test_irregular_cadence_is_explicit(tmp_path: Path) -> None:
    rows = _observations("g1", rise=6.0)
    del rows[10:15]
    metric = _extract(tmp_path, rows, [_association("g1")])[0]
    assert metric["cadence_status"] == "irregular"
    assert metric["coverage_fraction"] is None
    assert metric["response_assessment_reason"] == "inadequate_temporal_coverage"


def test_null_rise_and_zero_baseline_are_safe(tmp_path: Path) -> None:
    no_pre = [row for row in _observations("g1", rise=6.0) if _hour(row) >= 24]
    null_metric = _extract(tmp_path / "null", no_pre, [_association("g1")])[0]
    assert null_metric["absolute_rise"] is None
    assert null_metric["response_detected"] is False

    zero = _observations("g2", baseline=0.0, rise=2.0)
    zero_metric = _extract(tmp_path / "zero", zero, [_association("g2")])[0]
    assert zero_metric["relative_rise"] is None
    assert zero_metric["response_detected"] is True


def test_strongest_gauge_uses_response_score_not_row_order(tmp_path: Path) -> None:
    weak = _observations("g-weak", rise=2.0)
    strong = _observations("g-strong", rise=10.0)
    root = tmp_path / "strength"
    metrics = _extract(root, [*weak, *strong], [_association("g-weak"), _association("g-strong")])
    assert len(metrics) == 2
    summary = pq.read_table(root / "metrics" / "usgs_episode_response_summary.parquet").to_pylist()[
        0
    ]
    assert summary["strongest_response_gauge"] == "g-strong"
    assert summary["interpretation_scope"] == "nearby_monitoring_location_not_connectivity_proof"


def test_weak_association_and_no_gauge_do_not_become_support(tmp_path: Path) -> None:
    weak = _association("g1", "fallback_within_100km")
    metric = _extract(tmp_path / "weak", _observations("g1", rise=10.0), [weak])[0]
    assert metric["association_allowed"] is False
    assert metric["response_assessment_reason"] == "weak_geographic_association"

    root = tmp_path / "none"
    _extract(root, [], [_association("", "no_suitable_gauge")])
    summary = pq.read_table(root / "metrics" / "usgs_episode_response_summary.parquet").to_pylist()[
        0
    ]
    assert summary["nearby_gauge_response_detected"] == "unknown"


def test_provisional_status_and_qualifiers_are_preserved(tmp_path: Path) -> None:
    rows = _observations("g1", rise=6.0)
    rows[25]["approval_status"] = "provisional"
    rows[25]["qualifier"] = "P"
    metric = _extract(tmp_path, rows, [_association("g1")])[0]
    assert metric["quality_acceptable"] is True
    assert metric["approval_statuses"] == "approved;provisional"
    assert metric["qualifiers"] == "P"
    assert metric["provisional_observation_fraction"] > 0


def test_excluded_qualifier_prevents_response(tmp_path: Path) -> None:
    rows = _observations("g1", rise=10.0)
    rows[25]["qualifier"] = "Ice"
    metric = _extract(tmp_path, rows, [_association("g1")])[0]
    assert metric["response_detected"] is False
    assert metric["quality_reasons"] == "excluded_qualifier"


def _extract(
    root: Path, observations: list[dict[str, Any]], associations: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    observation_path = root / "observations.parquet"
    association_path = root / "associations.parquet"
    _write(observation_path, observations)
    _write(association_path, associations)
    paths = extract_usgs(observation_path, association_path, root, RULES)
    return pq.read_table(paths["usgs_gauge_response_metrics"]).to_pylist()


def _observations(gauge: str, baseline: float = 1.0, rise: float = 6.0) -> list[dict[str, Any]]:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=84)
    return [
        {
            "episode_id": "episode-1",
            "monitoring_location_id": gauge,
            "parameter_code": "00060",
            "request_start_utc": start.isoformat(),
            "request_end_utc": end.isoformat(),
            "time_utc": (start + timedelta(hours=hour)).isoformat(),
            "value": baseline if hour < 24 else baseline + rise,
            "units": "ft3/s",
            "qualifier": "",
            "approval_status": "approved",
            "backend": "waterdata",
        }
        for hour in range(85)
    ]


def _association(gauge: str, quality: str = "inside_25km_buffer") -> dict[str, Any]:
    return {
        "episode_id": "episode-1",
        "monitoring_location_id": gauge,
        "association_quality": quality,
        "distance_to_episode_km": 10.0 if gauge else None,
    }


def _hour(row: dict[str, Any]) -> int:
    start = datetime.fromisoformat(str(row["request_start_utc"]))
    observed = datetime.fromisoformat(str(row["time_utc"]))
    return int((observed - start).total_seconds() // 3600)


def _write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        pq.write_table(pa.Table.from_pylist(rows), path)
    else:
        pq.write_table(
            pa.table(
                {
                    "episode_id": pa.array([], pa.string()),
                    "monitoring_location_id": pa.array([], pa.string()),
                    "parameter_code": pa.array([], pa.string()),
                }
            ),
            path,
        )
