from __future__ import annotations

from pathlib import Path

from scripts.synthetic_demo import run_demo


def test_synthetic_demo_is_complete_and_deterministic(tmp_path: Path) -> None:
    first = run_demo(tmp_path / "first")
    second = run_demo(tmp_path / "second")
    assert first == second
    assert set(first) == {"raw_artifact_hashes", "canonical_content_hashes"}
    assert set(first["canonical_content_hashes"]) == {"catalog", "phase_metrics"}
    assert (tmp_path / "first" / "candidate_cohort" / "eligible_event_records").is_dir()
    assert (tmp_path / "first" / "provisional_catalog" / "episodes.parquet").exists()
    assert (tmp_path / "first" / "trends" / "synthetic_interest.svg").exists()
