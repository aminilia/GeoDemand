from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geodemand.mrms import (
    MrmsError,
    closest_object,
    discover_episode_outputs,
    extract_mrms,
    fetch_mrms,
    inventory_mrms,
    mask_precipitation,
    parse_mrms_key,
    peak_count,
    rolling_max,
    sample_mrms,
)


def test_discovery_fails_for_incomplete_episode_output(tmp_path: Path) -> None:
    (tmp_path / "clustering_sensitivity.csv").write_text("policy,episode_count\n", encoding="utf-8")

    with pytest.raises(MrmsError, match="Missing: conservative episodes"):
        discover_episode_outputs(tmp_path)


def test_sample_selection_is_deterministic_and_includes_merge_groups(
    episode_root: Path,
    tmp_path: Path,
) -> None:
    first = sample_mrms(episode_root, tmp_path / "first", seed=7)
    second = sample_mrms(episode_root, tmp_path / "second", seed=7)

    first_rows = pq.read_table(first["verification_sample"]).to_pylist()
    second_rows = pq.read_table(second["verification_sample"]).to_pylist()
    comparison_rows = pq.read_table(first["comparison_groups"]).to_pylist()

    assert first_rows == second_rows
    assert any(row["selection_stratum"] == "balanced_merge_candidate" for row in first_rows)
    assert any(row["selection_stratum"] == "split_boundary" for row in first_rows)
    assert any(row["spatial_review_required"] for row in first_rows)
    assert any(row["additional_balanced_members"] > 0 for row in comparison_rows)


def test_s3_key_parsing_and_closest_timestamp_selection() -> None:
    key = (
        "CONUS/MultiSensor_QPE_01H_Pass2/20240102/"
        "MRMS_MultiSensor_QPE_01H_Pass2_00.00_20240102-010000.grib2.gz"
    )
    parsed = parse_mrms_key(key)

    assert parsed["product"] == "MultiSensor_QPE_01H_Pass2"
    assert parsed["valid_time_utc"].isoformat() == "2024-01-02T01:00:00+00:00"
    selected = closest_object(
        [{"Key": key, "Size": 123, "ETag": "abc"}],
        "MultiSensor_QPE_01H_Pass2",
        parsed["valid_time_utc"],
    )
    assert selected["availability_status"] == "available"
    assert selected["time_offset_seconds"] == 0


def test_inventory_deduplicates_objects_and_estimates_bytes(
    episode_root: Path,
    tmp_path: Path,
) -> None:
    sample = sample_mrms(episode_root, tmp_path / "sample", seed=7, max_episodes=3)
    fs = FakeS3()

    artifacts = inventory_mrms(
        sample["verification_sample"], tmp_path / "mrms", fs=fs, max_episodes=2
    )
    rows = pq.read_table(artifacts["mrms_inventory"]).to_pylist()
    summary = artifacts["inventory_summary"].read_text(encoding="utf-8")

    assert rows
    assert len({(row["product"], row["key"]) for row in rows}) == len(rows)
    assert "estimated_compressed_download_bytes" in summary


def test_fetch_uses_cache_and_validates_checksum(tmp_path: Path) -> None:
    fs = FakeS3()
    plan = tmp_path / "plan.csv"
    key = FakeS3.key_for("MultiSensor_QPE_01H_Pass2", "20240102", "010000")
    plan.write_text(
        "\n".join(
            [
                "bucket,key,s3_uri,product,expected_valid_time_utc,discovered_valid_time_utc,"
                "time_offset_seconds,content_length,etag,last_modified,requesting_episode_ids,"
                "availability_status,selection_reason",
                f"noaa-mrms-pds,{key},s3://noaa-mrms-pds/{key},MultiSensor_QPE_01H_Pass2,"
                "2024-01-02T01:00:00+00:00,2024-01-02T01:00:00+00:00,0,7,etag,,e1,"
                "available,closest_valid_timestamp",
            ]
        ),
        encoding="utf-8",
    )

    fetch_mrms(plan, tmp_path / "mrms", fs=fs)
    second = fetch_mrms(plan, tmp_path / "mrms", fs=fs)
    rows = pq.read_table(second["mrms_file_manifest"]).to_pylist()

    assert fs.download_count == 1
    assert rows[0]["cache_hit"] is True
    assert len(rows[0]["compressed_sha256"]) == 64


def test_precipitation_masking_rolling_and_peak_detection() -> None:
    assert mask_precipitation([0, 1.5, -999]) == [0.0, 1.5, None]
    with pytest.raises(MrmsError, match="Negative precipitation"):
        mask_precipitation([-1])
    assert rolling_max([1, 2, None, 4], 2) == 4.0
    assert peak_count([0, 2, 2, 0, 0, 0, 0, 0, 0, 3], dry_gap_hours=3) == 2


def test_production_extract_refuses_to_emit_synthetic_metrics(
    episode_root: Path, tmp_path: Path
) -> None:
    sample = sample_mrms(episode_root, tmp_path / "sample", seed=7, max_episodes=2)
    fs = FakeS3()
    inventory = inventory_mrms(
        sample["verification_sample"], tmp_path / "mrms", fs=fs, max_episodes=1
    )
    fetched = fetch_mrms(inventory["mrms_download_plan"], tmp_path / "mrms", fs=fs)
    with pytest.raises(MrmsError, match="real_extraction_not_implemented"):
        extract_mrms(
            sample["verification_sample"], fetched["mrms_file_manifest"], tmp_path / "mrms"
        )
    assert not (tmp_path / "mrms" / "metrics" / "episode_precipitation_metrics.parquet").exists()


@pytest.fixture()
def episode_root(tmp_path: Path) -> Path:
    root = tmp_path / "episodes"
    for policy in ["conservative", "balanced"]:
        (root / f"episodes_{policy}" / "episodes").mkdir(parents=True)
        (root / f"episodes_{policy}" / "episode_membership").mkdir(parents=True)
    _write_parquet(root / "episodes_conservative" / "episodes" / "part-00000.parquet", _episodes())
    _write_parquet(
        root / "episodes_conservative" / "episode_membership" / "part-00000.parquet",
        _membership("conservative"),
    )
    _write_parquet(
        root / "episodes_balanced" / "episodes" / "part-00000.parquet", _balanced_episodes()
    )
    _write_parquet(
        root / "episodes_balanced" / "episode_membership" / "part-00000.parquet",
        _membership("balanced"),
    )
    (root / "clustering_sensitivity.csv").write_text("policy,episode_count\n", encoding="utf-8")
    (root / "clustering_agreement.csv").write_text(
        "comparison,pairwise_membership_agreement\n", encoding="utf-8"
    )
    return root


class FakeS3:
    content = b"gribish"

    def __init__(self) -> None:
        self.download_count = 0

    @staticmethod
    def key_for(product: str, day: str, hhmmss: str) -> str:
        return f"CONUS/{product}/{day}/MRMS_{product}_00.00_{day}-{hhmmss}.grib2.gz"

    def ls(self, path: str, detail: bool = True) -> list[dict[str, Any]]:
        del detail
        parts = path.rstrip("/").split("/")
        product = parts[-2]
        day = parts[-1]
        return [
            {
                "Key": self.key_for(product, day, "000000"),
                "Size": len(self.content),
                "ETag": "etag",
                "LastModified": "2024-01-02T00:00:00+00:00",
            },
            {
                "Key": self.key_for(product, day, "010000"),
                "Size": len(self.content),
                "ETag": "etag",
                "LastModified": "2024-01-02T01:00:00+00:00",
            },
        ]

    def info(self, path: str) -> dict[str, Any]:
        del path
        return {"Size": len(self.content), "ETag": "etag"}

    def open(self, path: str, mode: str = "rb") -> io.BytesIO:
        del path, mode
        self.download_count += 1
        return io.BytesIO(self.content)


def _episodes() -> list[dict[str, object]]:
    rows = []
    for index in range(1, 24):
        rows.append(
            {
                "episode_id": f"c{index}",
                "policy": "conservative",
                "episode_start_date": f"2024-01-{(index % 9) + 1:02d}",
                "episode_end_date": f"2024-01-{(index % 9) + 1:02d}",
                "episode_duration_days": index % 6,
                "member_count": 1 if index % 5 == 0 else (index % 8) + 2,
                "state_codes": "CA;NV" if index % 4 == 0 else "CA",
                "state_count": 2 if index % 4 == 0 else 1,
                "primary_state_code": "CA",
                "union_area_km2": 1.0,
                "representative_longitude": -120.0 + index / 100,
                "representative_latitude": 38.0,
                "candidate_split": "validation",
                "crosses_split_boundary": index == 3,
                "spatial_review_required": index in {4, 8, 12, 16, 20},
                "clustering_review_reason": "synthetic" if index in {4, 8, 12, 16, 20} else "",
            }
        )
    return rows


def _balanced_episodes() -> list[dict[str, object]]:
    return [
        {**row, "episode_id": f"b{math_group}", "policy": "balanced"}
        for math_group, row in enumerate(_episodes(), start=1)
    ]


def _membership(policy: str) -> list[dict[str, object]]:
    rows = []
    if policy == "conservative":
        for episode in _episodes():
            for index in range(int(episode["member_count"])):
                rows.append(
                    {
                        "episode_id": episode["episode_id"],
                        "event_record_id": f"{episode['episode_id']}_m{index}",
                    }
                )
        return rows
    for episode in _episodes():
        balanced_id = (
            "bmerge" if episode["episode_id"] in {"c1", "c2", "c3"} else f"b{episode['episode_id']}"
        )
        for index in range(int(episode["member_count"])):
            rows.append(
                {
                    "episode_id": balanced_id,
                    "event_record_id": f"{episode['episode_id']}_m{index}",
                }
            )
    return rows


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)
