from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geodemand.imerg import (
    EarthaccessImergClient,
    ImergError,
    aggregate_half_hours_to_hourly,
    extract_imerg,
    fetch_imerg,
    inventory_imerg,
    rate_to_half_hour_mm,
    selfcheck_imerg,
)
from geodemand.observations import (
    assess_observations,
    compare_precipitation,
    pilot_sample,
    write_review_stubs,
)
from geodemand.usgs import (
    DataretrievalUsgsClient,
    discover_usgs,
    extract_usgs,
    fetch_usgs,
    selfcheck_usgs,
)

ROOT = Path(__file__).parents[1]
USGS_RULES = ROOT / "config" / "usgs_response_rules.yaml"


def test_imerg_conversion_inventory_fetch_and_extract(tmp_path: Path) -> None:
    sample = _sample(tmp_path)
    client = FakeImergClient()

    assert rate_to_half_hour_mm(4.0) == 2.0
    assert rate_to_half_hour_mm(-9999.0, fill_value=-9999.0) is None
    assert aggregate_half_hours_to_hourly([1.0, 2.0, 3.0, None]) == [3.0, None]
    assert selfcheck_imerg(tmp_path)["authentication"]["strategy"] in {"none", "netrc"}

    inventory = inventory_imerg(sample, tmp_path / "imerg", client=client, max_episodes=2)
    rows = pq.read_table(inventory["imerg_inventory"]).to_pylist()
    assert len({row["granule_id"] for row in rows}) == len(rows)
    assert client.searches[0][0] == datetime(2021, 12, 31, 18, tzinfo=UTC)
    assert client.searches[0][1] == datetime(2022, 1, 1, 6, tzinfo=UTC)

    fetched = fetch_imerg(inventory["imerg_download_plan"], tmp_path / "imerg", client=client)
    fetched_again = fetch_imerg(inventory["imerg_download_plan"], tmp_path / "imerg", client=client)
    manifest = pq.read_table(fetched_again["imerg_file_manifest"]).to_pylist()
    assert client.download_count == len(rows)
    assert all(row["cache_hit"] for row in manifest)

    with pytest.raises(ImergError, match="real_imerg_extraction_not_implemented"):
        extract_imerg(sample, fetched["imerg_file_manifest"], tmp_path / "imerg")
    for relative in (
        "metrics/imerg_episode_precipitation_metrics.parquet",
        "metrics/imerg_member_precipitation_metrics.parquet",
        "metrics/imerg_episode_precipitation_timeseries.parquet",
    ):
        assert not (tmp_path / "imerg" / relative).exists()


def test_imerg_authentication_state_and_download_failure_cleanup(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setenv("EARTHDATA_TOKEN", "test-placeholder-token")
    checks = selfcheck_imerg(tmp_path)
    assert checks["authentication"] == {"available": True, "strategy": "EARTHDATA_TOKEN"}
    assert "test-placeholder-token" not in json.dumps(checks)

    sample = _sample(tmp_path)
    inventory = inventory_imerg(sample, tmp_path / "imerg", client=FakeImergClient())
    fetched = fetch_imerg(
        inventory["imerg_download_plan"],
        tmp_path / "failed",
        client=FailingImergClient(),
    )
    assert "simulated download failure" in fetched["imerg_download_failures"].read_text("utf-8")
    assert not list((tmp_path / "failed" / "cache").glob("*.tmp"))


def test_imerg_zero_fill_and_missing_half_hour_rules() -> None:
    assert rate_to_half_hour_mm(0.0) == 0.0
    assert rate_to_half_hour_mm(-9999.0, fill_value=-9999.0) is None
    assert aggregate_half_hours_to_hourly([0.0, 0.0, 1.0]) == [0.0, None]


def test_earthaccess_adapter_normalizes_cmr_result() -> None:
    module = FakeEarthaccessModule()
    client = EarthaccessImergClient(module=module)
    rows = client.search_granules(
        "GPM_3IMERGHH",
        "07",
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)),
        (-121.0, 37.0, -119.0, 39.0),
    )

    assert rows[0]["granule_id"] == "IMERG.TEST"
    assert rows[0]["concept_id"] == "G123"
    assert rows[0]["file_size"] == 2 * 1024 * 1024
    assert module.search_arguments["short_name"] == "GPM_3IMERGHH"


def test_usgs_discover_fetch_extract_and_no_gauge_unknown(tmp_path: Path) -> None:
    sample = _sample(tmp_path)
    client = FakeUsgsClient()

    assert selfcheck_usgs(tmp_path)["api_key_present"] is False
    discovered = discover_usgs(sample, tmp_path / "usgs", client=client, max_episodes=2)
    associations = pq.read_table(discovered["usgs_episode_gauge_associations"]).to_pylist()
    assert any(row["association_quality"] == "inside_25km_buffer" for row in associations)

    fetched = fetch_usgs(discovered["usgs_request_plan"], tmp_path / "usgs", client=client)
    observations = pq.read_table(fetched["usgs_observations"]).to_pylist()
    assert observations
    assert all(row["data_origin"] == "observed" for row in observations)
    assert all(row["schema_version"] == "physical-evidence-v1" for row in observations)
    extracted = extract_usgs(
        fetched["usgs_observations"],
        discovered["usgs_episode_gauge_associations"],
        tmp_path / "usgs",
        USGS_RULES,
    )
    metrics = pq.read_table(extracted["usgs_gauge_response_metrics"]).to_pylist()
    assert any(row["response_detected"] for row in metrics)

    no_gauge = discover_usgs(sample, tmp_path / "nogauge", client=FakeUsgsClient(no_gauges=True))
    fetched_empty = fetch_usgs(no_gauge["usgs_request_plan"], tmp_path / "nogauge", client=client)
    summary = extract_usgs(
        fetched_empty["usgs_observations"],
        no_gauge["usgs_episode_gauge_associations"],
        tmp_path / "nogauge",
        USGS_RULES,
    )
    rows = pq.read_table(summary["usgs_episode_response_summary"]).to_pylist()
    assert all(row["hydrologic_response_supported"] == "unknown" for row in rows)


def test_usgs_request_cache_reuses_identical_normalized_response(tmp_path: Path) -> None:
    sample = _sample(tmp_path)
    client = FakeUsgsClient()
    discovered = discover_usgs(sample, tmp_path / "usgs", client=client, max_episodes=1)
    first = fetch_usgs(discovered["usgs_request_plan"], tmp_path / "usgs", client=client)
    calls_after_first = client.fetch_count
    second = fetch_usgs(discovered["usgs_request_plan"], tmp_path / "usgs")

    first_rows = pq.read_table(first["usgs_observations"]).to_pylist()
    second_rows = pq.read_table(second["usgs_observations"]).to_pylist()
    manifest = pq.read_table(second["usgs_file_or_request_manifest"]).to_pylist()
    assert first_rows == second_rows
    assert client.fetch_count == calls_after_first
    assert all(row["cache_hit"] for row in manifest)
    assert all("api_key" not in row for row in manifest)


def test_dataretrieval_adapter_filters_and_normalizes_waterdata() -> None:
    client = DataretrievalUsgsClient(module=FakeDataretrievalModule())
    gauges = client.search_gauges(
        -120.0,
        38.0,
        50.0,
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 2, tzinfo=UTC),
        ["00060", "00065"],
    )
    observations = client.fetch_observations(
        "USGS-1",
        "00060",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 2, tzinfo=UTC),
    )

    assert gauges[0]["monitoring_location_id"] == "USGS-1"
    assert gauges[0]["parameter_codes"] == ["00060"]
    assert observations[0]["time_utc"] == "2024-01-01T01:00:00+00:00"
    assert observations[0]["qualifier"] == "P"
    assert observations[0]["approval_status"] == "provisional"


def test_observations_pilot_compare_assess_and_review_stubs(tmp_path: Path) -> None:
    sample = _sample(tmp_path)
    first = pilot_sample(sample, tmp_path / "pilot1", target_count=3)
    second = pilot_sample(sample, tmp_path / "pilot2", target_count=3)
    assert (
        pq.read_table(first["pilot_verification_sample"]).to_pylist()
        == pq.read_table(second["pilot_verification_sample"]).to_pylist()
    )

    mrms_metrics, mrms_series, imerg_metrics, imerg_series, usgs_summary = _source_outputs(tmp_path)
    compared = compare_precipitation(
        mrms_metrics,
        mrms_series,
        imerg_metrics,
        imerg_series,
        sample,
        tmp_path / "multi",
    )
    comparison_rows = pq.read_table(compared["mrms_imerg_episode_comparison"]).to_pylist()
    assert comparison_rows[0]["common_hour_count"] == 3
    assert comparison_rows[0]["mean_absolute_error_mm"] >= 0

    assessed = assess_observations(
        sample,
        mrms_metrics,
        imerg_metrics,
        compared["mrms_imerg_episode_comparison"],
        usgs_summary,
        tmp_path / "multi",
    )
    rows = pq.read_table(assessed["multisource_episode_assessment"]).to_pylist()
    assert any(row["integrated_evidence_category"] for row in rows)
    review_stubs = write_review_stubs(
        assessed["multisource_episode_assessment"], tmp_path / "multi"
    )
    assert review_stubs["review_stub_manifest"].exists()


def test_pilot_sample_hash_and_coverage_are_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "verification.parquet"
    rows = []
    states = ["CA", "TX", "NY", "FL", "WA", "CO"]
    for index in range(40):
        year = 2022 + index % 4
        month = 1 + index % 12
        rows.append(
            {
                "episode_id": f"episode-{index:02d}",
                "selection_stratum": (
                    "balanced_merge_candidate"
                    if index < 6
                    else "split_boundary"
                    if index < 9
                    else ""
                ),
                "member_count": 1 if index % 6 == 0 else 2 + index % 15,
                "episode_duration_days": 1 + index % 4,
                "state_codes": states[index % len(states)],
                "region": f"region-{index % 4}",
                "episode_start_date": f"{year}-{month:02d}-01",
                "episode_end_date": f"{year}-{month:02d}-02",
                "representative_longitude": -120.0 + index,
                "representative_latitude": 30.0 + index / 10,
                "additional_balanced_members": 2 if index < 6 else 0,
            }
        )
    _write(source, rows)
    first = pilot_sample(source, tmp_path / "first", target_count=30)
    second = pilot_sample(source, tmp_path / "second", target_count=30)
    first_manifest = json.loads(first["pilot_sample_manifest"].read_text("utf-8"))
    second_manifest = json.loads(second["pilot_sample_manifest"].read_text("utf-8"))
    summary = json.loads(first["pilot_sample_summary"].read_text("utf-8"))

    assert first_manifest == second_manifest
    assert pq.read_table(first["pilot_verification_sample"]).num_rows == 30
    assert set(summary["year_counts"]) == {"2022", "2023", "2024", "2025"}
    assert set(summary["season_counts"]) == {"fall", "spring", "summer", "winter"}
    assert len(summary["region_counts"]) == 4


class FakeImergClient:
    def __init__(self) -> None:
        self.download_count = 0
        self.searches: list[tuple[datetime, datetime]] = []

    def search_granules(
        self,
        short_name: str,
        version: str,
        temporal: tuple[datetime, datetime],
        bounding_box: tuple[float, float, float, float] | None,
    ) -> list[dict[str, Any]]:
        del short_name, version, bounding_box
        start = temporal[0]
        self.searches.append(temporal)
        return [
            {
                "granule_id": f"g{start:%Y%m%d%H}",
                "concept_id": "C123",
                "beginning_time_utc": start,
                "ending_time_utc": start.replace(minute=30),
                "production_time_utc": start,
                "bounding_box": [-125, 25, -65, 50],
                "access_url": f"https://example.test/{start:%Y%m%d%H}.HDF5",
                "file_size": 4,
            }
        ]

    def download(self, url: str, output_path: Path) -> None:
        del url
        self.download_count += 1
        output_path.write_bytes(b"imrg")


class FailingImergClient(FakeImergClient):
    def download(self, url: str, output_path: Path) -> None:
        del url, output_path
        raise OSError("simulated download failure")


class FakeGranule(dict[str, Any]):
    def data_links(self) -> list[str]:
        return ["https://example.test/imerg.h5"]

    def size(self) -> float:
        return 2.0


class FakeEarthaccessModule:
    def __init__(self) -> None:
        self.search_arguments: dict[str, Any] = {}

    def search_data(self, **kwargs: Any) -> list[FakeGranule]:
        self.search_arguments = kwargs
        return [
            FakeGranule(
                {
                    "meta": {"concept-id": "G123"},
                    "umm": {
                        "GranuleUR": "IMERG.TEST",
                        "TemporalExtent": {
                            "RangeDateTime": {
                                "BeginningDateTime": "2024-01-01T00:00:00Z",
                                "EndingDateTime": "2024-01-01T00:29:59Z",
                            }
                        },
                        "DataGranule": {"ProductionDateTime": "2024-02-01T00:00:00Z"},
                    },
                }
            )
        ]


class FakeFrame:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def to_dict(self, orient: str) -> list[dict[str, Any]]:
        assert orient == "records"
        return self.rows


class FakeWaterdata:
    def get_monitoring_locations(self, **kwargs: Any) -> tuple[FakeFrame, dict[str, Any]]:
        del kwargs
        return (
            FakeFrame(
                [
                    {
                        "monitoring_location_id": "USGS-1",
                        "longitude": -120.1,
                        "latitude": 38.1,
                        "site_type": "Stream",
                    }
                ]
            ),
            {},
        )

    def get_time_series_metadata(self, **kwargs: Any) -> tuple[FakeFrame, dict[str, Any]]:
        del kwargs
        return (
            FakeFrame(
                [
                    {
                        "monitoring_location_id": "USGS-1",
                        "parameter_code": "00060",
                        "begin_utc": "2023-01-01T00:00:00+00:00",
                        "end_utc": "2025-01-01T00:00:00+00:00",
                    }
                ]
            ),
            {},
        )

    def get_continuous(self, **kwargs: Any) -> tuple[FakeFrame, dict[str, Any]]:
        del kwargs
        return (
            FakeFrame(
                [
                    {
                        "time": "2024-01-01T01:00:00+00:00",
                        "value": 5.0,
                        "unit_of_measure": "ft3/s",
                        "qualifier": "P",
                        "approval_status": "provisional",
                    }
                ]
            ),
            {},
        )


class FakeDataretrievalModule:
    waterdata = FakeWaterdata()


class FakeUsgsClient:
    def __init__(self, no_gauges: bool = False) -> None:
        self.no_gauges = no_gauges
        self.fetch_count = 0

    def search_gauges(
        self,
        longitude: float,
        latitude: float,
        radius_km: float,
        start_utc: datetime,
        end_utc: datetime,
        parameter_codes: list[str],
    ) -> list[dict[str, Any]]:
        del longitude, latitude, radius_km, start_utc, end_utc, parameter_codes
        if self.no_gauges:
            return []
        return [
            {
                "monitoring_location_id": "USGS-1",
                "distance_km": 12.0,
                "parameter_codes": ["00060", "00065"],
                "site_type": "stream",
                "backend": "waterdata",
            }
        ]

    def fetch_observations(
        self,
        monitoring_location_id: str,
        parameter_code: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[dict[str, Any]]:
        del monitoring_location_id
        self.fetch_count += 1
        hours = int((end_utc - start_utc).total_seconds() // 3600) + 1
        return [
            {
                "time_utc": (start_utc + timedelta(hours=index)).isoformat(),
                "value": 1.0 if index < 24 else 7.0,
                "units": "ft3/s" if parameter_code == "00060" else "ft",
                "qualifier": "P" if index == 25 else "",
                "approval_status": "provisional" if index == 25 else "approved",
                "parameter_code": parameter_code,
            }
            for index in range(hours)
        ]


def _sample(tmp_path: Path) -> Path:
    rows = [
        {
            "episode_id": f"e{index}",
            "selection_stratum": "balanced_merge_candidate" if index == 1 else "singleton",
            "member_count": index,
            "episode_duration_days": 1,
            "state_codes": "CA",
            "episode_start_date": f"202{index}-01-01",
            "episode_end_date": f"202{index}-01-01",
            "representative_longitude": -120.0,
            "representative_latitude": 38.0,
            "additional_balanced_members": 1 if index == 1 else 0,
        }
        for index in range(2, 6)
    ]
    path = tmp_path / "sample.parquet"
    _write(path, rows)
    return path


def _source_outputs(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    mrms_metrics = tmp_path / "mrms_metrics.parquet"
    imerg_metrics = tmp_path / "imerg_metrics.parquet"
    mrms_series = tmp_path / "mrms_series.parquet"
    imerg_series = tmp_path / "imerg_series.parquet"
    usgs_summary = tmp_path / "usgs_summary.parquet"
    _write(mrms_metrics, [{"episode_id": "e2", "coverage_fraction": 1.0}])
    _write(imerg_metrics, [{"episode_id": "e2", "coverage_fraction": 1.0}])
    _write(
        mrms_series,
        [
            {"episode_id": "e2", "valid_time_utc": f"t{index}", "area_mean_qpe_mm": float(index)}
            for index in range(3)
        ],
    )
    _write(
        imerg_series,
        [
            {
                "episode_id": "e2",
                "valid_time_utc": f"t{index}",
                "area_mean_imerg_mm": float(index) + 0.5,
            }
            for index in range(3)
        ],
    )
    _write(
        usgs_summary,
        [
            {
                "episode_id": "e2",
                "hydrologic_response_supported": True,
                "no_gauge_reason": "",
                "best_usgs_association_quality": "inside_25km_buffer",
            }
        ],
    )
    return mrms_metrics, mrms_series, imerg_metrics, imerg_series, usgs_summary


def _write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
