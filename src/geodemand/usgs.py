from __future__ import annotations

import csv
import hashlib
import importlib
import json
import math
import os
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from geodemand import __version__
from geodemand.schemas import PHYSICAL_EVIDENCE_SCHEMA_VERSION

DISCHARGE = "00060"
GAGE_HEIGHT = "00065"
PARAMETER_NAMES = {DISCHARGE: "discharge", GAGE_HEIGHT: "gage_height"}
DEFAULT_RADIUS_KM = 50.0
FALLBACK_RADIUS_KM = 100.0
MAX_GAUGES_PER_EPISODE = 3
MAX_ATTEMPTS = 3
ZERO_BASELINE_EPSILON = 1e-9
RESPONSE_RATIO_THRESHOLD = 1.5
EPISODE_BUFFER_KM = 25.0
MIN_CADENCE_OBSERVATIONS = 2


class UsgsError(ValueError):
    """Raised when USGS verification processing cannot continue."""


class UsgsClient(Protocol):
    def search_gauges(
        self,
        longitude: float,
        latitude: float,
        radius_km: float,
        start_utc: datetime,
        end_utc: datetime,
        parameter_codes: Sequence[str],
    ) -> list[Mapping[str, Any]]: ...

    def fetch_observations(
        self,
        monitoring_location_id: str,
        parameter_code: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[Mapping[str, Any]]: ...


class DataretrievalUsgsClient:
    """Adapter for modern USGS Water Data operations exposed by dataretrieval."""

    def __init__(self, module: Any | None = None) -> None:
        try:
            package = module or importlib.import_module("dataretrieval")
            self._waterdata = package.waterdata
        except Exception as exc:
            raise UsgsError(
                "USGS support is unavailable. Install the 'usgs' optional dependency group."
            ) from exc

    def search_gauges(
        self,
        longitude: float,
        latitude: float,
        radius_km: float,
        start_utc: datetime,
        end_utc: datetime,
        parameter_codes: Sequence[str],
    ) -> list[Mapping[str, Any]]:
        bbox = _search_bbox(longitude, latitude, radius_km)
        try:
            locations, _ = self._waterdata.get_monitoring_locations(
                bbox=list(bbox), limit=500, convert_type=True
            )
            location_rows = _frame_records(locations)
            identifiers = sorted(
                {
                    str(_first(row, "monitoring_location_id", "monitoringLocationIdentifier"))
                    for row in location_rows
                    if _first(row, "monitoring_location_id", "monitoringLocationIdentifier")
                }
            )
            if not identifiers:
                return []
            metadata, _ = self._waterdata.get_time_series_metadata(
                monitoring_location_id=identifiers,
                parameter_code=list(parameter_codes),
                limit=10_000,
                convert_type=True,
            )
        except Exception as exc:
            raise UsgsError(f"USGS monitoring-location discovery failed: {exc}") from exc
        parameters_by_location: dict[str, set[str]] = {}
        for row in _frame_records(metadata):
            identifier = str(
                _first(row, "monitoring_location_id", "monitoringLocationIdentifier") or ""
            )
            parameter = str(_first(row, "parameter_code", "parameterCode") or "")
            if identifier and parameter and _metadata_spans(row, start_utc, end_utc):
                parameters_by_location.setdefault(identifier, set()).add(parameter)
        candidates: list[Mapping[str, Any]] = []
        for row in location_rows:
            identifier = str(
                _first(row, "monitoring_location_id", "monitoringLocationIdentifier") or ""
            )
            parameters = sorted(parameters_by_location.get(identifier, set()))
            if not parameters:
                continue
            gauge_lon, gauge_lat = _location_coordinates(row)
            if gauge_lon is None or gauge_lat is None:
                continue
            distance = _haversine_km(longitude, latitude, gauge_lon, gauge_lat)
            if distance > radius_km:
                continue
            candidates.append(
                {
                    "monitoring_location_id": identifier,
                    "distance_km": distance,
                    "longitude": gauge_lon,
                    "latitude": gauge_lat,
                    "parameter_codes": parameters,
                    "site_type": str(_first(row, "site_type", "site_type_code") or ""),
                    "backend": "waterdata",
                }
            )
        return candidates

    def fetch_observations(
        self,
        monitoring_location_id: str,
        parameter_code: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[Mapping[str, Any]]:
        try:
            frame, _ = self._waterdata.get_continuous(
                monitoring_location_id=monitoring_location_id,
                parameter_code=parameter_code,
                time=f"{start_utc.isoformat()}/{end_utc.isoformat()}",
                convert_type=True,
            )
        except Exception as exc:
            raise UsgsError(f"USGS continuous-value request failed: {exc}") from exc
        return [_normalized_observation(row, parameter_code) for row in _frame_records(frame)]


def selfcheck_usgs(working_root: Path) -> dict[str, Any]:
    working_root.mkdir(parents=True, exist_ok=True)
    return {
        "package_version": __version__,
        "dataretrieval_import": _check_import("dataretrieval"),
        "temporary_file_write": _check_temp_write(working_root),
        "http_connectivity": {"ok": False, "reason": "network check is opt-in"},
        "monitoring_location_search": {"ok": False, "reason": "network check is opt-in"},
        "continuous_value_endpoint": {"ok": False, "reason": "network check is opt-in"},
        "parameter_metadata_lookup": {"ok": False, "reason": "network check is opt-in"},
        "api_key_present": bool(os.getenv("USGS_API_KEY")),
        "small_public_response": {"ok": False, "reason": "network check is opt-in"},
    }


def discover_usgs(
    sample_path: Path,
    output_root: Path,
    client: UsgsClient | None = None,
    max_episodes: int | None = None,
    max_gauges: int = MAX_GAUGES_PER_EPISODE,
) -> dict[str, Path]:
    sample_rows = _read_rows(sample_path)
    provenance = _provenance(
        source_dataset="USGS Water Data",
        source_product="monitoring location metadata",
        source_manifest_hash=_sha256(sample_path),
        decoder_version="dataretrieval-site-adapter-v1",
        rule_version="usgs-discovery-v1",
    )
    if max_episodes is not None:
        sample_rows = sample_rows[:max_episodes]
    candidate_rows: list[dict[str, Any]] = []
    association_rows: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    for row in sample_rows:
        start, end = _window(row)
        gauges = _search(client, row, start, end, DEFAULT_RADIUS_KM)
        radius = DEFAULT_RADIUS_KM
        if not gauges:
            gauges = _search(client, row, start, end, FALLBACK_RADIUS_KM)
            radius = FALLBACK_RADIUS_KM
        ranked = _rank_gauges(gauges, radius)[:max_gauges]
        if not ranked:
            association_rows.append({**_no_gauge_row(row), **provenance})
            continue
        for gauge in ranked:
            candidate_rows.append(_candidate_row(row, gauge))
            association = _association_row(row, gauge)
            association_rows.append({**association, **provenance})
            for parameter in _gauge_parameters(gauge):
                request_rows.append(
                    {
                        "episode_id": row["episode_id"],
                        "monitoring_location_id": gauge["monitoring_location_id"],
                        "parameter_code": parameter,
                        "request_start_utc": start.isoformat(),
                        "request_end_utc": end.isoformat(),
                        "backend": str(gauge.get("backend", "waterdata")),
                        "association_quality": association["association_quality"],
                    }
                )
    output_dir = output_root / "inventory"
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = output_dir / "usgs_candidate_gauges.parquet"
    association_path = output_dir / "usgs_episode_gauge_associations.parquet"
    request_path = output_dir / "usgs_request_plan.csv"
    summary_path = output_dir / "usgs_inventory_summary.json"
    _write_parquet(candidate_path, candidate_rows)
    _write_parquet(association_path, association_rows)
    _write_csv(request_path, request_rows, _request_fields())
    _write_json(
        summary_path,
        {
            "episode_count": len(sample_rows),
            "candidate_gauge_count": len(candidate_rows),
            "request_count": len(request_rows),
            "association_counts": dict(
                sorted(Counter(row["association_quality"] for row in association_rows).items())
            ),
        },
    )
    return {
        "usgs_candidate_gauges": candidate_path,
        "usgs_episode_gauge_associations": association_path,
        "usgs_request_plan": request_path,
        "usgs_inventory_summary": summary_path,
    }


def fetch_usgs(
    request_plan: Path,
    output_root: Path,
    client: UsgsClient | None = None,
    max_episodes: int | None = None,
) -> dict[str, Path]:
    rows = _read_csv(request_plan)
    provenance = _provenance(
        source_dataset="USGS Water Data",
        source_product="continuous values",
        source_manifest_hash=_sha256(request_plan),
        decoder_version="dataretrieval-continuous-values-v1",
        rule_version="usgs-fetch-v1",
    )
    if max_episodes is not None:
        allowed = sorted({row["episode_id"] for row in rows})[:max_episodes]
        rows = [row for row in rows if row["episode_id"] in allowed]
    observations: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    cache_dir = output_root / "cache"
    request_cache_dir = cache_dir / "requests"
    request_cache_dir.mkdir(parents=True, exist_ok=True)
    for row in rows:
        key = (
            row["monitoring_location_id"],
            row["parameter_code"],
            row["request_start_utc"],
            row["request_end_utc"],
        )
        if key in seen:
            continue
        seen.add(key)
        start = datetime.fromisoformat(row["request_start_utc"])
        end = datetime.fromisoformat(row["request_end_utc"])
        cache_path = request_cache_dir / f"{_request_hash(key)}.json"
        cache_hit = cache_path.exists()
        if cache_hit:
            result = cast(list[Mapping[str, Any]], json.loads(cache_path.read_text("utf-8")))
        else:
            if client is None:
                failures.append({**row, "failure_reason": "client_unavailable"})
                continue
            try:
                result = _fetch_with_retry(
                    client, row["monitoring_location_id"], row["parameter_code"], start, end
                )
            except Exception as exc:
                failures.append(
                    {
                        **row,
                        "failure_reason": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            _write_request_cache(cache_path, result)
        for item in result:
            observations.append({**row, **dict(item), **provenance})
        manifest_rows.append(
            {
                **row,
                "cache_key": cache_path.stem,
                "cache_hit": cache_hit,
                "observation_count": len(result),
                "api_key_present": bool(os.getenv("USGS_API_KEY")),
            }
        )
    observation_path = cache_dir / "usgs_observations.parquet"
    manifest_path = cache_dir / "usgs_file_or_request_manifest.parquet"
    failure_path = cache_dir / "usgs_download_failures.csv"
    summary_path = cache_dir / "usgs_fetch_summary.json"
    _write_parquet(observation_path, observations)
    _write_parquet(manifest_path, manifest_rows)
    _write_csv(failure_path, failures, list(failures[0]) if failures else ["failure_reason"])
    _write_json(
        summary_path, {"observation_count": len(observations), "failure_count": len(failures)}
    )
    return {
        "usgs_observations": observation_path,
        "usgs_file_or_request_manifest": manifest_path,
        "usgs_download_failures": failure_path,
        "usgs_fetch_summary": summary_path,
    }


def estimate_usgs_fetch(request_plan: Path, max_episodes: int | None = None) -> dict[str, int]:
    rows = _read_csv(request_plan)
    if max_episodes is not None:
        allowed = set(sorted({row["episode_id"] for row in rows})[:max_episodes])
        rows = [row for row in rows if row["episode_id"] in allowed]
    requests = {
        (
            row["monitoring_location_id"],
            row["parameter_code"],
            row["request_start_utc"],
            row["request_end_utc"],
        )
        for row in rows
    }
    return {
        "planned_episodes": len({row["episode_id"] for row in rows}),
        "planned_requests": len(requests),
    }


def extract_usgs(
    observations_path: Path,
    associations_path: Path,
    output_root: Path,
    rules_path: Path,
) -> dict[str, Path]:
    rules = _load_response_rules(rules_path)
    observations = _read_rows(observations_path)
    associations = _read_rows(associations_path)
    assoc_by_key = {(row["episode_id"], row["monitoring_location_id"]): row for row in associations}
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in observations:
        grouped.setdefault(
            (row["episode_id"], row["monitoring_location_id"], row["parameter_code"]),
            [],
        ).append(row)
    metrics = [
        _metric_row(key, values, assoc_by_key.get((key[0], key[1]), {}), rules)
        for key, values in sorted(grouped.items())
    ]
    provenance = _provenance(
        source_dataset="USGS Water Data",
        source_product="continuous values",
        source_manifest_hash=_sha256(observations_path),
        decoder_version="dataretrieval-continuous-values-v1",
        rule_version=str(rules["rule_version"]),
    )
    metrics = [{**row, **provenance} for row in metrics]
    summary = [{**row, **provenance} for row in _episode_summary(metrics, associations)]
    metrics_dir = output_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = metrics_dir / "usgs_gauge_response_metrics.parquet"
    summary_path = metrics_dir / "usgs_episode_response_summary.parquet"
    _write_parquet(metrics_path, metrics)
    _write_parquet(summary_path, summary)
    return {
        "usgs_gauge_response_metrics": metrics_path,
        "usgs_episode_response_summary": summary_path,
    }


def response_metrics(values: Sequence[float], pre_count: int = 3) -> dict[str, Any]:
    if not values:
        return {"response_detected": False, "response_assessment_reason": "missing_observations"}
    pre = list(values[:pre_count]) or [values[0]]
    event = list(values[pre_count:]) or list(values)
    pre_median = sorted(pre)[len(pre) // 2]
    event_max = max(event)
    absolute_rise = event_max - pre_median
    relative_rise = None if abs(pre_median) < ZERO_BASELINE_EPSILON else event_max / pre_median
    rates = [values[index + 1] - values[index] for index in range(len(values) - 1)]
    max_rate = max(rates) if rates else 0.0
    detected = absolute_rise > 0 and (
        relative_rise is None or relative_rise >= RESPONSE_RATIO_THRESHOLD or max_rate > 0
    )
    return {
        "pre_event_median": pre_median,
        "event_maximum": event_max,
        "absolute_rise": absolute_rise,
        "relative_rise": relative_rise,
        "maximum_positive_rate_of_change": max_rate,
        "response_detected": detected,
        "response_assessment_reason": "rise_detected" if detected else "no_clear_rise",
    }


def _search(
    client: UsgsClient | None,
    row: Mapping[str, Any],
    start: datetime,
    end: datetime,
    radius: float,
) -> list[Mapping[str, Any]]:
    if client is None:
        return []
    return client.search_gauges(
        float(row["representative_longitude"]),
        float(row["representative_latitude"]),
        radius,
        start,
        end,
        [DISCHARGE, GAGE_HEIGHT],
    )


def _rank_gauges(gauges: list[Mapping[str, Any]], radius: float) -> list[Mapping[str, Any]]:
    return sorted(
        gauges,
        key=lambda gauge: (
            float(gauge.get("distance_km", radius)),
            0 if DISCHARGE in _gauge_parameters(gauge) else 1,
            str(gauge.get("monitoring_location_id", "")),
        ),
    )


def _candidate_row(episode: Mapping[str, Any], gauge: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "episode_id": episode["episode_id"],
        "monitoring_location_id": gauge["monitoring_location_id"],
        "distance_to_episode_km": float(gauge.get("distance_km", 0.0)),
        "parameter_codes": ";".join(_gauge_parameters(gauge)),
        "site_type": str(gauge.get("site_type", "")),
        "backend": str(gauge.get("backend", "waterdata")),
    }


def _association_row(episode: Mapping[str, Any], gauge: Mapping[str, Any]) -> dict[str, Any]:
    distance = float(gauge.get("distance_km", 999.0))
    if bool(gauge.get("inside_geometry", False)):
        quality = "inside_episode_geometry"
    elif distance <= EPISODE_BUFFER_KM:
        quality = "inside_25km_buffer"
    elif distance <= DEFAULT_RADIUS_KM:
        quality = "nearby_within_50km"
    elif distance <= FALLBACK_RADIUS_KM:
        quality = "fallback_within_100km"
    else:
        quality = "uncertain_association"
    return {
        "episode_id": episode["episode_id"],
        "monitoring_location_id": gauge["monitoring_location_id"],
        "association_quality": quality,
        "distance_to_episode_km": distance,
    }


def _no_gauge_row(episode: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "episode_id": episode["episode_id"],
        "monitoring_location_id": "",
        "association_quality": "no_suitable_gauge",
        "distance_to_episode_km": None,
    }


def _window(row: Mapping[str, Any]) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(str(row["episode_start_date"])).replace(tzinfo=UTC)
    end = datetime.fromisoformat(str(row["episode_end_date"])).replace(tzinfo=UTC)
    return start - timedelta(hours=30), end + timedelta(hours=54)


def _fetch_with_retry(
    client: UsgsClient,
    monitoring_location_id: str,
    parameter_code: str,
    start: datetime,
    end: datetime,
) -> list[Mapping[str, Any]]:
    for attempt in range(MAX_ATTEMPTS):
        try:
            return client.fetch_observations(monitoring_location_id, parameter_code, start, end)
        except Exception:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            time.sleep(0.25 * 2**attempt)
    return []


def _request_hash(key: tuple[str, str, str, str]) -> str:
    return hashlib.sha256("\x1f".join(key).encode()).hexdigest()


def _write_request_cache(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    payload = json.dumps([dict(row) for row in rows], indent=2, sort_keys=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(path)


def _metric_row(
    key: tuple[str, str, str],
    rows: list[dict[str, Any]],
    association: Mapping[str, Any],
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: str(row.get("time_utc", "")))
    observations = [
        (_optional_datetime(row.get("time_utc")), float(row["value"]))
        for row in ordered
        if row.get("value") is not None and _optional_datetime(row.get("time_utc")) is not None
    ]
    request_start = _optional_datetime(ordered[0].get("request_start_utc")) if ordered else None
    request_end = _optional_datetime(ordered[0].get("request_end_utc")) if ordered else None
    if request_start is None or request_end is None:
        raise UsgsError(f"USGS observations are missing request bounds for {key}")
    event_start = request_start + timedelta(hours=24)
    event_end = request_end - timedelta(hours=48)
    stats = _windowed_response_metrics(observations, event_start, event_end, key[2], rules)
    qualified = sum(1 for row in ordered if row.get("qualifier"))
    provisional = sum(
        1 for row in ordered if str(row.get("approval_status", "")).lower() == "provisional"
    )
    cadence = _cadence_assessment(observations, request_start, request_end, rules)
    coverage_rules = cast(Mapping[str, Any], rules["coverage"])
    pre_count = sum(1 for time, _ in observations if time is not None and time < event_start)
    event_count = sum(
        1 for time, _ in observations if time is not None and event_start <= time <= event_end
    )
    coverage_fraction = cadence["coverage_fraction"]
    adequate_coverage = bool(
        cadence["cadence_status"] == "regular"
        and coverage_fraction is not None
        and float(coverage_fraction) >= float(coverage_rules["minimum_fraction"])
        and pre_count >= int(coverage_rules["minimum_pre_event_observations"])
        and event_count >= int(coverage_rules["minimum_event_observations"])
    )
    allowed_associations = set(
        cast(Mapping[str, Any], rules["association"])["allowed_quality_categories"]
    )
    association_allowed = association.get("association_quality") in allowed_associations
    quality = _quality_assessment(ordered, rules)
    response_detected = bool(
        stats["threshold_response_detected"]
        and adequate_coverage
        and association_allowed
        and quality["quality_acceptable"]
    )
    response_score = _response_score(stats, key[2], rules) if response_detected else 0.0
    return {
        "episode_id": key[0],
        "monitoring_location_id": key[1],
        "parameter_code": key[2],
        "parameter_name": PARAMETER_NAMES.get(key[2], key[2]),
        "units": ordered[0].get("units", "") if ordered else "",
        "association_quality": association.get("association_quality", ""),
        "distance_to_episode_km": association.get("distance_to_episode_km"),
        "observation_start_utc": ordered[0].get("time_utc", "") if ordered else "",
        "observation_end_utc": ordered[-1].get("time_utc", "") if ordered else "",
        "expected_observation_count": cadence["expected_observation_count"],
        "available_observation_count": len(observations),
        "coverage_fraction": coverage_fraction,
        "cadence_seconds": cadence["cadence_seconds"],
        "cadence_status": cadence["cadence_status"],
        "pre_event_observation_count": pre_count,
        "event_observation_count": event_count,
        "adequate_temporal_coverage": adequate_coverage,
        "lag_from_mrms_peak_hours": None,
        "lag_from_imerg_peak_hours": None,
        "qualified_observation_fraction": qualified / len(ordered) if ordered else None,
        "provisional_observation_fraction": provisional / len(ordered) if ordered else None,
        "qualifiers": ";".join(
            sorted({str(row.get("qualifier")) for row in ordered if row.get("qualifier")})
        ),
        "approval_statuses": ";".join(
            sorted(
                {str(row.get("approval_status")) for row in ordered if row.get("approval_status")}
            )
        ),
        "backend": ordered[0].get("backend", "") if ordered else "",
        "association_allowed": association_allowed,
        "quality_acceptable": quality["quality_acceptable"],
        "quality_reasons": ";".join(quality["quality_reasons"]),
        **stats,
        "response_signal_strength": stats.get("absolute_rise"),
        "response_score": response_score,
        "response_detected": response_detected,
        "response_assessment_reason": (
            "response_threshold_met"
            if response_detected
            else "inadequate_temporal_coverage"
            if not adequate_coverage
            else "weak_geographic_association"
            if not association_allowed
            else "quality_rules_failed"
            if not quality["quality_acceptable"]
            else str(stats["response_assessment_reason"])
        ),
    }


def _windowed_response_metrics(
    observations: list[tuple[datetime | None, float]],
    event_start: datetime,
    event_end: datetime,
    parameter_code: str,
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    valid = [(time, value) for time, value in observations if time is not None]
    pre = [(time, value) for time, value in valid if time < event_start]
    event = [(time, value) for time, value in valid if event_start <= time <= event_end]
    post = [(time, value) for time, value in valid if time > event_end]
    if not event:
        return {
            "pre_event_median": _median_values([value for _, value in pre]),
            "pre_event_minimum": min((value for _, value in pre), default=None),
            "event_maximum": None,
            "post_event_maximum": max((value for _, value in post), default=None),
            "absolute_rise": None,
            "relative_rise": None,
            "maximum_positive_rate_of_change": None,
            "time_of_maximum_utc": "",
            "time_of_maximum_rise_utc": "",
            "threshold_response_detected": False,
            "response_assessment_reason": "missing_event_window_observations",
        }
    baseline = _median_values([value for _, value in pre])
    event_time, event_max = max(event, key=lambda item: item[1])
    absolute_rise = None if baseline is None else event_max - baseline
    epsilon = float(cast(Mapping[str, Any], rules["response"])["zero_baseline_epsilon"])
    relative_rise = None if baseline is None or abs(baseline) < epsilon else event_max / baseline
    rates = []
    for (left_time, left_value), (right_time, right_value) in zip(valid, valid[1:], strict=False):
        elapsed_hours = (right_time - left_time).total_seconds() / 3600
        if elapsed_hours > 0:
            rates.append(((right_value - left_value) / elapsed_hours, right_time))
    maximum_rate, maximum_rate_time = max(rates, default=(None, None), key=lambda item: item[0])
    parameter_name = PARAMETER_NAMES.get(parameter_code, parameter_code)
    parameter_rules = cast(
        Mapping[str, Any], cast(Mapping[str, Any], rules["response"])[parameter_name]
    )
    minimum_absolute = float(parameter_rules["minimum_absolute_rise"])
    minimum_relative = float(parameter_rules["minimum_relative_ratio"])
    detected = bool(
        absolute_rise is not None
        and absolute_rise >= minimum_absolute
        and (relative_rise is None or relative_rise >= minimum_relative)
    )
    return {
        "pre_event_median": baseline,
        "pre_event_minimum": min((value for _, value in pre), default=None),
        "event_maximum": event_max,
        "post_event_maximum": max((value for _, value in post), default=None),
        "absolute_rise": absolute_rise,
        "relative_rise": relative_rise,
        "maximum_positive_rate_of_change": maximum_rate,
        "time_of_maximum_utc": event_time.isoformat(),
        "time_of_maximum_rise_utc": maximum_rate_time.isoformat() if maximum_rate_time else "",
        "threshold_response_detected": detected,
        "response_assessment_reason": "rise_detected" if detected else "no_clear_rise",
    }


def _cadence_assessment(
    observations: list[tuple[datetime | None, float]],
    request_start: datetime,
    request_end: datetime,
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    times = sorted(time for time, _ in observations if time is not None)
    coverage_rules = cast(Mapping[str, Any], rules["coverage"])
    minimum = int(coverage_rules["minimum_cadence_observations"])
    if len(times) < minimum:
        return {
            "expected_observation_count": None,
            "coverage_fraction": None,
            "cadence_seconds": None,
            "cadence_status": "insufficient_for_cadence",
        }
    intervals = sorted(
        (right - left).total_seconds()
        for left, right in zip(times, times[1:], strict=False)
        if right > left
    )
    if not intervals:
        return {
            "expected_observation_count": None,
            "coverage_fraction": None,
            "cadence_seconds": None,
            "cadence_status": "duplicate_or_unordered_times",
        }
    cadence = intervals[len(intervals) // 2]
    maximum_deviation = float(coverage_rules["maximum_interval_deviation_fraction"])
    irregular = any(abs(interval - cadence) / cadence > maximum_deviation for interval in intervals)
    if irregular:
        return {
            "expected_observation_count": None,
            "coverage_fraction": None,
            "cadence_seconds": cadence,
            "cadence_status": "irregular",
        }
    expected = int((request_end - request_start).total_seconds() // cadence) + 1
    return {
        "expected_observation_count": expected,
        "coverage_fraction": min(len(times) / expected, 1.0),
        "cadence_seconds": cadence,
        "cadence_status": "regular",
    }


def _quality_assessment(
    observations: Sequence[Mapping[str, Any]], rules: Mapping[str, Any]
) -> dict[str, Any]:
    quality_rules = cast(Mapping[str, Any], rules["quality"])
    excluded = {str(value).casefold() for value in quality_rules["excluded_qualifiers"]}
    statuses = {str(value).casefold() for value in quality_rules["allowed_approval_statuses"]}
    reasons: list[str] = []
    if any(str(row.get("qualifier", "")).casefold() in excluded for row in observations):
        reasons.append("excluded_qualifier")
    if any(str(row.get("approval_status", "")).casefold() not in statuses for row in observations):
        reasons.append("unsupported_approval_status")
    provisional = sum(
        str(row.get("approval_status", "")).casefold() == "provisional" for row in observations
    )
    provisional_fraction = provisional / len(observations) if observations else 0.0
    if provisional and not bool(quality_rules["allow_provisional_observations"]):
        reasons.append("provisional_disallowed")
    if provisional_fraction > float(quality_rules["maximum_provisional_fraction"]):
        reasons.append("excess_provisional_fraction")
    return {"quality_acceptable": not reasons, "quality_reasons": sorted(reasons)}


def _response_score(
    stats: Mapping[str, Any], parameter_code: str, rules: Mapping[str, Any]
) -> float:
    parameter_name = PARAMETER_NAMES.get(parameter_code, parameter_code)
    parameter_rules = cast(
        Mapping[str, Any], cast(Mapping[str, Any], rules["response"])[parameter_name]
    )
    absolute = stats.get("absolute_rise")
    relative = stats.get("relative_rise")
    absolute_component = (
        0.0
        if absolute is None
        else float(absolute) / float(parameter_rules["minimum_absolute_rise"])
    )
    relative_component = (
        1.0
        if relative is None
        else float(relative) / float(parameter_rules["minimum_relative_ratio"])
    )
    return round(absolute_component + relative_component, 12)


def _median_values(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _episode_summary(
    metrics: list[dict[str, Any]],
    associations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    episodes = sorted({row["episode_id"] for row in associations})
    rows = []
    for episode_id in episodes:
        episode_metrics = [row for row in metrics if row["episode_id"] == episode_id]
        episode_assoc = [row for row in associations if row["episode_id"] == episode_id]
        no_gauge = all(row["association_quality"] == "no_suitable_gauge" for row in episode_assoc)
        detected = [row for row in episode_metrics if row["response_detected"]]
        adequate = [row for row in episode_metrics if row["adequate_temporal_coverage"]]
        strongest = max(
            detected,
            default=None,
            key=lambda row: (
                float(row.get("response_score") or 0.0),
                str(row["monitoring_location_id"]),
                str(row["parameter_code"]),
            ),
        )
        numeric_rises = [
            float(row["absolute_rise"])
            for row in episode_metrics
            if row.get("absolute_rise") is not None
        ]
        rows.append(
            {
                "episode_id": episode_id,
                "candidate_gauge_count": len(
                    [row for row in episode_assoc if row["monitoring_location_id"]]
                ),
                "selected_gauge_count": len(
                    {row["monitoring_location_id"] for row in episode_metrics}
                ),
                "gauges_with_adequate_coverage": len(
                    {row["monitoring_location_id"] for row in adequate}
                ),
                "gauges_with_detected_response": len(
                    {row["monitoring_location_id"] for row in detected}
                ),
                "strongest_response_gauge": (
                    strongest["monitoring_location_id"] if strongest else ""
                ),
                "strongest_response_parameter": strongest["parameter_code"] if strongest else "",
                "strongest_response_score": strongest["response_score"] if strongest else None,
                "shortest_precipitation_to_response_lag": None,
                "median_precipitation_to_response_lag": None,
                "nearby_gauge_response_detected": "unknown" if no_gauge else bool(detected),
                "hydrologic_response_supported": "unknown" if no_gauge else bool(detected),
                "interpretation_scope": "nearby_monitoring_location_not_connectivity_proof",
                "support_strength": max(numeric_rises, default=None),
                "best_usgs_association_quality": (
                    strongest.get("association_quality", "") if strongest else ""
                ),
                "no_gauge_reason": "no_suitable_gauge" if no_gauge else "",
                "manual_review_flag": no_gauge,
            }
        )
    return rows


def _gauge_parameters(gauge: Mapping[str, Any]) -> list[str]:
    value = gauge.get("parameter_codes", [])
    if isinstance(value, str):
        return [item for item in value.split(";") if item]
    return [str(item) for item in value]


def _load_response_rules(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    required = {"rule_version", "coverage", "association", "response", "quality"}
    if not isinstance(payload, dict) or not required.issubset(payload):
        missing = sorted(required - set(payload if isinstance(payload, dict) else {}))
        raise UsgsError("USGS response rules are missing sections: " + ", ".join(missing))
    response = cast(Mapping[str, Any], payload["response"])
    for parameter in ("discharge", "gage_height"):
        if parameter not in response:
            raise UsgsError(f"USGS response rules are missing {parameter} thresholds.")
    return cast(dict[str, Any], payload)


def _provenance(
    source_dataset: str,
    source_product: str,
    source_manifest_hash: str,
    decoder_version: str,
    rule_version: str,
) -> dict[str, str]:
    return {
        "schema_version": PHYSICAL_EVIDENCE_SCHEMA_VERSION,
        "data_origin": "observed",
        "source_dataset": source_dataset,
        "source_product": source_product,
        "source_manifest_hash": source_manifest_hash,
        "decoder_version": decoder_version,
        "code_commit": os.getenv("GEODEMAND_CODE_COMMIT", "unknown-unpackaged"),
        "rule_version": rule_version,
    }


def _frame_records(frame: Any) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], frame.to_dict(orient="records"))


def _first(row: Mapping[str, Any], *fields: str) -> Any:
    for field in fields:
        value = row.get(field)
        if value is not None and str(value) != "nan":
            return value
    return None


def _search_bbox(
    longitude: float, latitude: float, radius_km: float
) -> tuple[float, float, float, float]:
    latitude_delta = radius_km / 111.0
    longitude_scale = max(math.cos(math.radians(latitude)), 0.01)
    longitude_delta = radius_km / (111.0 * longitude_scale)
    return (
        longitude - longitude_delta,
        latitude - latitude_delta,
        longitude + longitude_delta,
        latitude + latitude_delta,
    )


def _location_coordinates(row: Mapping[str, Any]) -> tuple[float | None, float | None]:
    geometry = row.get("geometry")
    if geometry is not None and hasattr(geometry, "x") and hasattr(geometry, "y"):
        return float(geometry.x), float(geometry.y)
    longitude = _first(row, "longitude", "decimal_longitude", "longitude_measure")
    latitude = _first(row, "latitude", "decimal_latitude", "latitude_measure")
    if longitude is None or latitude is None:
        return None, None
    return float(longitude), float(latitude)


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    value = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _metadata_spans(row: Mapping[str, Any], start_utc: datetime, end_utc: datetime) -> bool:
    begin = _optional_datetime(_first(row, "begin_utc", "begin"))
    end = _optional_datetime(_first(row, "end_utc", "end"))
    return (begin is None or begin <= start_utc) and (end is None or end >= end_utc)


def _optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    parsed = (
        value.to_pydatetime()
        if hasattr(value, "to_pydatetime")
        else datetime.fromisoformat(str(value))
    )
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalized_observation(row: Mapping[str, Any], parameter_code: str) -> dict[str, Any]:
    observed_time = _first(row, "time", "time_utc", "dateTime")
    parsed_time = _optional_datetime(observed_time)
    return {
        "time_utc": parsed_time.isoformat() if parsed_time else "",
        "original_time": str(observed_time or ""),
        "value": _first(row, "value", "result", "measurement_value"),
        "units": str(_first(row, "unit_of_measure", "units") or ""),
        "qualifier": str(_first(row, "qualifier", "qualifiers") or ""),
        "approval_status": str(_first(row, "approval_status", "approvalStatus") or ""),
        "statistic_code": str(_first(row, "statistic_id", "statistic_code") or ""),
        "time_series_identifier": str(_first(row, "time_series_id", "timeSeriesIdentifier") or ""),
        "parameter_code": parameter_code,
    }


def _request_fields() -> list[str]:
    return [
        "episode_id",
        "monitoring_location_id",
        "parameter_code",
        "request_start_utc",
        "request_end_utc",
        "backend",
        "association_quality",
    ]


def _check_import(module: str) -> dict[str, Any]:
    try:
        imported = importlib.import_module(module)
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}
    return {"ok": True, "version": str(getattr(imported, "__version__", "unknown"))}


def _check_temp_write(root: Path) -> dict[str, Any]:
    try:
        path = root / ".write-test"
        path.write_text("ok", encoding="utf-8")
        path.unlink()
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def _read_rows(path: Path) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
