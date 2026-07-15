from __future__ import annotations

import csv
import json
import logging
import math
import time
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import Transformer
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform
from shapely.strtree import STRtree

from geodemand import __version__

LOGGER = logging.getLogger(__name__)

DEFAULT_START_DATE = date(2022, 1, 1)
DEFAULT_END_DATE = date(2025, 12, 31)
DEVELOPMENT_END_DATE = date(2023, 12, 31)
VALIDATION_END_DATE = date(2024, 12, 31)
CONUS_STATE_CODES = frozenset(
    {
        "AL",
        "AR",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DC",
        "DE",
        "FL",
        "GA",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "KY",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    }
)
SECONDARY_DOMAIN_BY_STATE = {
    "AK": "alaska",
    "HI": "hawaii",
    "PR": "puerto_rico",
}
OTHER_TERRITORY_CODES = frozenset({"AS", "GU", "MP", "VI", "UM"})
DOMAIN_VALUES = frozenset(
    {
        "conus",
        "alaska",
        "hawaii",
        "puerto_rico",
        "other_us_territory",
        "multi_domain",
        "unclassified",
    }
)
PRIMARY_EXCLUSION_REASONS = (
    "missing_start_date",
    "malformed_start_date",
    "missing_state_assignment",
    "invalid_duration",
    "outside_temporal_window",
    "outside_primary_domain",
    "other",
)
AREA_CRS = "EPSG:6933"
AREA_TRANSFORMER = Transformer.from_crs("EPSG:4326", AREA_CRS, always_xy=True)
DISTANCE_THRESHOLDS_KM = (5.0, 10.0, 25.0)
IOU_THRESHOLDS = (0.05, 0.10, 0.25)
DURATION_GT_1_DAY = 1
DURATION_GT_3_DAYS = 3
DURATION_GT_7_DAYS = 7
DURATION_GT_30_DAYS = 30
DATE_GAP_LE_1_DAY = 1
DATE_GAP_LE_2_DAYS = 2


class CohortError(ValueError):
    """Raised when cohort inputs cannot be processed."""


@dataclass(frozen=True)
class CohortArtifacts:
    event_cohort: Path
    eligible_event_records: Path
    secondary_domain_records: Path
    excluded_event_records: Path
    event_state_records: Path
    cohort_summary: Path
    exclusion_reasons: Path
    state_year_counts: Path
    temporal_quality: Path
    overlap_diagnostics: Path
    manifest: Path


@dataclass
class CohortAccumulator:
    processed_rows: int = 0
    source_us_intersecting_rows: int = 0
    eligible_rows: int = 0
    secondary_domain_rows: int = 0
    excluded_rows: int = 0
    temporal_quality_source: Counter[str] = field(default_factory=Counter)
    temporal_quality_eligible: Counter[str] = field(default_factory=Counter)
    records_by_year_month_source: Counter[tuple[int, int]] = field(default_factory=Counter)
    records_by_year_month_eligible: Counter[tuple[int, int]] = field(default_factory=Counter)
    durations_source: list[int] = field(default_factory=list)
    durations_eligible: list[int] = field(default_factory=list)
    split_counts: Counter[str] = field(default_factory=Counter)
    state_year_counts: Counter[tuple[str, int, str, str, str, str]] = field(default_factory=Counter)
    exclusion_primary_counts: Counter[str] = field(default_factory=Counter)
    exclusion_secondary_counts: Counter[str] = field(default_factory=Counter)
    domain_counts: Counter[str] = field(default_factory=Counter)

    @property
    def balanced(self) -> bool:
        return self.source_us_intersecting_rows == (
            self.eligible_rows + self.secondary_domain_rows + self.excluded_rows
        )


def inspect_cohort_inputs(events_path: Path, state_overlaps_path: Path) -> dict[str, Any]:
    events_file = _first_parquet_file(events_path)
    states_file = _first_parquet_file(state_overlaps_path)
    return {
        "events": _schema_payload(events_file),
        "state_overlaps": _schema_payload(states_file),
        "domain_values": sorted(DOMAIN_VALUES),
        "default_window": {
            "start_date": DEFAULT_START_DATE.isoformat(),
            "end_date": DEFAULT_END_DATE.isoformat(),
        },
    }


def build_cohort(
    events_path: Path,
    state_overlaps_path: Path,
    output_dir: Path,
    start_date: date = DEFAULT_START_DATE,
    end_date: date = DEFAULT_END_DATE,
    primary_domain: str = "conus",
    batch_size: int = 10_000,
    max_rows: int | None = None,
) -> dict[str, Path]:
    if primary_domain not in DOMAIN_VALUES:
        raise CohortError(f"Unsupported primary domain: {primary_domain}")
    if end_date < start_date:
        raise CohortError("Cohort end date must be on or after start date.")

    started_at = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = CohortArtifacts(
        event_cohort=output_dir / "event_cohort",
        eligible_event_records=output_dir / "eligible_event_records",
        secondary_domain_records=output_dir / "secondary_domain_records",
        excluded_event_records=output_dir / "excluded_event_records",
        event_state_records=output_dir / "event_state_records",
        cohort_summary=output_dir / "cohort_summary.json",
        exclusion_reasons=output_dir / "exclusion_reasons.csv",
        state_year_counts=output_dir / "state_year_counts.csv",
        temporal_quality=output_dir / "temporal_quality.csv",
        overlap_diagnostics=output_dir / "overlap_diagnostics.csv",
        manifest=output_dir / "manifest.json",
    )
    geo_metadata = _geo_metadata(events_path)
    events, accumulator = _classify_events(
        events_path,
        state_overlaps_path,
        start_date,
        end_date,
        primary_domain,
        batch_size,
        max_rows,
    )
    state_rows = _state_records_for_events(state_overlaps_path, events)
    _write_event_outputs(artifacts, events, geo_metadata)
    _write_state_records(artifacts.event_state_records / "part-00000.parquet", state_rows)
    _write_json(artifacts.cohort_summary, _summary_payload(accumulator))
    _write_exclusion_reasons(artifacts.exclusion_reasons, accumulator)
    _write_state_year_counts(artifacts.state_year_counts, accumulator)
    _write_temporal_quality(artifacts.temporal_quality, accumulator)
    diagnostics = _overlap_diagnostics(events, state_rows)
    _write_csv(
        artifacts.overlap_diagnostics,
        diagnostics,
        [
            "year",
            "state_code",
            "temporal_rule",
            "spatial_rule",
            "candidate_pair_count",
            "participating_record_count",
        ],
    )
    processing_seconds = time.perf_counter() - started_at
    _write_json(
        artifacts.manifest,
        {
            "run_timestamp_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "package_version": __version__,
            "events_input": str(events_path),
            "state_overlaps_input": str(state_overlaps_path),
            "output_dir": str(output_dir),
            "configuration": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "primary_domain": primary_domain,
                "batch_size": batch_size,
                "max_rows": max_rows,
                "area_crs": AREA_CRS,
            },
            "row_accounting": _row_accounting_payload(accumulator),
            "timing": {
                "processing_seconds": round(processing_seconds, 6),
                "rows_per_second": round(
                    accumulator.processed_rows / processing_seconds
                    if processing_seconds > 0
                    else 0.0,
                    3,
                ),
            },
        },
    )
    return {
        "event_cohort": artifacts.event_cohort,
        "eligible_event_records": artifacts.eligible_event_records,
        "secondary_domain_records": artifacts.secondary_domain_records,
        "excluded_event_records": artifacts.excluded_event_records,
        "event_state_records": artifacts.event_state_records,
        "cohort_summary": artifacts.cohort_summary,
        "exclusion_reasons": artifacts.exclusion_reasons,
        "state_year_counts": artifacts.state_year_counts,
        "temporal_quality": artifacts.temporal_quality,
        "overlap_diagnostics": artifacts.overlap_diagnostics,
        "manifest": artifacts.manifest,
    }


def _classify_events(
    events_path: Path,
    state_overlaps_path: Path,
    start_date: date,
    end_date: date,
    primary_domain: str,
    batch_size: int,
    max_rows: int | None,
) -> tuple[list[dict[str, Any]], CohortAccumulator]:
    accumulator = CohortAccumulator()
    state_codes_by_event = _state_codes_by_event(state_overlaps_path)
    events: list[dict[str, Any]] = []
    for batch in _iter_batches(events_path, batch_size, max_rows):
        data = batch.to_pydict()
        accumulator.processed_rows += batch.num_rows
        us_intersection_values = data.get("intersects_us_state_union")
        if us_intersection_values is None:
            us_intersection_values = data["intersects_united_states"]
        for index in range(batch.num_rows):
            if not bool(us_intersection_values[index]):
                continue
            accumulator.source_us_intersecting_rows += 1
            row = {key: values[index] for key, values in data.items()}
            source_id = str(row["source_record_id"])
            state_codes = sorted(state_codes_by_event.get(source_id, set()))
            if not state_codes and row.get("primary_state_code"):
                state_codes = [str(row["primary_state_code"])]
            study_domain = classify_study_domain(state_codes)
            accumulator.domain_counts[study_domain] += 1
            event = _cohort_event_row(row, state_codes, study_domain)
            primary_reason, secondary_reasons = _exclusion_reasons(
                event,
                start_date,
                end_date,
                primary_domain,
            )
            event["primary_exclusion_reason"] = primary_reason
            event["secondary_exclusion_reasons"] = ";".join(secondary_reasons)
            if primary_reason is None:
                event["cohort_status"] = "eligible"
                accumulator.eligible_rows += 1
                accumulator.split_counts[str(event["candidate_split"])] += 1
            elif primary_reason == "outside_primary_domain":
                event["cohort_status"] = "secondary_domain"
                accumulator.secondary_domain_rows += 1
                accumulator.exclusion_primary_counts[primary_reason] += 1
            else:
                event["cohort_status"] = "excluded"
                accumulator.excluded_rows += 1
                accumulator.exclusion_primary_counts[primary_reason] += 1
            for reason in secondary_reasons:
                accumulator.exclusion_secondary_counts[reason] += 1
            _merge_temporal_quality(accumulator, event, scope="source")
            if event["cohort_status"] == "eligible":
                _merge_temporal_quality(accumulator, event, scope="eligible")
            if event["start_date"] is not None and event["primary_state_code"] is not None:
                terminal_category = str(event["cohort_status"])
                split = (
                    str(event["candidate_split"])
                    if terminal_category == "eligible"
                    else "not_applicable"
                )
                accumulator.state_year_counts[
                    (
                        str(event["primary_state_code"]),
                        event["start_date"].year,
                        split,
                        terminal_category,
                        str(event["study_domain"]),
                        str(event["primary_exclusion_reason"] or "not_applicable"),
                    )
                ] += 1
            events.append(event)
    return events, accumulator


def classify_study_domain(state_codes: list[str]) -> str:
    if not state_codes:
        return "unclassified"
    domains = {_domain_for_state(code) for code in state_codes}
    domains.discard("unclassified")
    if not domains:
        return "unclassified"
    if len(domains) > 1:
        return "multi_domain"
    return domains.pop()


def _domain_for_state(code: str) -> str:
    normalized = code.upper()
    if normalized in CONUS_STATE_CODES:
        return "conus"
    if normalized in SECONDARY_DOMAIN_BY_STATE:
        return SECONDARY_DOMAIN_BY_STATE[normalized]
    if normalized in OTHER_TERRITORY_CODES:
        return "other_us_territory"
    return "unclassified"


def _cohort_event_row(
    row: dict[str, Any],
    state_codes: list[str],
    study_domain: str,
) -> dict[str, Any]:
    start = _parse_date(row.get("event_start_date"))
    end = _parse_date(row.get("event_end_date"))
    duration = (end - start).days if start is not None and end is not None else None
    return {
        "event_record_id": str(row["source_record_id"]),
        "source_uuid": str(row["source_record_id"]),
        "start_date": start,
        "end_date": end,
        "duration_days": duration,
        "primary_state_code": row.get("primary_state_code"),
        "primary_state_name": row.get("primary_state_name"),
        "state_candidate_count": row.get("state_candidate_count"),
        "multi_state_event": row.get("multi_state_event"),
        "intersecting_state_codes": ";".join(state_codes),
        "event_area_km2": row.get("event_area_km2"),
        "us_overlap_area_km2": row.get("us_overlap_area_km2"),
        "representative_longitude": row.get("representative_longitude"),
        "representative_latitude": row.get("representative_latitude"),
        "study_domain": study_domain,
        "spatial_review_required": row.get("spatial_review_required"),
        "spatial_review_reason": row.get("spatial_review_reason"),
        "candidate_split": _candidate_split(start),
        "geometry": row.get("geometry"),
        "_geometry_obj": shapely.from_wkb(row["geometry"]) if row.get("geometry") else None,
    }


def _exclusion_reasons(
    event: dict[str, Any],
    start_date: date,
    end_date: date,
    primary_domain: str,
) -> tuple[str | None, list[str]]:
    reasons: list[str] = []
    if event["start_date"] is None:
        reasons.append("missing_start_date")
    elif not (start_date <= event["start_date"] <= end_date):
        reasons.append("outside_temporal_window")
    if event["duration_days"] is not None and int(event["duration_days"]) < 0:
        reasons.append("invalid_duration")
    if event["primary_state_code"] is None:
        reasons.append("missing_state_assignment")
    if event["study_domain"] != primary_domain:
        reasons.append("outside_primary_domain")
    if (
        "outside_primary_domain" in reasons
        and "missing_state_assignment" not in reasons
        and event["study_domain"] != "unclassified"
    ):
        return "outside_primary_domain", [
            reason for reason in reasons if reason != "outside_primary_domain"
        ]
    for candidate in PRIMARY_EXCLUSION_REASONS:
        if candidate in reasons:
            return candidate, [reason for reason in reasons if reason != candidate]
    return None, []


def _merge_temporal_quality(
    accumulator: CohortAccumulator,
    event: dict[str, Any],
    scope: str,
) -> None:
    quality = (
        accumulator.temporal_quality_eligible
        if scope == "eligible"
        else accumulator.temporal_quality_source
    )
    records_by_year_month = (
        accumulator.records_by_year_month_eligible
        if scope == "eligible"
        else accumulator.records_by_year_month_source
    )
    durations = (
        accumulator.durations_eligible if scope == "eligible" else accumulator.durations_source
    )
    start = event["start_date"]
    end = event["end_date"]
    duration = event["duration_days"]
    if start is not None:
        records_by_year_month[(start.year, start.month)] += 1
    if end is None:
        quality["missing_end_dates"] += 1
    if duration is not None:
        durations.append(int(duration))
        if duration < 0:
            quality["end_date_before_start_date"] += 1
        if duration == 0:
            quality["same_day_records"] += 1
        if duration > DURATION_GT_1_DAY:
            quality["duration_gt_1_day"] += 1
        if duration > DURATION_GT_3_DAYS:
            quality["duration_gt_3_days"] += 1
        if duration > DURATION_GT_7_DAYS:
            quality["duration_gt_7_days"] += 1
        if duration > DURATION_GT_30_DAYS:
            quality["duration_gt_30_days"] += 1


def _state_records_for_events(
    state_overlaps_path: Path,
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    event_by_id = {str(event["event_record_id"]): event for event in events}
    rows: list[dict[str, Any]] = []
    for batch in _iter_batches(state_overlaps_path, 100_000, None):
        data = batch.to_pydict()
        for index in range(batch.num_rows):
            event_id = str(data["source_record_id"][index])
            event = event_by_id.get(event_id)
            if event is None:
                continue
            rows.append(
                {
                    "event_record_id": event_id,
                    "state_code": data["state_code"][index],
                    "state_name": data["state_name"][index],
                    "start_date": event["start_date"],
                    "end_date": event["end_date"],
                    "state_overlap_area_km2": data["overlap_area_km2"][index],
                    "event_area_fraction": data["event_area_fraction"][index],
                    "us_overlap_fraction": data["us_overlap_fraction"][index],
                    "is_primary_state": data["is_primary_state"][index],
                    "study_domain": event["study_domain"],
                }
            )
    return sorted(rows, key=lambda item: (str(item["event_record_id"]), str(item["state_code"])))


def _state_codes_by_event(state_overlaps_path: Path) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = defaultdict(set)
    for batch in _iter_batches(state_overlaps_path, 100_000, None):
        data = batch.to_pydict()
        for index in range(batch.num_rows):
            source_id = data["source_record_id"][index]
            state_code = data["state_code"][index]
            if source_id is not None and state_code is not None:
                mapping[str(source_id)].add(str(state_code))
    return mapping


def _overlap_diagnostics(
    events: list[dict[str, Any]],
    state_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    eligible = [event for event in events if event["cohort_status"] == "eligible"]
    states_by_event: dict[str, set[str]] = defaultdict(set)
    for row in state_rows:
        states_by_event[str(row["event_record_id"])].add(str(row["state_code"]))
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for event in eligible:
        start = event["start_date"]
        if start is None:
            continue
        for state_code in states_by_event.get(str(event["event_record_id"]), set()):
            grouped[(start.year, state_code)].append(event)

    counters: dict[tuple[int, str, str, str], int] = Counter()
    participants: dict[tuple[int, str, str, str], set[str]] = defaultdict(set)
    for (year, state_code), group in grouped.items():
        projected = [_project_geometry(event["_geometry_obj"]) for event in group]
        tree = STRtree(projected)
        for left_index, event in enumerate(group):
            for right_index in tree.query(
                projected[left_index],
                predicate="dwithin",
                distance=25_000,
            ):
                right = int(right_index)
                if right <= left_index:
                    continue
                left_event = event
                right_event = group[right]
                for temporal_rule in _temporal_rules(left_event, right_event):
                    for spatial_rule in _spatial_rules(projected[left_index], projected[right]):
                        key = (year, state_code, temporal_rule, spatial_rule)
                        counters[key] += 1
                        participants[key].update(
                            [
                                str(left_event["event_record_id"]),
                                str(right_event["event_record_id"]),
                            ]
                        )
    return [
        {
            "year": year,
            "state_code": state_code,
            "temporal_rule": temporal_rule,
            "spatial_rule": spatial_rule,
            "candidate_pair_count": count,
            "participating_record_count": len(participants[key]),
        }
        for key, count in sorted(counters.items())
        for year, state_code, temporal_rule, spatial_rule in [key]
    ]


def _temporal_rules(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
    left_start = left["start_date"]
    right_start = right["start_date"]
    left_end = left["end_date"] or left_start
    right_end = right["end_date"] or right_start
    if left_start is None or right_start is None:
        return []
    rules: list[str] = []
    gap = abs((right_start - left_start).days)
    if gap == 0:
        rules.append("same_start_date")
    if gap <= DATE_GAP_LE_1_DAY:
        rules.append("date_gap_le_1_day")
    if gap <= DATE_GAP_LE_2_DAYS:
        rules.append("date_gap_le_2_days")
    if (
        left_end is not None
        and right_end is not None
        and left_start <= right_end
        and right_start <= left_end
    ):
        rules.append("date_intervals_overlap")
    return rules


def _spatial_rules(left: BaseGeometry, right: BaseGeometry) -> list[str]:
    rules: list[str] = []
    if left.intersects(right):
        rules.append("geometry_intersects")
    distance_km = left.representative_point().distance(right.representative_point()) / 1000
    for threshold in DISTANCE_THRESHOLDS_KM:
        if distance_km <= threshold:
            rules.append(f"representative_point_distance_le_{int(threshold)}km")
    union_area = left.union(right).area
    iou = left.intersection(right).area / union_area if union_area > 0 else 0.0
    for threshold in IOU_THRESHOLDS:
        if iou >= threshold:
            rules.append(f"iou_ge_{threshold:.2f}")
    return rules


def _candidate_split(value: date | None) -> str | None:
    if value is None:
        return None
    if DEFAULT_START_DATE <= value <= DEVELOPMENT_END_DATE:
        return "development"
    if date(2024, 1, 1) <= value <= VALIDATION_END_DATE:
        return "validation"
    if date(2025, 1, 1) <= value <= DEFAULT_END_DATE:
        return "test"
    return None


def _iter_batches(path: Path, batch_size: int, max_rows: int | None) -> Iterator[pa.RecordBatch]:
    parquet_files = _parquet_files(path)
    emitted = 0
    for parquet_path in parquet_files:
        parquet_file = pq.ParquetFile(parquet_path)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            if max_rows is not None and emitted >= max_rows:
                return
            limited = batch
            if max_rows is not None and emitted + batch.num_rows > max_rows:
                limited = batch.slice(0, max_rows - emitted)
            emitted += limited.num_rows
            yield limited


def _write_event_outputs(
    artifacts: CohortArtifacts,
    events: list[dict[str, Any]],
    geo_metadata: dict[bytes, bytes],
) -> None:
    eligible = [event for event in events if event["cohort_status"] == "eligible"]
    secondary = [event for event in events if event["cohort_status"] == "secondary_domain"]
    excluded = [event for event in events if event["cohort_status"] == "excluded"]
    _write_event_dataset(artifacts.eligible_event_records, eligible, geo_metadata)
    _write_event_dataset(
        artifacts.secondary_domain_records,
        secondary,
        geo_metadata,
        include_reason=True,
    )
    _write_event_dataset(
        artifacts.excluded_event_records,
        excluded,
        geo_metadata,
        include_reason=True,
    )
    _write_event_dataset(artifacts.event_cohort, eligible, geo_metadata)


def _write_event_dataset(
    output_dir: Path,
    rows: list[dict[str, Any]],
    geo_metadata: dict[bytes, bytes],
    include_reason: bool = False,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    clean_rows = [_public_event_row(row, include_reason=include_reason) for row in rows]
    pq.write_table(
        pa.Table.from_pylist(clean_rows, schema=_event_schema(include_reason, geo_metadata)),
        output_dir / "part-00000.parquet",
    )


def _write_state_records(output_path: Path, rows: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=_state_record_schema()), output_path)


def _public_event_row(row: dict[str, Any], include_reason: bool) -> dict[str, Any]:
    fields = [field.name for field in _event_schema(include_reason, {}).remove_metadata()]
    return {field: row.get(field) for field in fields}


def _event_schema(include_reason: bool, metadata: dict[bytes, bytes]) -> pa.Schema:
    fields = [
        pa.field("event_record_id", pa.string()),
        pa.field("source_uuid", pa.string()),
        pa.field("start_date", pa.date32()),
        pa.field("end_date", pa.date32()),
        pa.field("duration_days", pa.int64()),
        pa.field("primary_state_code", pa.string()),
        pa.field("primary_state_name", pa.string()),
        pa.field("state_candidate_count", pa.int64()),
        pa.field("multi_state_event", pa.bool_()),
        pa.field("intersecting_state_codes", pa.string()),
        pa.field("event_area_km2", pa.float64()),
        pa.field("us_overlap_area_km2", pa.float64()),
        pa.field("representative_longitude", pa.float64()),
        pa.field("representative_latitude", pa.float64()),
        pa.field("study_domain", pa.string()),
        pa.field("spatial_review_required", pa.bool_()),
        pa.field("spatial_review_reason", pa.string()),
        pa.field("candidate_split", pa.string()),
        pa.field("geometry", pa.binary()),
    ]
    if include_reason:
        fields.extend(
            [
                pa.field("primary_exclusion_reason", pa.string()),
                pa.field("secondary_exclusion_reasons", pa.string()),
            ]
        )
    return pa.schema(fields, metadata=metadata)


def _state_record_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("event_record_id", pa.string()),
            pa.field("state_code", pa.string()),
            pa.field("state_name", pa.string()),
            pa.field("start_date", pa.date32()),
            pa.field("end_date", pa.date32()),
            pa.field("state_overlap_area_km2", pa.float64()),
            pa.field("event_area_fraction", pa.float64()),
            pa.field("us_overlap_fraction", pa.float64()),
            pa.field("is_primary_state", pa.bool_()),
            pa.field("study_domain", pa.string()),
        ]
    )


def _summary_payload(accumulator: CohortAccumulator) -> dict[str, Any]:
    return {
        "row_accounting": _row_accounting_payload(accumulator),
        "domain_counts": dict(sorted(accumulator.domain_counts.items())),
        "candidate_split_counts": dict(sorted(accumulator.split_counts.items())),
        "exclusion_primary_counts": dict(sorted(accumulator.exclusion_primary_counts.items())),
        "temporal_quality": {
            "eligible_primary": _temporal_quality_payload(accumulator, scope="eligible"),
            "source_us_intersecting": _temporal_quality_payload(accumulator, scope="source"),
        },
    }


def _row_accounting_payload(accumulator: CohortAccumulator) -> dict[str, Any]:
    return {
        "source_us_intersecting_rows": accumulator.source_us_intersecting_rows,
        "eligible_primary_rows": accumulator.eligible_rows,
        "secondary_domain_rows": accumulator.secondary_domain_rows,
        "other_excluded_rows": accumulator.excluded_rows,
        "balanced": accumulator.balanced,
    }


def _temporal_quality_payload(accumulator: CohortAccumulator, scope: str) -> dict[str, Any]:
    durations = sorted(
        accumulator.durations_eligible if scope == "eligible" else accumulator.durations_source
    )
    quality = (
        accumulator.temporal_quality_eligible
        if scope == "eligible"
        else accumulator.temporal_quality_source
    )
    payload: dict[str, Any] = dict(sorted(quality.items()))
    payload["duration_quantiles"] = {
        "p50": _quantile(durations, 0.5),
        "p90": _quantile(durations, 0.9),
        "p95": _quantile(durations, 0.95),
        "p99": _quantile(durations, 0.99),
    }
    return payload


def _write_exclusion_reasons(path: Path, accumulator: CohortAccumulator) -> None:
    rows = [
        {
            "reason_type": "primary",
            "reason": reason,
            "count": accumulator.exclusion_primary_counts[reason],
        }
        for reason in PRIMARY_EXCLUSION_REASONS
        if accumulator.exclusion_primary_counts[reason] > 0
    ]
    rows.extend(
        {
            "reason_type": "secondary",
            "reason": reason,
            "count": count,
        }
        for reason, count in sorted(accumulator.exclusion_secondary_counts.items())
    )
    _write_csv(path, rows, ["reason_type", "reason", "count"])


def _write_state_year_counts(path: Path, accumulator: CohortAccumulator) -> None:
    rows = [
        {
            "state_code": state,
            "year": year,
            "candidate_split": split,
            "terminal_category": terminal_category,
            "study_domain": study_domain,
            "primary_exclusion_reason": primary_exclusion_reason,
            "count": count,
        }
        for (
            state,
            year,
            split,
            terminal_category,
            study_domain,
            primary_exclusion_reason,
        ), count in sorted(accumulator.state_year_counts.items())
    ]
    _write_csv(
        path,
        rows,
        [
            "state_code",
            "year",
            "candidate_split",
            "terminal_category",
            "study_domain",
            "primary_exclusion_reason",
            "count",
        ],
    )


def _write_temporal_quality(path: Path, accumulator: CohortAccumulator) -> None:
    rows: list[dict[str, Any]] = []
    for scope, records, quality in [
        (
            "source_us_intersecting",
            accumulator.records_by_year_month_source,
            accumulator.temporal_quality_source,
        ),
        (
            "eligible_primary",
            accumulator.records_by_year_month_eligible,
            accumulator.temporal_quality_eligible,
        ),
    ]:
        for (year, month), count in sorted(records.items()):
            rows.append({"scope": scope, "metric": f"records_{year}_{month:02d}", "value": count})
        for key, value in sorted(quality.items()):
            rows.append({"scope": scope, "metric": key, "value": value})
        for key, value in _temporal_quality_payload(accumulator, scope=scope.split("_")[0])[
            "duration_quantiles"
        ].items():
            rows.append({"scope": scope, "metric": f"duration_{key}", "value": value})
    _write_csv(path, rows, ["scope", "metric", "value"])


def _schema_payload(path: Path) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(path)
    return {
        "path": str(path),
        "rows": parquet_file.metadata.num_rows,
        "columns": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in parquet_file.schema_arrow
        ],
    }


def _geo_metadata(path: Path) -> dict[bytes, bytes]:
    metadata = pq.read_metadata(_first_parquet_file(path)).metadata or {}
    return {b"geo": metadata[b"geo"]} if b"geo" in metadata else {}


def _first_parquet_file(path: Path) -> Path:
    files = _parquet_files(path)
    if not files:
        raise CohortError(f"No Parquet files found at {path}")
    return files[0]


def _parquet_files(path: Path) -> list[Path]:
    resolved = path.expanduser().resolve()
    if resolved.is_file():
        return [resolved]
    if not resolved.exists():
        raise CohortError(f"Input path does not exist: {resolved}")
    return sorted(resolved.glob("*.parquet"))


def _parse_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _project_geometry(geometry: BaseGeometry | None) -> BaseGeometry:
    if geometry is None:
        return shapely.GeometryCollection()
    return transform(AREA_TRANSFORMER.transform, geometry)


def _quantile(values: list[int], quantile: float) -> float | None:
    if not values:
        return None
    index = max(0, min(len(values) - 1, math.ceil(quantile * len(values)) - 1))
    return float(values[index])


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
