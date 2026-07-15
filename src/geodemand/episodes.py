from __future__ import annotations

import csv
import hashlib
import json
import math
import time
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, cast

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import Transformer
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform
from shapely.strtree import STRtree

from geodemand import __version__

PolicyName = Literal["conservative", "balanced", "broad"]

AREA_CRS = "EPSG:6933"
AREA_TRANSFORMER = Transformer.from_crs("EPSG:4326", AREA_CRS, always_xy=True)
POLICY_VERSION = "0.6B-v1"
REVIEW_MEMBER_COUNT = 100
REVIEW_DURATION_DAYS = 7
REVIEW_DISTANCE_KM = 250.0
REVIEW_STATE_COUNT = 5
MIN_PAIR_MEMBERS = 2
DEVELOPMENT_END_YEAR = 2023
VALIDATION_YEAR = 2024


class EpisodeError(ValueError):
    """Raised when candidate episode clustering cannot be completed."""


@dataclass(frozen=True)
class ClusterPolicy:
    name: PolicyName
    max_date_gap_days: int | None
    require_interval_overlap: bool
    distance_km: float | None
    iou: float | None
    allow_geometry_intersects: bool = True


POLICIES: dict[PolicyName, ClusterPolicy] = {
    "conservative": ClusterPolicy("conservative", None, True, None, None),
    "balanced": ClusterPolicy("balanced", 1, False, 10.0, 0.10),
    "broad": ClusterPolicy("broad", 2, False, 25.0, 0.05),
}


@dataclass
class EpisodeEvent:
    event_record_id: str
    start_date: date
    end_date: date
    candidate_split: str
    primary_state_code: str
    state_codes: tuple[str, ...]
    spatial_review_required: bool
    spatial_review_reason: str
    geometry_wkb: bytes
    geometry: BaseGeometry
    projected_geometry: BaseGeometry


@dataclass(frozen=True)
class EpisodeEdge:
    event_record_id_a: str
    event_record_id_b: str
    policy: str
    temporal_gap_days: int
    date_intervals_overlap: bool
    geometries_intersect: bool
    representative_distance_km: float
    intersection_area_km2: float
    union_area_km2: float
    intersection_over_union: float
    shared_state_codes: str
    edge_reason: str


@dataclass(frozen=True)
class BuiltEpisodes:
    events: list[EpisodeEvent]
    edges: list[EpisodeEdge]
    episodes: list[dict[str, Any]]
    membership: list[dict[str, Any]]


def inspect_episode_inputs(events_path: Path, event_states_path: Path) -> dict[str, Any]:
    return {
        "events": _schema_payload(_first_parquet_file(events_path)),
        "event_states": _schema_payload(_first_parquet_file(event_states_path)),
        "policies": {name: _policy_payload(policy) for name, policy in POLICIES.items()},
    }


def build_episodes(
    events_path: Path,
    event_states_path: Path,
    output_dir: Path,
    policy_name: PolicyName,
    batch_size: int = 10_000,
    max_rows: int | None = None,
) -> dict[str, Path]:
    if policy_name not in POLICIES:
        raise EpisodeError(f"Unsupported episode clustering policy: {policy_name}")
    started_at = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)
    policy_dir = output_dir / f"episodes_{policy_name}"
    policy_dir.mkdir(parents=True, exist_ok=True)

    events = _load_events(events_path, event_states_path, batch_size, max_rows)
    built = _build_policy(events, POLICIES[policy_name])
    geo_metadata = _geo_metadata(events_path)

    artifacts = {
        "policy_dir": policy_dir,
        "episodes": policy_dir / "episodes",
        "episode_membership": policy_dir / "episode_membership",
        "episode_edges": policy_dir / "episode_edges",
        "episode_summary": policy_dir / "episode_summary.json",
        "episode_size_distribution": policy_dir / "episode_size_distribution.csv",
        "state_year_episode_counts": policy_dir / "state_year_episode_counts.csv",
        "bridge_diagnostics": policy_dir / "bridge_diagnostics.csv",
        "split_boundary_diagnostics": policy_dir / "split_boundary_diagnostics.csv",
        "manifest": policy_dir / "manifest.json",
    }
    _write_table_dataset(artifacts["episode_edges"], built.edges, _edge_schema())
    _write_table_dataset(artifacts["episode_membership"], built.membership, _membership_schema())
    _write_table_dataset(artifacts["episodes"], built.episodes, _episode_schema(geo_metadata))
    _write_json(artifacts["episode_summary"], _summary_payload(built, policy_name))
    _write_csv(
        artifacts["episode_size_distribution"],
        _size_distribution_rows(built.episodes),
        ["member_count", "episode_count"],
    )
    _write_csv(
        artifacts["state_year_episode_counts"],
        _state_year_rows(built.episodes),
        ["state_code", "year", "candidate_split", "episode_count"],
    )
    _write_csv(
        artifacts["bridge_diagnostics"],
        _bridge_rows(built.episodes),
        [
            "episode_id",
            "member_count",
            "episode_duration_days",
            "maximum_member_distance_km",
            "bounding_box_diagonal_km",
            "union_area_km2",
            "state_count",
            "maximum_adjacent_temporal_gap_days",
            "bridge_edge_count",
            "clustering_review_reason",
        ],
    )
    _write_csv(
        artifacts["split_boundary_diagnostics"],
        [row for row in built.episodes if bool(row["crosses_split_boundary"])],
        ["episode_id", "candidate_split", "episode_start_date", "episode_end_date", "member_count"],
    )
    processing_seconds = time.perf_counter() - started_at
    _write_json(
        artifacts["manifest"],
        {
            "run_timestamp_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "package_version": __version__,
            "policy": policy_name,
            "policy_version": POLICY_VERSION,
            "policy_thresholds": _policy_payload(POLICIES[policy_name]),
            "events_input": str(events_path),
            "event_states_input": str(event_states_path),
            "output_dir": str(policy_dir),
            "configuration": {"batch_size": batch_size, "max_rows": max_rows, "area_crs": AREA_CRS},
            "row_accounting": _row_accounting(built),
            "timing": {
                "processing_seconds": round(processing_seconds, 6),
                "rows_per_second": round(
                    len(events) / processing_seconds if processing_seconds > 0 else 0.0,
                    3,
                ),
            },
        },
    )
    return artifacts


def compare_episode_policies(
    events_path: Path,
    event_states_path: Path,
    output_dir: Path,
    batch_size: int = 10_000,
    max_rows: int | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    events = _load_events(events_path, event_states_path, batch_size, max_rows)
    built_by_policy = {name: _build_policy(events, policy) for name, policy in POLICIES.items()}
    policy_pairs: list[tuple[PolicyName, PolicyName]] = [
        ("conservative", "balanced"),
        ("balanced", "broad"),
    ]
    sensitivity_rows = [_sensitivity_row(name, built) for name, built in built_by_policy.items()]
    agreement_rows: list[dict[str, Any]] = []
    for narrower, broader in policy_pairs:
        agreement_rows.append(
            _assignment_agreement(
                narrower,
                broader,
                built_by_policy[narrower].membership,
                built_by_policy[broader].membership,
            )
        )
    sensitivity_path = output_dir / "clustering_sensitivity.csv"
    agreement_path = output_dir / "clustering_agreement.csv"
    _write_csv(
        sensitivity_path,
        sensitivity_rows,
        [
            "policy",
            "source_candidate_records",
            "episode_count",
            "singleton_episodes",
            "multi_member_episodes",
            "median_members",
            "p90_members",
            "p95_members",
            "p99_members",
            "maximum_members",
            "median_duration",
            "p95_duration",
            "maximum_duration",
            "multistate_episodes",
            "split_boundary_episodes",
            "review_flagged_episodes",
            "total_accepted_edges",
        ],
    )
    _write_csv(
        agreement_path,
        agreement_rows,
        [
            "comparison",
            "narrower_policy",
            "broader_policy",
            "narrower_episode_count",
            "broader_episode_count",
            "narrower_episodes_preserved_exactly",
            "episodes_merged_by_broader_policy",
            "episodes_split_relative_to_broader_policy",
            "records_retaining_identical_complete_episode_membership",
            "pairwise_membership_agreement",
        ],
    )
    return {"clustering_sensitivity": sensitivity_path, "clustering_agreement": agreement_path}


def _assignment_agreement(
    narrower_policy: PolicyName,
    broader_policy: PolicyName,
    left_membership: list[dict[str, Any]],
    right_membership: list[dict[str, Any]],
) -> dict[str, Any]:
    left = {str(row["event_record_id"]): str(row["episode_id"]) for row in left_membership}
    right = {str(row["event_record_id"]): str(row["episode_id"]) for row in right_membership}
    left_components = _component_sets(left_membership)
    right_components = _component_sets(right_membership)
    left_sets = _component_members_by_event(left_membership)
    right_sets = _component_members_by_event(right_membership)
    ids = sorted(set(left) & set(right))
    same_pairs = 0
    total_pairs = 0
    for index, event_a in enumerate(ids):
        for event_b in ids[index + 1 :]:
            total_pairs += 1
            same_pairs += int(
                (left[event_a] == left[event_b]) == (right[event_a] == right[event_b])
            )
    preserved = 0
    merged = 0
    split = 0
    for left_members in left_components.values():
        overlapping_right = {
            episode_id for member in left_members if (episode_id := right.get(member)) is not None
        }
        if len(overlapping_right) > 1:
            split += 1
            continue
        if not overlapping_right:
            continue
        right_members = right_components[next(iter(overlapping_right))]
        if right_members == left_members:
            preserved += 1
        elif left_members < right_members:
            merged += 1
    return {
        "comparison": f"{narrower_policy}_vs_{broader_policy}",
        "narrower_policy": narrower_policy,
        "broader_policy": broader_policy,
        "narrower_episode_count": len(left_components),
        "broader_episode_count": len(right_components),
        "narrower_episodes_preserved_exactly": preserved,
        "episodes_merged_by_broader_policy": merged,
        "episodes_split_relative_to_broader_policy": split,
        "records_retaining_identical_complete_episode_membership": sum(
            1 for event_id in ids if left_sets[event_id] == right_sets[event_id]
        ),
        "pairwise_membership_agreement": same_pairs / total_pairs if total_pairs else 1.0,
    }


def _component_sets(membership: list[dict[str, Any]]) -> dict[str, frozenset[str]]:
    members_by_episode: dict[str, set[str]] = defaultdict(set)
    for row in membership:
        members_by_episode[str(row["episode_id"])].add(str(row["event_record_id"]))
    return {
        episode_id: frozenset(members) for episode_id, members in sorted(members_by_episode.items())
    }


def _component_members_by_event(membership: list[dict[str, Any]]) -> dict[str, frozenset[str]]:
    return {
        event_id: members
        for members in _component_sets(membership).values()
        for event_id in members
    }


def _load_events(
    events_path: Path,
    event_states_path: Path,
    batch_size: int,
    max_rows: int | None,
) -> list[EpisodeEvent]:
    states_by_event = _states_by_event(event_states_path)
    events: list[EpisodeEvent] = []
    for batch in _iter_batches(events_path, batch_size, max_rows):
        data = batch.to_pydict()
        for index in range(batch.num_rows):
            event_id = str(data["event_record_id"][index])
            geometry = shapely.from_wkb(data["geometry"][index])
            start = _as_date(data["start_date"][index])
            end = _as_optional_date(data["end_date"][index]) or start
            state_codes = tuple(
                sorted(states_by_event.get(event_id) or {str(data["primary_state_code"][index])})
            )
            events.append(
                EpisodeEvent(
                    event_record_id=event_id,
                    start_date=start,
                    end_date=end,
                    candidate_split=str(data["candidate_split"][index]),
                    primary_state_code=str(data["primary_state_code"][index]),
                    state_codes=state_codes,
                    spatial_review_required=bool(data["spatial_review_required"][index]),
                    spatial_review_reason=str(data["spatial_review_reason"][index] or ""),
                    geometry_wkb=data["geometry"][index],
                    geometry=geometry,
                    projected_geometry=_project_geometry(geometry),
                )
            )
    return sorted(events, key=lambda event: event.event_record_id)


def _build_policy(events: list[EpisodeEvent], policy: ClusterPolicy) -> BuiltEpisodes:
    edges = _accepted_edges(events, policy)
    components = _connected_components([event.event_record_id for event in events], edges)
    event_by_id = {event.event_record_id: event for event in events}
    episodes = [
        _episode_row([event_by_id[event_id] for event_id in component], policy.name, edges)
        for component in components
    ]
    membership = [
        _membership_row(episode, member_id, rank, event_by_id[member_id], policy.name)
        for episode in episodes
        for rank, member_id in enumerate(str(episode["_member_ids"]).split(";"), start=1)
    ]
    public_episodes = [
        {key: value for key, value in episode.items() if key != "_member_ids"}
        for episode in episodes
    ]
    return BuiltEpisodes(events, edges, public_episodes, membership)


def _accepted_edges(events: list[EpisodeEvent], policy: ClusterPolicy) -> list[EpisodeEdge]:
    grouped: dict[tuple[int, str], list[EpisodeEvent]] = defaultdict(list)
    for event in events:
        for year in range(event.start_date.year - 1, event.start_date.year + 2):
            for state_code in event.state_codes:
                grouped[(year, state_code)].append(event)
    pair_states: dict[tuple[str, str], set[str]] = defaultdict(set)
    event_by_id = {event.event_record_id: event for event in events}
    broad_distance = 25_000.0
    for (_year, state_code), group in grouped.items():
        unique = sorted(
            {event.event_record_id: event for event in group}.values(),
            key=lambda item: item.event_record_id,
        )
        geometries = [event.projected_geometry for event in unique]
        tree = STRtree(geometries)
        for left_index, left in enumerate(unique):
            for right_raw in tree.query(
                geometries[left_index],
                predicate="dwithin",
                distance=broad_distance,
            ):
                right_index = int(right_raw)
                if right_index <= left_index:
                    continue
                right = unique[right_index]
                pair = cast(
                    tuple[str, str],
                    tuple(sorted((left.event_record_id, right.event_record_id))),
                )
                pair_states[pair].add(state_code)
    edges: list[EpisodeEdge] = []
    for pair, states in sorted(pair_states.items()):
        left = event_by_id[pair[0]]
        right = event_by_id[pair[1]]
        edge = _edge_if_accepted(left, right, sorted(states), policy)
        if edge is not None:
            edges.append(edge)
    return edges


def _edge_if_accepted(
    left: EpisodeEvent,
    right: EpisodeEvent,
    shared_states: list[str],
    policy: ClusterPolicy,
) -> EpisodeEdge | None:
    temporal_gap = _date_gap_days(left, right)
    intervals_overlap = left.start_date <= right.end_date and right.start_date <= left.end_date
    if policy.require_interval_overlap and not intervals_overlap:
        return None
    if policy.max_date_gap_days is not None and temporal_gap > policy.max_date_gap_days:
        return None
    intersection = left.projected_geometry.intersection(right.projected_geometry)
    intersection_area = _area_km2(intersection)
    union_area = _area_km2(left.projected_geometry.union(right.projected_geometry))
    iou = intersection_area / union_area if union_area > 0 else 0.0
    intersects = bool(left.projected_geometry.intersects(right.projected_geometry))
    distance_km = (
        left.projected_geometry.representative_point().distance(
            right.projected_geometry.representative_point()
        )
        / 1000.0
    )
    spatial_reasons: list[str] = []
    if intersects and policy.allow_geometry_intersects:
        spatial_reasons.append("geometries_intersect")
    if policy.iou is not None and iou >= policy.iou:
        spatial_reasons.append(f"iou_ge_{policy.iou:.2f}")
    if policy.distance_km is not None and distance_km <= policy.distance_km:
        spatial_reasons.append(f"representative_distance_le_{int(policy.distance_km)}km")
    if not spatial_reasons:
        return None
    return EpisodeEdge(
        event_record_id_a=min(left.event_record_id, right.event_record_id),
        event_record_id_b=max(left.event_record_id, right.event_record_id),
        policy=policy.name,
        temporal_gap_days=temporal_gap,
        date_intervals_overlap=intervals_overlap,
        geometries_intersect=intersects,
        representative_distance_km=distance_km,
        intersection_area_km2=intersection_area,
        union_area_km2=union_area,
        intersection_over_union=iou,
        shared_state_codes=";".join(shared_states),
        edge_reason=";".join(spatial_reasons),
    )


def _connected_components(event_ids: list[str], edges: list[EpisodeEdge]) -> list[list[str]]:
    neighbors: dict[str, set[str]] = {event_id: set() for event_id in event_ids}
    for edge in edges:
        neighbors[edge.event_record_id_a].add(edge.event_record_id_b)
        neighbors[edge.event_record_id_b].add(edge.event_record_id_a)
    seen: set[str] = set()
    components: list[list[str]] = []
    for event_id in sorted(event_ids):
        if event_id in seen:
            continue
        queue = deque([event_id])
        seen.add(event_id)
        component: list[str] = []
        while queue:
            current = queue.popleft()
            component.append(current)
            for neighbor in sorted(neighbors[current]):
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append(neighbor)
        components.append(sorted(component))
    return components


def _episode_row(
    members: list[EpisodeEvent],
    policy: str,
    edges: list[EpisodeEdge],
) -> dict[str, Any]:
    members = sorted(members, key=lambda event: event.event_record_id)
    member_ids = [member.event_record_id for member in members]
    start = min(member.start_date for member in members)
    end = max(member.end_date for member in members)
    split = _split_for_date(start)
    split_values = {member.candidate_split for member in members}
    projected_union = shapely.union_all([member.projected_geometry for member in members])
    source_union = shapely.union_all([member.geometry for member in members])
    state_codes = sorted({state for member in members for state in member.state_codes})
    max_distance = _max_member_distance_km(members)
    diagonal = _bbox_diagonal_km(projected_union)
    adjacent_gap = _max_adjacent_gap(members)
    bridge_count = _bridge_edge_count(member_ids, edges)
    review_reasons = _review_reasons(
        len(members),
        (end - start).days,
        max_distance,
        len(state_codes),
    )
    if any(member.spatial_review_required for member in members):
        review_reasons.append("member_spatial_review")
    episode_id = _episode_id(policy, member_ids)
    return {
        "episode_id": episode_id,
        "policy": policy,
        "episode_start_date": start,
        "episode_end_date": end,
        "episode_duration_days": (end - start).days,
        "member_count": len(members),
        "state_codes": ";".join(state_codes),
        "state_count": len(state_codes),
        "primary_state_code": Counter(member.primary_state_code for member in members).most_common(
            1
        )[0][0],
        "union_area_km2": _area_km2(projected_union),
        "representative_longitude": float(source_union.representative_point().x),
        "representative_latitude": float(source_union.representative_point().y),
        "maximum_member_distance_km": max_distance,
        "bounding_box_diagonal_km": diagonal,
        "candidate_split": split,
        "crosses_split_boundary": len(split_values) > 1,
        "spatial_review_required": bool(review_reasons),
        "clustering_review_reason": ";".join(dict.fromkeys(review_reasons)),
        "geometry": shapely.to_wkb(source_union),
        "maximum_adjacent_temporal_gap_days": adjacent_gap,
        "bridge_edge_count": bridge_count,
        "_member_ids": ";".join(member_ids),
    }


def _membership_row(
    episode: dict[str, Any],
    member_id: str,
    rank: int,
    event: EpisodeEvent,
    policy: str,
) -> dict[str, Any]:
    return {
        "episode_id": episode["episode_id"],
        "event_record_id": member_id,
        "policy": policy,
        "membership_rank": rank,
        "is_episode_anchor": rank == 1,
        "candidate_split": event.candidate_split,
        "primary_state_code": event.primary_state_code,
        "start_date": event.start_date,
        "end_date": event.end_date,
    }


def _sensitivity_row(policy: str, built: BuiltEpisodes) -> dict[str, Any]:
    sizes = sorted(int(row["member_count"]) for row in built.episodes)
    durations = sorted(int(row["episode_duration_days"]) for row in built.episodes)
    return {
        "policy": policy,
        "source_candidate_records": len(built.events),
        "episode_count": len(built.episodes),
        "singleton_episodes": sum(1 for size in sizes if size == 1),
        "multi_member_episodes": sum(1 for size in sizes if size > 1),
        "median_members": _quantile(sizes, 0.5),
        "p90_members": _quantile(sizes, 0.9),
        "p95_members": _quantile(sizes, 0.95),
        "p99_members": _quantile(sizes, 0.99),
        "maximum_members": max(sizes) if sizes else 0,
        "median_duration": _quantile(durations, 0.5),
        "p95_duration": _quantile(durations, 0.95),
        "maximum_duration": max(durations) if durations else 0,
        "multistate_episodes": sum(1 for row in built.episodes if int(row["state_count"]) > 1),
        "split_boundary_episodes": sum(
            1 for row in built.episodes if bool(row["crosses_split_boundary"])
        ),
        "review_flagged_episodes": sum(
            1 for row in built.episodes if bool(row["spatial_review_required"])
        ),
        "total_accepted_edges": len(built.edges),
    }


def _states_by_event(path: Path) -> dict[str, set[str]]:
    states: dict[str, set[str]] = defaultdict(set)
    for batch in _iter_batches(path, 100_000, None):
        data = batch.to_pydict()
        for index in range(batch.num_rows):
            states[str(data["event_record_id"][index])].add(str(data["state_code"][index]))
    return states


def _date_gap_days(left: EpisodeEvent, right: EpisodeEvent) -> int:
    if left.start_date <= right.end_date and right.start_date <= left.end_date:
        return 0
    if left.end_date < right.start_date:
        return (right.start_date - left.end_date).days
    return (left.start_date - right.end_date).days


def _episode_id(policy: str, member_ids: list[str]) -> str:
    payload = json.dumps(
        {"policy_version": POLICY_VERSION, "policy": policy, "members": sorted(member_ids)},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _review_reasons(
    member_count: int,
    duration_days: int,
    max_distance_km: float,
    state_count: int,
) -> list[str]:
    reasons: list[str] = []
    if member_count > REVIEW_MEMBER_COUNT:
        reasons.append("member_count_gt_100")
    if duration_days > REVIEW_DURATION_DAYS:
        reasons.append("duration_gt_7_days")
    if max_distance_km > REVIEW_DISTANCE_KM:
        reasons.append("maximum_member_distance_gt_250km")
    if state_count > REVIEW_STATE_COUNT:
        reasons.append("state_count_gt_5")
    return reasons


def _max_member_distance_km(members: list[EpisodeEvent]) -> float:
    max_distance = 0.0
    points = [member.projected_geometry.representative_point() for member in members]
    for index, left in enumerate(points):
        for right in points[index + 1 :]:
            max_distance = max(max_distance, left.distance(right) / 1000.0)
    return max_distance


def _bbox_diagonal_km(geometry: BaseGeometry) -> float:
    minx, miny, maxx, maxy = geometry.bounds
    return math.hypot(maxx - minx, maxy - miny) / 1000.0


def _max_adjacent_gap(members: list[EpisodeEvent]) -> int:
    ordered = sorted(members, key=lambda event: event.start_date)
    if len(ordered) < MIN_PAIR_MEMBERS:
        return 0
    return max(
        (ordered[index + 1].start_date - ordered[index].start_date).days
        for index in range(len(ordered) - 1)
    )


def _bridge_edge_count(member_ids: list[str], edges: list[EpisodeEdge]) -> int:
    member_set = set(member_ids)
    return sum(
        1
        for edge in edges
        if edge.event_record_id_a in member_set and edge.event_record_id_b in member_set
    )


def _split_for_date(value: date) -> str:
    if value.year <= DEVELOPMENT_END_YEAR:
        return "development"
    if value.year == VALIDATION_YEAR:
        return "validation"
    return "test"


def _summary_payload(built: BuiltEpisodes, policy: str) -> dict[str, Any]:
    return {
        "policy": policy,
        "policy_version": POLICY_VERSION,
        "row_accounting": _row_accounting(built),
        "episode_count": len(built.episodes),
        "total_accepted_edges": len(built.edges),
        "singleton_episodes": sum(1 for row in built.episodes if int(row["member_count"]) == 1),
        "review_flagged_episodes": sum(
            1 for row in built.episodes if bool(row["spatial_review_required"])
        ),
    }


def _row_accounting(built: BuiltEpisodes) -> dict[str, Any]:
    membership_rows = len(built.membership)
    member_sum = sum(int(row["member_count"]) for row in built.episodes)
    return {
        "eligible_candidate_records": len(built.events),
        "episode_membership_rows": membership_rows,
        "sum_episode_member_counts": member_sum,
        "balanced": len(built.events) == membership_rows == member_sum,
    }


def _size_distribution_rows(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(int(row["member_count"]) for row in episodes)
    return [
        {"member_count": size, "episode_count": count} for size, count in sorted(counts.items())
    ]


def _state_year_rows(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, int, str]] = Counter()
    for episode in episodes:
        for state_code in str(episode["state_codes"]).split(";"):
            key = (
                state_code,
                episode["episode_start_date"].year,
                str(episode["candidate_split"]),
            )
            counts[key] += 1
    return [
        {"state_code": state, "year": year, "candidate_split": split, "episode_count": count}
        for (state, year, split), count in sorted(counts.items())
    ]


def _bridge_rows(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = [
        "episode_id",
        "member_count",
        "episode_duration_days",
        "maximum_member_distance_km",
        "bounding_box_diagonal_km",
        "union_area_km2",
        "state_count",
        "maximum_adjacent_temporal_gap_days",
        "bridge_edge_count",
        "clustering_review_reason",
    ]
    return [{key: row[key] for key in keys} for row in episodes]


def _edge_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("event_record_id_a", pa.string()),
            pa.field("event_record_id_b", pa.string()),
            pa.field("policy", pa.string()),
            pa.field("temporal_gap_days", pa.int64()),
            pa.field("date_intervals_overlap", pa.bool_()),
            pa.field("geometries_intersect", pa.bool_()),
            pa.field("representative_distance_km", pa.float64()),
            pa.field("intersection_area_km2", pa.float64()),
            pa.field("union_area_km2", pa.float64()),
            pa.field("intersection_over_union", pa.float64()),
            pa.field("shared_state_codes", pa.string()),
            pa.field("edge_reason", pa.string()),
        ]
    )


def _membership_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("episode_id", pa.string()),
            pa.field("event_record_id", pa.string()),
            pa.field("policy", pa.string()),
            pa.field("membership_rank", pa.int64()),
            pa.field("is_episode_anchor", pa.bool_()),
            pa.field("candidate_split", pa.string()),
            pa.field("primary_state_code", pa.string()),
            pa.field("start_date", pa.date32()),
            pa.field("end_date", pa.date32()),
        ]
    )


def _episode_schema(metadata: dict[bytes, bytes]) -> pa.Schema:
    return pa.schema(
        [
            pa.field("episode_id", pa.string()),
            pa.field("policy", pa.string()),
            pa.field("episode_start_date", pa.date32()),
            pa.field("episode_end_date", pa.date32()),
            pa.field("episode_duration_days", pa.int64()),
            pa.field("member_count", pa.int64()),
            pa.field("state_codes", pa.string()),
            pa.field("state_count", pa.int64()),
            pa.field("primary_state_code", pa.string()),
            pa.field("union_area_km2", pa.float64()),
            pa.field("representative_longitude", pa.float64()),
            pa.field("representative_latitude", pa.float64()),
            pa.field("maximum_member_distance_km", pa.float64()),
            pa.field("bounding_box_diagonal_km", pa.float64()),
            pa.field("candidate_split", pa.string()),
            pa.field("crosses_split_boundary", pa.bool_()),
            pa.field("spatial_review_required", pa.bool_()),
            pa.field("clustering_review_reason", pa.string()),
            pa.field("geometry", pa.binary()),
            pa.field("maximum_adjacent_temporal_gap_days", pa.int64()),
            pa.field("bridge_edge_count", pa.int64()),
        ],
        metadata=metadata,
    )


def _write_table_dataset(
    output_dir: Path,
    rows: Iterable[Any],
    schema: pa.Schema,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    py_rows = [row.__dict__ if hasattr(row, "__dict__") else row for row in rows]
    pq.write_table(pa.Table.from_pylist(py_rows, schema=schema), output_dir / "part-00000.parquet")


def _iter_batches(path: Path, batch_size: int, max_rows: int | None) -> Iterator[pa.RecordBatch]:
    emitted = 0
    for parquet_path in _parquet_files(path):
        parquet_file = pq.ParquetFile(parquet_path)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            if max_rows is not None and emitted >= max_rows:
                return
            limited = batch
            if max_rows is not None and emitted + batch.num_rows > max_rows:
                limited = batch.slice(0, max_rows - emitted)
            emitted += limited.num_rows
            yield limited


def _parquet_files(path: Path) -> list[Path]:
    resolved = path.expanduser().resolve()
    if resolved.is_file():
        return [resolved]
    if not resolved.exists():
        raise EpisodeError(f"Input path does not exist: {resolved}")
    files = sorted(resolved.glob("*.parquet"))
    if not files:
        raise EpisodeError(f"No Parquet files found at {resolved}")
    return files


def _first_parquet_file(path: Path) -> Path:
    return _parquet_files(path)[0]


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


def _project_geometry(geometry: BaseGeometry) -> BaseGeometry:
    return transform(AREA_TRANSFORMER.transform, geometry)


def _area_km2(geometry: BaseGeometry) -> float:
    return 0.0 if geometry.is_empty else float(geometry.area) / 1_000_000.0


def _as_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _as_optional_date(value: object) -> date | None:
    if value is None:
        return None
    return _as_date(value)


def _quantile(values: list[int], quantile: float) -> float | None:
    if not values:
        return None
    index = max(0, min(len(values) - 1, math.ceil(quantile * len(values)) - 1))
    return float(values[index])


def _policy_payload(policy: ClusterPolicy) -> dict[str, Any]:
    return {
        "policy_version": POLICY_VERSION,
        "max_date_gap_days": policy.max_date_gap_days,
        "require_interval_overlap": policy.require_interval_overlap,
        "distance_km": policy.distance_km,
        "iou": policy.iou,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
