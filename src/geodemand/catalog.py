from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, cast

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
import yaml

from geodemand import __version__
from geodemand.schemas import validate_physical_evidence_provenance

CatalogStatus = Literal[
    "retained_provisionally",
    "merge_candidate",
    "split_candidate",
    "physically_supported",
    "weak_physical_support",
    "insufficient_evidence",
    "manual_review_required",
    "excluded_from_analysis",
]
BRIDGE_MIN_MEMBER_COUNT = 2
SMALL_SIZE_MAX = 5
MEDIUM_SIZE_MAX = 25
LARGE_SIZE_MAX = 100
MIN_SPLIT_GROUP_COUNT = 2
MIN_MERGE_EPISODE_COUNT = 2


class CatalogError(ValueError):
    """Raised when provisional catalog processing cannot continue."""


@dataclass(frozen=True)
class CatalogInputs:
    eligible_events: Path
    event_states: Path
    cohort_summary: Path
    conservative_episodes: Path
    conservative_membership: Path
    conservative_edges: Path
    conservative_summary: Path
    conservative_manifest: Path
    conservative_bridge_diagnostics: Path | None
    conservative_split_diagnostics: Path | None
    balanced_episodes: Path
    balanced_membership: Path
    balanced_edges: Path
    balanced_summary: Path
    balanced_manifest: Path
    clustering_sensitivity: Path
    clustering_agreement: Path
    mrms_episode_metrics: Path | None
    mrms_member_metrics: Path | None
    mrms_coherence: Path | None
    usgs_associations: Path | None
    usgs_gauge_metrics: Path | None
    usgs_episode_summary: Path | None
    usgs_request_manifest: Path | None


def discover_catalog_inputs(
    cohort_root: Path,
    episode_root: Path,
    comparison_root: Path,
    mrms_root: Path | None = None,
    usgs_root: Path | None = None,
) -> CatalogInputs:
    required = {
        "eligible event records": _find_dataset(cohort_root, "eligible_event_records"),
        "event state records": _find_dataset(cohort_root, "event_state_records"),
        "cohort summary": _find_file(cohort_root, "cohort_summary.json"),
        "conservative episodes": _find_policy_dataset(episode_root, "conservative", "episodes"),
        "conservative membership": _find_policy_dataset(
            episode_root, "conservative", "episode_membership"
        ),
        "conservative edges": _find_policy_dataset(episode_root, "conservative", "episode_edges"),
        "conservative summary": _find_policy_file(
            episode_root, "conservative", "episode_summary.json"
        ),
        "conservative manifest": _find_policy_file(episode_root, "conservative", "manifest.json"),
        "balanced episodes": _find_policy_dataset(episode_root, "balanced", "episodes"),
        "balanced membership": _find_policy_dataset(episode_root, "balanced", "episode_membership"),
        "balanced edges": _find_policy_dataset(episode_root, "balanced", "episode_edges"),
        "balanced summary": _find_policy_file(episode_root, "balanced", "episode_summary.json"),
        "balanced manifest": _find_policy_file(episode_root, "balanced", "manifest.json"),
        "clustering sensitivity": _find_file(comparison_root, "clustering_sensitivity.csv"),
        "clustering agreement": _find_file(comparison_root, "clustering_agreement.csv"),
    }
    missing = [name for name, path in required.items() if path is None]
    if missing:
        raise CatalogError("Required catalog inputs are missing: " + ", ".join(missing))
    resolved = cast(dict[str, Path], required)
    return CatalogInputs(
        eligible_events=resolved["eligible event records"],
        event_states=resolved["event state records"],
        cohort_summary=resolved["cohort summary"],
        conservative_episodes=resolved["conservative episodes"],
        conservative_membership=resolved["conservative membership"],
        conservative_edges=resolved["conservative edges"],
        conservative_summary=resolved["conservative summary"],
        conservative_manifest=resolved["conservative manifest"],
        conservative_bridge_diagnostics=_find_policy_file(
            episode_root, "conservative", "bridge_diagnostics.csv"
        ),
        conservative_split_diagnostics=_find_policy_file(
            episode_root, "conservative", "split_boundary_diagnostics.csv"
        ),
        balanced_episodes=resolved["balanced episodes"],
        balanced_membership=resolved["balanced membership"],
        balanced_edges=resolved["balanced edges"],
        balanced_summary=resolved["balanced summary"],
        balanced_manifest=resolved["balanced manifest"],
        clustering_sensitivity=resolved["clustering sensitivity"],
        clustering_agreement=resolved["clustering agreement"],
        mrms_episode_metrics=_optional_dataset(mrms_root, "episode_precipitation_metrics"),
        mrms_member_metrics=_optional_dataset(mrms_root, "member_precipitation_metrics"),
        mrms_coherence=_optional_dataset(mrms_root, "physical_coherence_assessment"),
        usgs_associations=_optional_dataset(usgs_root, "usgs_episode_gauge_associations"),
        usgs_gauge_metrics=_optional_dataset(usgs_root, "usgs_gauge_response_metrics"),
        usgs_episode_summary=_optional_dataset(usgs_root, "usgs_episode_response_summary"),
        usgs_request_manifest=_optional_dataset(usgs_root, "usgs_file_or_request_manifest"),
    )


def inspect_catalog_inputs(inputs: CatalogInputs) -> dict[str, Any]:
    availability = _availability(inputs)
    return {
        "eligible_event_count": _row_count(inputs.eligible_events),
        "conservative_episode_count": _row_count(inputs.conservative_episodes),
        "conservative_membership_count": _row_count(inputs.conservative_membership),
        "balanced_episode_count": _row_count(inputs.balanced_episodes),
        "balanced_membership_count": _row_count(inputs.balanced_membership),
        "availability": availability,
    }


def load_catalog_rules(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not payload.get("rule_version"):
        raise CatalogError("Catalog rules must be a mapping with rule_version.")
    return payload


def stable_provisional_id(rule_version: str, event_ids: Sequence[str]) -> str:
    members = sorted(set(event_ids))
    if not members:
        raise CatalogError("A provisional episode cannot have zero members.")
    payload = "\x1f".join([rule_version, *members])
    return hashlib.sha256(payload.encode()).hexdigest()


def build_provisional_catalog(  # noqa: PLR0915
    inputs: CatalogInputs,
    output_dir: Path,
    rules_path: Path,
) -> dict[str, Path]:
    rules = load_catalog_rules(rules_path)
    rule_version = str(rules["rule_version"])
    output_dir.mkdir(parents=True, exist_ok=True)
    events = {str(row["event_record_id"]): row for row in _read_rows(inputs.eligible_events)}
    conservative_episodes = _read_rows(inputs.conservative_episodes)
    conservative_episode_by_id = {str(row["episode_id"]): row for row in conservative_episodes}
    conservative_membership = _read_rows(inputs.conservative_membership)
    balanced_membership = _read_rows(inputs.balanced_membership)
    conservative_edges = _read_rows(inputs.conservative_edges)
    for label, evidence_path in (
        ("MRMS episode metrics", inputs.mrms_episode_metrics),
        ("MRMS member metrics", inputs.mrms_member_metrics),
        ("MRMS coherence", inputs.mrms_coherence),
        ("USGS associations", inputs.usgs_associations),
        ("USGS gauge metrics", inputs.usgs_gauge_metrics),
        ("USGS episode summary", inputs.usgs_episode_summary),
    ):
        _validate_observed_evidence(evidence_path, label)
    members_by_conservative = _group_members(conservative_membership)
    conservative_by_event = {
        str(row["event_record_id"]): str(row["episode_id"]) for row in conservative_membership
    }
    balanced_by_event = {
        str(row["event_record_id"]): str(row["episode_id"]) for row in balanced_membership
    }
    conservative_by_balanced: dict[str, set[str]] = defaultdict(set)
    for event_id, balanced_id in balanced_by_event.items():
        conservative_by_balanced[balanced_id].add(conservative_by_event[event_id])
    edge_counts: Counter[str] = Counter()
    for edge in conservative_edges:
        episode_id = conservative_by_event.get(str(edge["event_record_id_a"]))
        if episode_id:
            edge_counts[episode_id] += 1
    mrms_metrics = _index_optional(inputs.mrms_episode_metrics)
    mrms_coherence = _index_optional(inputs.mrms_coherence)
    usgs_summaries = _index_optional(inputs.usgs_episode_summary)
    usgs_associations = _group_optional(inputs.usgs_associations)
    usgs_gauge_metrics = _group_optional(inputs.usgs_gauge_metrics)
    geo_metadata = _parquet_schema(inputs.conservative_episodes).metadata or {}

    episode_rows: list[dict[str, Any]] = []
    membership_rows: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    quality_rows: list[dict[str, Any]] = []
    split_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []
    decision_rows: list[dict[str, Any]] = []
    for source_episode in sorted(conservative_episodes, key=lambda row: str(row["episode_id"])):
        source_id = str(source_episode["episode_id"])
        member_ids = members_by_conservative[source_id]
        provisional_id = stable_provisional_id(rule_version, member_ids)
        balanced_ids = sorted({balanced_by_event[event_id] for event_id in member_ids})
        related_balanced = balanced_ids[0] if len(balanced_ids) == 1 else ";".join(balanced_ids)
        balanced_sources = sorted(
            {
                conservative_id
                for balanced_id in balanced_ids
                for conservative_id in conservative_by_balanced[balanced_id]
            }
        )
        balanced_splits = {
            _split_for_date(_as_date(conservative_episode_by_id[item]["episode_start_date"]), rules)
            for item in balanced_sources
        }
        balanced_crosses_split = len(balanced_splits) > 1
        balanced_merge_supported = _balanced_merge_supported(
            balanced_sources,
            conservative_episode_by_id,
            mrms_metrics,
            mrms_coherence,
            rules,
        )
        evidence = _evidence_row(
            provisional_id,
            source_episode,
            related_balanced,
            balanced_sources,
            edge_counts[source_id],
            sum(len(members_by_conservative[item]) for item in balanced_sources),
            mrms_metrics.get(source_id),
            mrms_coherence.get(source_id),
            usgs_summaries.get(source_id),
            usgs_associations.get(source_id, []),
            usgs_gauge_metrics.get(source_id, []),
            balanced_crosses_split,
            balanced_merge_supported,
            rules,
        )
        status, review_reasons, evidence_strength = _catalog_decision(
            source_episode, evidence, rules
        )
        split = _split_for_date(_as_date(source_episode["episode_start_date"]), rules)
        geometry_wkb = source_episode["geometry"]
        geometry = shapely.from_wkb(geometry_wkb)
        bounds = [float(value) for value in geometry.bounds]
        states = str(source_episode["state_codes"])
        flags = _clustering_flags(source_episode, rules)
        episode_rows.append(
            {
                "provisional_episode_id": provisional_id,
                "source_conservative_episode_ids": source_id,
                "source_conservative_episode_id": source_id,
                "related_balanced_episode_id": related_balanced,
                "member_count": len(member_ids),
                "start_date": _as_date(source_episode["episode_start_date"]).isoformat(),
                "end_date": _as_date(source_episode["episode_end_date"]).isoformat(),
                "duration_days": int(source_episode["episode_duration_days"]),
                "candidate_split": split,
                "state_count": int(source_episode["state_count"]),
                "states": states,
                "primary_state": str(source_episode["primary_state_code"]),
                "geometry": geometry_wkb,
                "union_area_km2": float(source_episode["union_area_km2"]),
                "bounding_box": json.dumps(bounds, separators=(",", ":")),
                "representative_longitude": float(source_episode["representative_longitude"]),
                "representative_latitude": float(source_episode["representative_latitude"]),
                "multistate_flag": int(source_episode["state_count"]) > 1,
                "split_boundary_flag": bool(source_episode["crosses_split_boundary"])
                or balanced_crosses_split,
                "spatial_review_flag": bool(source_episode["spatial_review_required"]),
                "large_cluster_flag": flags["large_cluster"],
                "long_duration_flag": flags["long_duration"],
                "provisional_catalog_status": status,
                "evidence_strength": evidence_strength,
                "manual_review_flag": bool(review_reasons),
                "manual_review_reasons": ";".join(review_reasons),
                "rule_version": rule_version,
            }
        )
        evidence_rows.append(evidence)
        split_rows.append(
            {
                "provisional_episode_id": provisional_id,
                "candidate_split": split,
                "episode_start_date": _as_date(source_episode["episode_start_date"]).isoformat(),
                "rule_version": rule_version,
            }
        )
        for event_id in member_ids:
            event = events[event_id]
            membership_rows.append(
                {
                    "provisional_episode_id": provisional_id,
                    "event_record_id": event_id,
                    "source_conservative_episode_id": source_id,
                    "source_balanced_episode_id": balanced_by_event[event_id],
                    "membership_action": "pending_review" if review_reasons else "retained",
                    "membership_action_reason": ";".join(review_reasons),
                    "candidate_split": split,
                    "state_codes": str(event["intersecting_state_codes"]),
                    "start_date": _as_date(event["start_date"]).isoformat(),
                    "end_date": _as_date(event["end_date"]).isoformat(),
                }
            )
        for reason in review_reasons:
            quality_rows.append(
                {
                    "provisional_episode_id": provisional_id,
                    "quality_flag": reason,
                    "flag_source": "catalog_rules",
                    "rule_version": rule_version,
                }
            )
        if review_reasons:
            review_rows.append(
                _review_row(
                    provisional_id,
                    source_id,
                    related_balanced,
                    source_episode,
                    split,
                    review_reasons,
                    evidence,
                    rules,
                )
            )
        decision_rows.append(
            _initial_decision_row(
                provisional_id,
                source_id,
                len(member_ids),
                rule_version,
                status,
                review_reasons,
            )
        )

    paths = _catalog_paths(output_dir)
    _write_parquet(paths["episodes"], episode_rows, geo_metadata)
    _write_parquet(paths["episode_membership"], membership_rows)
    _write_parquet(paths["episode_evidence"], evidence_rows)
    _write_parquet(paths["episode_quality_flags"], quality_rows)
    _write_parquet(paths["episode_split_assignments"], split_rows)
    _write_parquet(paths["clustering_decisions"], decision_rows)
    _write_parquet(paths["manual_review_queue"], review_rows)
    _write_review_template(paths["manual_review_template"], review_rows)
    validation = _validation_payload(
        episode_rows, membership_rows, split_rows, rule_version, evidence_rows
    )
    _write_csv(paths["split_leakage_diagnostics"], validation["diagnostics"])
    summary = _catalog_summary(episode_rows, evidence_rows, membership_rows, len(events))
    _write_json(paths["catalog_summary"], summary)
    availability = _availability(inputs)
    _write_json(paths["evidence_availability_summary"], availability)
    _write_json(paths["input_availability"], inspect_catalog_inputs(inputs))
    _write_json(
        paths["validation_summary"], {k: v for k, v in validation.items() if k != "diagnostics"}
    )
    _write_json(paths["catalog_rules_resolved"], rules)
    _write_summary_csvs(output_dir, episode_rows, evidence_rows, review_rows)
    manifest = {
        "package_version": __version__,
        "rule_version": rule_version,
        "configuration_sha256": _sha256_file(rules_path),
        "input_hashes": _input_hashes(inputs),
        "output_hashes": {
            name: _sha256_file(path)
            for name, path in paths.items()
            if path.exists() and name != "manifest"
        },
    }
    _write_json(paths["manifest"], manifest)
    if not validation["valid"]:
        raise CatalogError("Catalog validation failed; inspect validation_summary.json.")
    return paths


def validate_catalog(catalog_dir: Path) -> dict[str, Any]:
    episodes = _read_rows(catalog_dir / "episodes.parquet")
    membership = _read_rows(catalog_dir / "episode_membership.parquet")
    splits = _read_rows(catalog_dir / "episode_split_assignments.parquet")
    evidence = _read_rows(catalog_dir / "episode_evidence.parquet")
    versions = {str(row["rule_version"]) for row in episodes}
    if len(versions) != 1:
        raise CatalogError("Catalog contains multiple or missing rule versions.")
    payload = _validation_payload(episodes, membership, splits, next(iter(versions)), evidence)
    _write_json(
        catalog_dir / "validation_summary.json",
        {key: value for key, value in payload.items() if key != "diagnostics"},
    )
    if not payload["valid"]:
        raise CatalogError("Catalog validation failed.")
    return payload


def summarize_catalog(catalog_dir: Path) -> dict[str, Any]:
    episodes = _read_rows(catalog_dir / "episodes.parquet")
    evidence = _read_rows(catalog_dir / "episode_evidence.parquet")
    membership = _read_rows(catalog_dir / "episode_membership.parquet")
    summary = _catalog_summary(episodes, evidence, membership, len(membership))
    _write_json(catalog_dir / "catalog_summary.json", summary)
    review = _read_rows(catalog_dir / "manual_review_queue.parquet")
    _write_summary_csvs(catalog_dir, episodes, evidence, review)
    return summary


def apply_catalog_decisions(  # noqa: PLR0912, PLR0915
    catalog_dir: Path,
    decisions_csv: Path,
    rule_version: str,
    output_dir: Path,
) -> Path:
    episodes = _read_rows(catalog_dir / "episodes.parquet")
    membership = _read_rows(catalog_dir / "episode_membership.parquet")
    known = {str(row["provisional_episode_id"]): row for row in episodes}
    members_by_episode = _group_members(membership, "provisional_episode_id")
    with decisions_csv.open(encoding="utf-8", newline="") as handle:
        decisions = list(csv.DictReader(handle))
    seen: set[str] = set()
    output_rows = []
    assigned_members: set[str] = set()
    for row in decisions:
        episode_id = str(row.get("provisional_episode_id", ""))
        if episode_id not in known:
            raise CatalogError(f"Unknown provisional episode ID: {episode_id}")
        action = str(row.get("reviewer_action", "")).strip()
        reason = str(row.get("reviewer_reason", "")).strip()
        reviewer = str(row.get("reviewer_name", "")).strip()
        if action and (not reason or not reviewer):
            raise CatalogError(f"Decision {episode_id} requires reviewer and reason.")
        source_ids = [episode_id]
        if action == "merge":
            source_ids = sorted(
                {item for item in str(row.get("merge_episode_ids", episode_id)).split(";") if item}
            )
            if len(source_ids) < MIN_MERGE_EPISODE_COUNT:
                raise CatalogError(f"Merge decision {episode_id} requires at least two episodes.")
            unknown = sorted(set(source_ids) - set(known))
            if unknown:
                raise CatalogError("Unknown merge episode IDs: " + ", ".join(unknown))
            source_splits = {str(known[item]["candidate_split"]) for item in source_ids}
            override = str(row.get("cross_split_override", "")).lower() in {"1", "true", "yes"}
            if len(source_splits) > 1 and not override:
                raise CatalogError(
                    f"Merge decision {episode_id} crosses splits and requires an explicit override."
                )
        duplicate_sources = seen.intersection(source_ids)
        if duplicate_sources:
            raise CatalogError("Duplicate review decision: " + ", ".join(sorted(duplicate_sources)))
        seen.update(source_ids)
        source_members = sorted(
            member for source_id in source_ids for member in members_by_episode[source_id]
        )
        if action == "split":
            groups = _parse_split_groups(str(row.get("resulting_member_groups", "")))
            flattened = [member for group in groups for member in group]
            if sorted(flattened) != sorted(source_members) or len(flattened) != len(set(flattened)):
                raise CatalogError(
                    f"Split decision {episode_id} omits or duplicates source members."
                )
            resulting = [stable_provisional_id(rule_version, group) for group in groups]
        elif action == "merge":
            resulting = [stable_provisional_id(rule_version, source_members)]
        else:
            resulting = [episode_id]
        overlap = assigned_members.intersection(source_members)
        if overlap:
            raise CatalogError(f"Decision assignments duplicate {len(overlap)} event records.")
        assigned_members.update(source_members)
        output_rows.append(
            {
                "decision_id": hashlib.sha256(
                    f"{rule_version}:{episode_id}:{action}:{reason}".encode()
                ).hexdigest(),
                "decision_type": action or "deferred",
                "source_episode_ids": ";".join(source_ids),
                "resulting_episode_ids": ";".join(resulting),
                "affected_event_record_count": len(source_members),
                "decision_source": "manual" if action else "deferred",
                "rule_version": rule_version,
                "reviewer": reviewer,
                "reason": reason,
                "timestamp": str(row.get("review_timestamp", "")),
                "input_manifest_hash": _sha256_file(catalog_dir / "manifest.json"),
            }
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / "clustering_decisions.parquet"
    _write_parquet(output, output_rows)
    return output


def _evidence_row(
    provisional_id: str,
    source: Mapping[str, Any],
    balanced_id: str,
    balanced_sources: Sequence[str],
    edge_count: int,
    balanced_member_count: int,
    mrms: Mapping[str, Any] | None,
    coherence: Mapping[str, Any] | None,
    usgs: Mapping[str, Any] | None,
    associations: Sequence[Mapping[str, Any]],
    gauge_metrics: Sequence[Mapping[str, Any]],
    balanced_crosses_split: bool,
    balanced_merge_supported: bool,
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    mrms_status = "available" if mrms else "pending"
    minimum_mrms_coverage = float(
        cast(Mapping[str, Any], rules["mrms"])["minimum_coverage_fraction"]
    )
    if mrms and float(mrms.get("coverage_fraction", 0.0)) < minimum_mrms_coverage:
        mrms_status = "insufficient_coverage"
    minimum_mrms_quality = float(
        cast(Mapping[str, Any], rules["mrms"])["minimum_quality_valid_fraction"]
    )
    if mrms and float(mrms.get("quality_valid_fraction", 0.0)) < minimum_mrms_quality:
        mrms_status = "insufficient_coverage"
    if usgs:
        usgs_status = "no_suitable_gauge" if usgs.get("no_gauge_reason") else "available"
    else:
        usgs_status = "pending"
    strongest_rise = max(
        (
            float(row["absolute_rise"])
            for row in gauge_metrics
            if row.get("absolute_rise") is not None
        ),
        default=None,
    )
    association_order = {
        "inside_episode_geometry": 0,
        "inside_25km_buffer": 1,
        "nearby_within_50km": 2,
        "fallback_within_100km": 3,
        "uncertain_association": 4,
        "no_suitable_gauge": 5,
    }
    best_association = min(
        (str(row["association_quality"]) for row in associations),
        key=lambda value: association_order.get(value, 99),
        default="",
    )
    return {
        "provisional_episode_id": provisional_id,
        "source_conservative_episode_id": str(source["episode_id"]),
        "related_balanced_episode_id": balanced_id,
        "conservative_member_count": int(source["member_count"]),
        "conservative_duration_days": int(source["episode_duration_days"]),
        "conservative_state_count": int(source["state_count"]),
        "conservative_edge_count": edge_count,
        "conservative_bridge_edge_count": int(source.get("bridge_edge_count", 0)),
        "conservative_articulation_point_count": None,
        "conservative_maximum_member_distance_km": float(source["maximum_member_distance_km"]),
        "conservative_review_reasons": str(source.get("clustering_review_reason", "")),
        "balanced_merges_multiple_conservative_episodes": len(balanced_sources) > 1,
        "balanced_source_episode_count": len(balanced_sources),
        "balanced_total_member_count": balanced_member_count,
        "balanced_crosses_split_boundary": balanced_crosses_split,
        "balanced_merge_physically_supported": balanced_merge_supported,
        "mrms_status": mrms_status,
        "mrms_coverage_fraction": _value(mrms, "coverage_fraction"),
        "mrms_quality_valid_fraction": _value(mrms, "quality_valid_fraction"),
        "mrms_max_1h_mm": _first_value(mrms, "max_gridcell_1h_mm", "maximum_gridcell_1h_mm"),
        "mrms_max_3h_mm": _first_value(mrms, "max_gridcell_3h_mm", "maximum_gridcell_3h_mm"),
        "mrms_max_6h_mm": _first_value(mrms, "max_gridcell_6h_mm", "maximum_gridcell_6h_mm"),
        "mrms_total_area_mean_mm": _first_value(
            mrms, "episode_total_area_mean_mm", "total_area_mean_mm"
        ),
        "mrms_rainfall_peak_count": _value(mrms, "rainfall_peak_count"),
        "mrms_largest_dry_gap_hours": _value(mrms, "largest_dry_gap_hours"),
        "mrms_member_peak_time_range_hours": _value(coherence, "member_peak_time_range_hours"),
        "mrms_median_member_timeseries_correlation": _value(
            coherence, "median_member_timeseries_correlation"
        ),
        "mrms_minimum_member_timeseries_correlation": _value(
            coherence, "minimum_member_timeseries_correlation"
        ),
        "mrms_multiple_peak_flag": _value(coherence, "multiple_precipitation_peak_flag"),
        "mrms_coherence_assessment": _value(coherence, "provisional_category"),
        "usgs_status": usgs_status,
        "usgs_candidate_gauge_count": _int_value(usgs, "candidate_gauge_count"),
        "usgs_selected_gauge_count": _int_value(usgs, "selected_gauge_count"),
        "usgs_adequate_coverage_gauge_count": _int_value(usgs, "gauges_with_adequate_coverage"),
        "usgs_detected_response_count": _int_value(usgs, "gauges_with_detected_response"),
        "usgs_best_association_quality": best_association,
        "usgs_strongest_response_parameter": str(
            usgs.get("strongest_response_parameter", "") if usgs else ""
        ),
        "usgs_strongest_absolute_rise": strongest_rise,
        "usgs_shortest_precipitation_to_response_lag_hours": usgs.get(
            "shortest_precipitation_to_response_lag"
        )
        if usgs
        else None,
        "usgs_hydrologic_response_supported": usgs.get("hydrologic_response_supported")
        if usgs_status == "available" and usgs
        else None,
        "usgs_support_strength": usgs.get("support_strength") if usgs else None,
        "imerg_status": "deferred",
    }


def _catalog_decision(
    source: Mapping[str, Any], evidence: Mapping[str, Any], rules: Mapping[str, Any]
) -> tuple[CatalogStatus, list[str], str]:
    flags = _clustering_flags(source, rules)
    reasons = [name for name, enabled in flags.items() if enabled]
    if bool(source["crosses_split_boundary"]) or bool(evidence["balanced_crosses_split_boundary"]):
        reasons.append("split_boundary")
    if bool(source["spatial_review_required"]):
        reasons.append("geometry_review")
    if bool(evidence["balanced_merges_multiple_conservative_episodes"]):
        reasons.append("possible_undermerge")
    mrms_available = evidence["mrms_status"] == "available"
    if mrms_available:
        split_conditions = _mrms_split_conditions(evidence, rules)
        minimum = int(cast(Mapping[str, Any], rules["mrms"])["split_minimum_conditions"])
        if len(split_conditions) >= minimum:
            return "split_candidate", sorted(set(reasons + split_conditions)), "physical"
        if (
            bool(evidence["balanced_merges_multiple_conservative_episodes"])
            and bool(evidence["balanced_merge_physically_supported"])
            and not bool(evidence["balanced_crosses_split_boundary"])
        ):
            return "merge_candidate", sorted(set(reasons)), "physical"
        meaningful = float(evidence.get("mrms_max_1h_mm") or 0.0) >= float(
            cast(Mapping[str, Any], rules["mrms"])["meaningful_max_1h_mm"]
        )
        if meaningful and not reasons:
            usgs_support = evidence.get("usgs_hydrologic_response_supported")
            if usgs_support is True or evidence["usgs_status"] == "no_suitable_gauge":
                return "physically_supported", [], "mrms_usgs"
            return "weak_physical_support", ["weak_physical_signal"], "mrms"
    if reasons:
        return "manual_review_required", sorted(set(reasons)), "clustering_only"
    return "retained_provisionally", [], "pending_physical_evidence"


def _clustering_flags(source: Mapping[str, Any], rules: Mapping[str, Any]) -> dict[str, bool]:
    limits = cast(Mapping[str, Any], rules["clustering_review"])
    return {
        "large_cluster": int(source["member_count"]) > int(limits["large_member_count"]),
        "long_duration": int(source["episode_duration_days"]) > int(limits["long_duration_days"]),
        "high_state_count": int(source["state_count"]) > int(limits["high_state_count"]),
        "maximum_member_distance": float(source["maximum_member_distance_km"])
        > float(limits["maximum_member_distance_km"]),
        "bridge_chaining": int(source.get("bridge_edge_count", 0)) > 0
        and int(source["member_count"]) > BRIDGE_MIN_MEMBER_COUNT,
    }


def _mrms_split_conditions(evidence: Mapping[str, Any], rules: Mapping[str, Any]) -> list[str]:
    mrms = cast(Mapping[str, Any], rules["mrms"])
    conditions = []
    if int(evidence.get("mrms_rainfall_peak_count") or 0) >= int(mrms["multiple_peak_minimum"]):
        conditions.append("mrms_multiple_peaks")
    if int(evidence.get("mrms_largest_dry_gap_hours") or 0) >= int(mrms["long_dry_gap_hours"]):
        conditions.append("mrms_long_dry_gap")
    correlation = evidence.get("mrms_median_member_timeseries_correlation")
    if correlation is not None and float(correlation) < float(mrms["weak_median_correlation"]):
        conditions.append("mrms_weak_member_correlation")
    peak_range = evidence.get("mrms_member_peak_time_range_hours")
    if peak_range is not None and float(peak_range) > float(mrms["large_peak_time_range_hours"]):
        conditions.append("mrms_large_peak_time_range")
    return conditions


def _balanced_merge_supported(  # noqa: PLR0911
    source_ids: Sequence[str],
    episodes: Mapping[str, Mapping[str, Any]],
    metrics: Mapping[str, Mapping[str, Any]],
    coherence: Mapping[str, Mapping[str, Any]],
    rules: Mapping[str, Any],
) -> bool:
    if len(source_ids) < MIN_MERGE_EPISODE_COUNT:
        return False
    mrms = cast(Mapping[str, Any], rules["mrms"])
    peak_times: list[datetime] = []
    for source_id in source_ids:
        metric = metrics.get(source_id)
        assessment = coherence.get(source_id)
        if metric is None or assessment is None:
            return False
        if float(metric.get("coverage_fraction", 0.0)) < float(mrms["minimum_coverage_fraction"]):
            return False
        if float(metric.get("quality_valid_fraction", 0.0)) < float(
            mrms["minimum_quality_valid_fraction"]
        ):
            return False
        if float(metric.get("largest_dry_gap_hours", 0.0)) > float(
            mrms["merge_maximum_dry_gap_hours"]
        ):
            return False
        correlation = assessment.get("median_member_timeseries_correlation")
        if correlation is None or float(correlation) < float(mrms["merge_minimum_correlation"]):
            return False
        peak = metric.get("time_of_max_area_mean_utc")
        if peak is None:
            return False
        peak_times.append(datetime.fromisoformat(str(peak).replace("Z", "+00:00")))
    source_splits = {
        _split_for_date(_as_date(episodes[source_id]["episode_start_date"]), rules)
        for source_id in source_ids
    }
    if len(source_splits) > 1:
        return False
    peak_range = (max(peak_times) - min(peak_times)).total_seconds() / 3600
    return peak_range <= float(mrms["merge_maximum_peak_difference_hours"])


def _review_row(
    provisional_id: str,
    source_id: str,
    balanced_id: str,
    source: Mapping[str, Any],
    split: str,
    reasons: Sequence[str],
    evidence: Mapping[str, Any],
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    priorities = cast(Mapping[str, Any], rules["review_priority"])
    review_types = [_review_type(reason) for reason in reasons]
    review_type = max(review_types, key=lambda reason: int(priorities.get(reason, 0)))
    priority = int(priorities.get(review_type, 10))
    action = _recommended_action(review_type)
    return {
        "review_id": hashlib.sha256(
            f"{rules['rule_version']}:{provisional_id}".encode()
        ).hexdigest(),
        "provisional_episode_id": provisional_id,
        "source_conservative_episode_ids": source_id,
        "related_balanced_episode_id": balanced_id,
        "review_priority": priority,
        "review_type": review_type,
        "review_reasons": ";".join(sorted(set(reasons))),
        "member_count": int(source["member_count"]),
        "duration_days": int(source["episode_duration_days"]),
        "states": str(source["state_codes"]),
        "candidate_split": split,
        "mrms_status": evidence["mrms_status"],
        "usgs_status": evidence["usgs_status"],
        "evidence_summary": (
            f"mrms={evidence['mrms_status']};usgs={evidence['usgs_status']};"
            f"balanced_sources={evidence['balanced_source_episode_count']}"
        ),
        "recommended_action": action,
        "reviewer_action": "",
        "reviewer_reason": "",
        "reviewer_name": "",
        "review_timestamp": "",
        "decision_version": "",
    }


def _recommended_action(review_type: str) -> str:
    return {
        "possible_undermerge": "obtain_mrms",
        "split_boundary": "retain",
        "geometry_review": "inspect_geometry",
        "large_cluster": "inspect_timeseries",
        "long_duration": "inspect_timeseries",
        "bridge_chaining": "inspect_timeseries",
    }.get(review_type, "no_action")


def _review_type(reason: str) -> str:
    if reason.startswith("mrms_"):
        return "possible_overmerge"
    return {
        "high_state_count": "large_cluster",
        "maximum_member_distance": "possible_overmerge",
        "bridge_chaining": "possible_overmerge",
        "possible_undermerge": "possible_undermerge",
        "large_cluster": "large_cluster",
        "long_duration": "long_duration",
        "split_boundary": "split_boundary",
        "geometry_review": "geometry_review",
        "weak_physical_signal": "weak_physical_signal",
    }.get(reason, "insufficient_evidence")


def _initial_decision_row(
    provisional_id: str,
    source_id: str,
    member_count: int,
    rule_version: str,
    status: str,
    reasons: Sequence[str],
) -> dict[str, Any]:
    source = "deferred" if reasons else "rule_based"
    return {
        "decision_id": hashlib.sha256(
            f"{rule_version}:{provisional_id}:{source}".encode()
        ).hexdigest(),
        "decision_type": "review_pending" if reasons else "retain",
        "source_episode_ids": source_id,
        "resulting_episode_ids": provisional_id,
        "affected_event_record_count": member_count,
        "decision_source": source,
        "rule_version": rule_version,
        "reviewer": "",
        "reason": ";".join(reasons) if reasons else status,
        "timestamp": "",
        "input_manifest_hash": "",
    }


def _validation_payload(
    episodes: Sequence[Mapping[str, Any]],
    membership: Sequence[Mapping[str, Any]],
    splits: Sequence[Mapping[str, Any]],
    rule_version: str,
    evidence: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    event_ids = [str(row["event_record_id"]) for row in membership]
    episode_ids = [str(row["provisional_episode_id"]) for row in episodes]
    split_ids = [str(row["provisional_episode_id"]) for row in splits]
    evidence_ids = [str(row["provisional_episode_id"]) for row in evidence]
    members = _group_members(membership, "provisional_episode_id")
    stable = all(
        stable_provisional_id(rule_version, members[episode_id]) == episode_id
        for episode_id in episode_ids
    )
    valid_geometry = all(
        (geometry := shapely.from_wkb(row["geometry"])) is not None
        and not geometry.is_empty
        and geometry.is_valid
        for row in episodes
    )
    valid_lineage = all(
        bool(str(row.get("source_conservative_episode_ids", ""))) for row in episodes
    ) and all(bool(str(row.get("source_conservative_episode_id", ""))) for row in membership)
    episode_split = {
        str(row["provisional_episode_id"]): str(row["candidate_split"]) for row in splits
    }
    split_integrity = all(
        str(row["candidate_split"]) == episode_split.get(str(row["provisional_episode_id"]))
        for row in membership
    )
    diagnostics = [
        {"check": "unique_event_membership", "passed": len(event_ids) == len(set(event_ids))},
        {"check": "one_split_per_episode", "passed": sorted(episode_ids) == sorted(split_ids)},
        {"check": "stable_provisional_ids", "passed": stable},
        {
            "check": "membership_episode_lineage",
            "passed": set(members) == set(episode_ids),
        },
        {"check": "source_lineage_present", "passed": valid_lineage},
        {"check": "valid_episode_geometry", "passed": valid_geometry},
        {
            "check": "one_evidence_row_per_episode",
            "passed": sorted(episode_ids) == sorted(evidence_ids),
        },
        {"check": "membership_split_integrity", "passed": split_integrity},
    ]
    return {
        "valid": all(bool(row["passed"]) for row in diagnostics),
        "episode_count": len(episodes),
        "membership_row_count": len(membership),
        "unique_event_record_count": len(set(event_ids)),
        "duplicate_membership_count": len(event_ids) - len(set(event_ids)),
        "split_assignment_count": len(splits),
        "cross_split_membership_leakage_count": sum(
            str(row["candidate_split"]) != episode_split.get(str(row["provisional_episode_id"]))
            for row in membership
        ),
        "stable_ids": stable,
        "diagnostics": diagnostics,
    }


def _catalog_summary(
    episodes: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
    membership: Sequence[Mapping[str, Any]],
    source_count: int,
) -> dict[str, Any]:
    statuses = Counter(str(row["provisional_catalog_status"]) for row in episodes)
    mrms = Counter(str(row["mrms_status"]) for row in evidence)
    usgs = Counter(str(row["usgs_status"]) for row in evidence)
    splits = Counter(str(row["candidate_split"]) for row in episodes)
    return {
        "source_eligible_record_count": source_count,
        "source_conservative_episode_count": len(episodes),
        "provisional_episode_count": len(episodes),
        "total_membership_rows": len(membership),
        "retained_provisional_count": statuses["retained_provisionally"],
        "physically_supported_count": statuses["physically_supported"],
        "weak_physical_support_count": statuses["weak_physical_support"],
        "merge_candidate_count": statuses["merge_candidate"],
        "split_candidate_count": statuses["split_candidate"],
        "manual_review_count": sum(bool(row["manual_review_flag"]) for row in episodes),
        "insufficient_evidence_count": statuses["insufficient_evidence"],
        "excluded_count": statuses["excluded_from_analysis"],
        "mrms_available_count": mrms["available"],
        "mrms_pending_count": mrms["pending"],
        "usgs_available_count": usgs["available"],
        "no_suitable_gauge_count": usgs["no_suitable_gauge"],
        "imerg_deferred_count": len(evidence),
        "development_episode_count": splits["development"],
        "validation_episode_count": splits["validation"],
        "test_episode_count": splits["test"],
        "split_boundary_count": sum(bool(row["split_boundary_flag"]) for row in episodes),
        "multistate_count": sum(bool(row["multistate_flag"]) for row in episodes),
        "large_cluster_count": sum(bool(row["large_cluster_flag"]) for row in episodes),
        "long_duration_count": sum(bool(row["long_duration_flag"]) for row in episodes),
    }


def _write_summary_csvs(
    output_dir: Path,
    episodes: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
    reviews: Sequence[Mapping[str, Any]],
) -> None:
    _write_counter_csv(
        output_dir / "catalog_status_by_year.csv",
        Counter(
            (str(row["start_date"])[:4], str(row["provisional_catalog_status"])) for row in episodes
        ),
        ["year", "status", "count"],
    )
    _write_counter_csv(
        output_dir / "catalog_status_by_state.csv",
        Counter(
            (state, str(row["provisional_catalog_status"]))
            for row in episodes
            for state in str(row["states"]).split(";")
            if state
        ),
        ["state", "status", "count"],
    )
    _write_counter_csv(
        output_dir / "catalog_status_by_size.csv",
        Counter(
            (_size_bin(int(row["member_count"])), str(row["provisional_catalog_status"]))
            for row in episodes
        ),
        ["size_bin", "status", "count"],
    )
    _write_counter_csv(
        output_dir / "evidence_status_counts.csv",
        Counter(
            (source, str(row[f"{source}_status"]))
            for row in evidence
            for source in ("mrms", "usgs", "imerg")
        ),
        ["source", "status", "count"],
    )
    _write_counter_csv(
        output_dir / "manual_review_reason_counts.csv",
        Counter(
            reason for row in reviews for reason in str(row["review_reasons"]).split(";") if reason
        ),
        ["reason", "count"],
    )


def _availability(inputs: CatalogInputs) -> dict[str, Any]:
    return {
        "candidate_cohort": "available",
        "conservative_policy": "available",
        "balanced_policy": "available",
        "clustering_comparison": "available",
        "mrms_episode_metrics": "available" if inputs.mrms_episode_metrics else "pending",
        "mrms_member_metrics": "available" if inputs.mrms_member_metrics else "pending",
        "mrms_coherence": "available" if inputs.mrms_coherence else "pending",
        "usgs_associations": "available" if inputs.usgs_associations else "pending",
        "usgs_gauge_metrics": "available" if inputs.usgs_gauge_metrics else "pending",
        "usgs_episode_summary": "available" if inputs.usgs_episode_summary else "pending",
        "imerg": "deferred",
    }


def _catalog_paths(output_dir: Path) -> dict[str, Path]:
    names = [
        "episodes",
        "episode_membership",
        "episode_evidence",
        "episode_quality_flags",
        "episode_split_assignments",
        "clustering_decisions",
        "manual_review_queue",
    ]
    paths = {name: output_dir / f"{name}.parquet" for name in names}
    paths.update(
        {
            "manual_review_template": output_dir / "manual_review_template.csv",
            "split_leakage_diagnostics": output_dir / "split_leakage_diagnostics.csv",
            "catalog_summary": output_dir / "catalog_summary.json",
            "evidence_availability_summary": output_dir / "evidence_availability_summary.json",
            "input_availability": output_dir / "input_availability.json",
            "validation_summary": output_dir / "validation_summary.json",
            "catalog_rules_resolved": output_dir / "catalog_rules_resolved.json",
            "manifest": output_dir / "manifest.json",
        }
    )
    return paths


def _write_review_template(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = (
        list(rows[0])
        if rows
        else [
            "review_id",
            "provisional_episode_id",
            "reviewer_action",
            "reviewer_reason",
            "reviewer_name",
            "review_timestamp",
            "decision_version",
            "resulting_member_groups",
            "merge_episode_ids",
            "cross_split_override",
        ]
    )
    if "resulting_member_groups" not in fields:
        fields.append("resulting_member_groups")
    if "merge_episode_ids" not in fields:
        fields.append("merge_episode_ids")
    if "cross_split_override" not in fields:
        fields.append("cross_split_override")
    _write_csv(path, rows, fields)


def _split_for_date(value: date, rules: Mapping[str, Any]) -> str:
    periods = cast(Mapping[str, Sequence[Any]], rules["split_periods"])
    for name, bounds in periods.items():
        if _as_date(bounds[0]) <= value <= _as_date(bounds[1]):
            return name
    raise CatalogError(f"Episode start date {value.isoformat()} is outside configured splits.")


def _find_policy_dataset(root: Path, policy: str, dataset: str) -> Path | None:
    candidates = sorted(
        path
        for path in root.rglob(dataset)
        if path.is_dir() and policy in str(path).lower() and any(path.glob("*.parquet"))
    )
    return candidates[0] if candidates else None


def _find_policy_file(root: Path, policy: str, name: str) -> Path | None:
    candidates = sorted(path for path in root.rglob(name) if policy in str(path.parent).lower())
    return candidates[0] if candidates else None


def _find_dataset(root: Path, name: str) -> Path | None:
    candidates = [
        path
        for path in [
            root / name,
            root / f"{name}.parquet",
            *root.rglob(name),
            *root.rglob(f"{name}.parquet"),
        ]
        if path.exists()
    ]
    for path in sorted(candidates):
        if path.is_file() and path.suffix == ".parquet":
            return path
        if path.is_dir() and any(path.glob("*.parquet")):
            return path
    return None


def _find_file(root: Path, name: str) -> Path | None:
    direct = root / name
    if direct.exists():
        return direct
    matches = sorted(root.rglob(name))
    return matches[0] if matches else None


def _optional_dataset(root: Path | None, name: str) -> Path | None:
    return _find_dataset(root, name) if root and root.exists() else None


def _read_rows(path: Path) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _validate_observed_evidence(path: Path | None, label: str) -> None:
    if path is None:
        return
    try:
        validate_physical_evidence_provenance(pq.read_table(path), label)
    except ValueError as exc:
        raise CatalogError(str(exc)) from exc


def _row_count(path: Path) -> int:
    if path.is_file():
        return int(pq.ParquetFile(path).metadata.num_rows)
    return sum(
        int(pq.ParquetFile(item).metadata.num_rows) for item in sorted(path.glob("*.parquet"))
    )


def _parquet_schema(path: Path) -> pa.Schema:
    source = path if path.is_file() else sorted(path.glob("*.parquet"))[0]
    return pq.ParquetFile(source).schema_arrow


def _write_parquet(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    metadata: Mapping[bytes, bytes] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        table = pa.Table.from_pylist([dict(row) for row in rows])
    else:
        table = pa.table({"provisional_episode_id": pa.array([], type=pa.string())})
    if metadata:
        table = table.replace_schema_metadata(dict(metadata))
    pq.write_table(table, path, compression="zstd")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    fields: Sequence[str] | None = None,
) -> None:
    fieldnames = list(fields or (list(rows[0]) if rows else []))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_counter_csv(path: Path, counts: Counter[Any], fields: Sequence[str]) -> None:
    rows = []
    for key, count in sorted(counts.items(), key=lambda item: str(item[0])):
        values = key if isinstance(key, tuple) else (key,)
        rows.append(dict(zip([*fields[:-1], fields[-1]], [*values, count], strict=True)))
    _write_csv(path, rows, fields)


def _group_members(
    rows: Sequence[Mapping[str, Any]], episode_field: str = "episode_id"
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        grouped[str(row[episode_field])].append(str(row["event_record_id"]))
    return {key: sorted(value) for key, value in grouped.items()}


def _index_optional(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    return {str(row["episode_id"]): row for row in _read_rows(path)}


def _group_optional(path: Path | None) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if path:
        for row in _read_rows(path):
            grouped[str(row["episode_id"])].append(row)
    return dict(grouped)


def _value(row: Mapping[str, Any] | None, field: str) -> Any:
    return row.get(field) if row else None


def _int_value(row: Mapping[str, Any] | None, field: str) -> int:
    value = row.get(field) if row else None
    return int(value) if value is not None else 0


def _first_value(row: Mapping[str, Any] | None, *fields: str) -> Any:
    if row:
        for field in fields:
            if field in row:
                return row[field]
    return None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _size_bin(value: int) -> str:
    if value == 1:
        return "1"
    if value <= SMALL_SIZE_MAX:
        return "2-5"
    if value <= MEDIUM_SIZE_MAX:
        return "6-25"
    if value <= LARGE_SIZE_MAX:
        return "26-100"
    return "101+"


def _parse_split_groups(value: str) -> list[list[str]]:
    groups = [sorted(item for item in group.split(";") if item) for group in value.split("|")]
    if len(groups) < MIN_SPLIT_GROUP_COUNT or any(not group for group in groups):
        raise CatalogError("Split decisions require two or more complete member groups.")
    return groups


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha256_dataset(path: Path) -> str:
    if path.is_file():
        return _sha256_file(path)
    digest = hashlib.sha256()
    for item in sorted(path.glob("*.parquet")):
        digest.update(item.name.encode())
        digest.update(item.read_bytes())
    return digest.hexdigest()


def _input_hashes(inputs: CatalogInputs) -> dict[str, str]:
    sources: dict[str, Path | None] = {
        "eligible_events": inputs.eligible_events,
        "event_states": inputs.event_states,
        "cohort_summary": inputs.cohort_summary,
        "conservative_episodes": inputs.conservative_episodes,
        "conservative_membership": inputs.conservative_membership,
        "conservative_edges": inputs.conservative_edges,
        "conservative_summary": inputs.conservative_summary,
        "conservative_manifest": inputs.conservative_manifest,
        "conservative_bridge_diagnostics": inputs.conservative_bridge_diagnostics,
        "conservative_split_diagnostics": inputs.conservative_split_diagnostics,
        "balanced_episodes": inputs.balanced_episodes,
        "balanced_membership": inputs.balanced_membership,
        "balanced_edges": inputs.balanced_edges,
        "balanced_summary": inputs.balanced_summary,
        "balanced_manifest": inputs.balanced_manifest,
        "clustering_sensitivity": inputs.clustering_sensitivity,
        "clustering_agreement": inputs.clustering_agreement,
        "mrms_episode_metrics": inputs.mrms_episode_metrics,
        "mrms_member_metrics": inputs.mrms_member_metrics,
        "mrms_coherence": inputs.mrms_coherence,
        "usgs_associations": inputs.usgs_associations,
        "usgs_gauge_metrics": inputs.usgs_gauge_metrics,
        "usgs_episode_summary": inputs.usgs_episode_summary,
        "usgs_request_manifest": inputs.usgs_request_manifest,
    }
    return {
        name: _sha256_dataset(path) for name, path in sorted(sources.items()) if path is not None
    }
