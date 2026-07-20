from __future__ import annotations

import csv
import hashlib
import json
import math
import shutil
import statistics
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from geodemand import __version__

Backend = Literal["official_api", "manual_csv", "classic_web_experimental"]
PARSER_VERSION = "0.8A-v1"
MAX_TARGETS_PER_BATCH = 4
SMALL_FOOTPRINT_KM2 = 25.0
COMPACT_FOOTPRINT_PROXY_KM2 = 10.0
MAX_INTEREST = 100.0
MIN_CORRELATION_VALUES = 2
MINI_PILOT_EPISODE_COUNT = 5
STANDARDIZED_BATCH_COUNT = 5
CONCEPTS_PER_BATCH = 5
MINI_PILOT_MINIMUM_YEARS = 3
MINI_PILOT_MINIMUM_STATES = 3
MINI_PILOT_MINIMUM_MEMBER_CLASSES = 2
TERMINOLOGY_TIERS = {
    "primary_broad",
    "secondary_specific",
    "exploratory_low_volume",
    "context",
    "anchor_candidate",
}
PROXY_TYPES = {
    "retailer_brand",
    "preparation_product",
    "damage_mitigation_product",
}
VALID_STATES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
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
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
}
REGIONS = {
    **dict.fromkeys(("CT", "ME", "MA", "NH", "RI", "VT", "NJ", "NY", "PA"), "northeast"),
    **dict.fromkeys(
        ("IN", "IL", "MI", "OH", "WI", "IA", "KS", "MN", "MO", "NE", "ND", "SD"),
        "midwest",
    ),
    **dict.fromkeys(
        (
            "DE",
            "DC",
            "FL",
            "GA",
            "MD",
            "NC",
            "SC",
            "VA",
            "WV",
            "AL",
            "KY",
            "MS",
            "TN",
            "AR",
            "LA",
            "OK",
            "TX",
        ),
        "south",
    ),
    **dict.fromkeys(
        ("AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY", "AK", "CA", "HI", "OR", "WA"),
        "west",
    ),
}
COASTAL = {
    "AK",
    "AL",
    "CA",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "LA",
    "ME",
    "MD",
    "MA",
    "MS",
    "NH",
    "NJ",
    "NY",
    "NC",
    "OR",
    "RI",
    "SC",
    "TX",
    "VA",
    "WA",
}


class TrendsError(ValueError):
    """Raised when a Trends feasibility stage cannot continue."""


class TrendsSidecar(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    backend: Backend
    requested_concepts: list[str] = Field(min_length=1, max_length=5)
    requested_labels: list[str] = Field(min_length=1, max_length=5)
    query_types: list[Literal["term", "topic"]] = Field(min_length=1, max_length=5)
    semantic_families: list[str] = Field(min_length=1, max_length=5)
    geography: str
    geography_level: str
    start_date: date
    end_date: date
    category: int | str
    search_property: str
    export_date: date
    source_filename: str
    episode_id: str
    request_role: str = "treated_state"
    treated_geography: str | None = None
    batch_id: str | None = None
    comparison_batch_id: str | None = None
    anchor_concept_id: str | None = None
    repeat_id: str | None = None
    notes: str = ""
    terminology_version: str = "unknown_legacy"

    @model_validator(mode="after")
    def validate_parallel_fields(self) -> TrendsSidecar:
        lengths = {
            len(self.requested_concepts),
            len(self.requested_labels),
            len(self.query_types),
            len(self.semantic_families),
        }
        if len(lengths) != 1:
            raise ValueError("Concept, label, query-type, and family lists must align.")
        if self.backend != "manual_csv":
            raise ValueError("CSV imports require backend=manual_csv.")
        if self.end_date < self.start_date:
            raise ValueError("Sidecar end_date precedes start_date.")
        return self


def load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TrendsError(f"Expected a YAML mapping in {path}.")
    return payload


def load_terms_config(path: Path) -> dict[str, Any]:  # noqa: PLR0912
    payload = load_yaml(path)
    if not payload.get("dictionary_version"):
        raise TrendsError("Terminology dictionary requires dictionary_version.")
    concepts = cast(list[dict[str, Any]], payload.get("concepts", []))
    required = {
        "concept_id",
        "query_text",
        "query_type",
        "semantic_family",
        "terminology_tier",
        "intended_response_stage",
        "expected_volume",
        "active",
        "anchor_candidate",
        "context_candidate",
        "exclusion_reason",
        "version",
    }
    for concept in concepts:
        missing = sorted(required - set(concept))
        if missing:
            raise TrendsError(
                f"Concept {concept.get('concept_id', '<unknown>')} is missing: "
                + ", ".join(missing)
            )
        if concept["terminology_tier"] not in TERMINOLOGY_TIERS:
            raise TrendsError(
                f"Invalid terminology tier for {concept['concept_id']}: "
                f"{concept['terminology_tier']}"
            )
        if bool(concept["anchor_candidate"]) == bool(concept["context_candidate"]) and concept[
            "terminology_tier"
        ] in {"anchor_candidate", "context"}:
            raise TrendsError(
                f"Control concept {concept['concept_id']} must be anchor or context, not both."
            )
        if concept["semantic_family"] == "behavioral_demand_proxy":
            proxy_required = {
                "proxy_type",
                "interpretation_scope",
                "promotion_sensitive",
                "seasonal_sensitive",
                "ambiguity_notes",
            }
            proxy_missing = sorted(proxy_required - set(concept))
            if proxy_missing:
                raise TrendsError(
                    f"Behavioral proxy {concept['concept_id']} is missing: "
                    + ", ".join(proxy_missing)
                )
            if concept["proxy_type"] not in PROXY_TYPES:
                raise TrendsError(
                    f"Invalid proxy type for {concept['concept_id']}: {concept['proxy_type']}"
                )
    concept_ids = [str(row["concept_id"]) for row in concepts]
    if len(concept_ids) != len(set(concept_ids)):
        raise TrendsError("Terminology concept IDs must be unique.")
    known = set(concept_ids)
    batches = cast(list[dict[str, Any]], payload.get("standardized_batches", []))
    if len(batches) != STANDARDIZED_BATCH_COUNT:
        raise TrendsError("Exactly five standardized terminology batches are required.")
    for batch in batches:
        members = [str(item) for item in cast(Sequence[Any], batch.get("concepts", []))]
        if len(members) != CONCEPTS_PER_BATCH or len(members) != len(set(members)):
            raise TrendsError(f"Batch {batch.get('batch_id')} must contain five unique concepts.")
        unknown = sorted(set(members) - known)
        if unknown:
            raise TrendsError(f"Batch {batch.get('batch_id')} has unknown concepts: {unknown}")
    national_batches = cast(list[dict[str, Any]], payload.get("national_comparison_batches", []))
    for batch in national_batches:
        members = [str(item) for item in cast(Sequence[Any], batch.get("concepts", []))]
        if not members or len(members) > CONCEPTS_PER_BATCH:
            raise TrendsError(
                f"National batch {batch.get('batch_id')} must contain one to five concepts."
            )
        unknown = sorted(set(members) - known)
        if unknown:
            raise TrendsError(
                f"National batch {batch.get('batch_id')} has unknown concepts: {unknown}"
            )
    pairs = cast(list[dict[str, Any]], payload.get("broad_specific_pairs", []))
    for pair in pairs:
        unknown_pair = {
            str(pair.get("broad_concept_id")),
            str(pair.get("specific_concept_id")),
        } - known
        if unknown_pair:
            raise TrendsError(f"Comparison pair {pair.get('pair_id')} has unknown concepts.")
    return payload


def inspect_trends(catalog_dir: Path, trends_root: Path) -> dict[str, Any]:
    episodes = catalog_dir / "episodes.parquet"
    if not episodes.exists():
        raise TrendsError(f"Catalog episodes are missing: {episodes}")
    return {
        "catalog_episode_count": _row_count(episodes),
        "pilot_exists": (trends_root / "manifests" / "trends_pilot_episodes.parquet").exists(),
        "geography_mapping_exists": (
            trends_root / "geography" / "geography_mapping.parquet"
        ).exists(),
        "request_plan_exists": (trends_root / "planning" / "trends_request_plan.parquet").exists(),
        "observations_exist": (trends_root / "processed" / "trends_observations.parquet").exists(),
        "raw_csv_count": len(list((trends_root / "raw").glob("*.csv")))
        if (trends_root / "raw").exists()
        else 0,
    }


def official_api_selfcheck(config_path: Path | None = None) -> dict[str, Any]:
    configured = config_path is not None and config_path.exists()
    return {
        "official_api_status": "unavailable_or_not_authorized",
        "configuration_exists": configured,
        "credentials_available": False,
        "authentication_succeeds": False,
        "documented_endpoint_reachable": False,
        "accessible_date_range": "rolling_five_year_window_if_authorized",
        "supported_temporal_aggregations": ["daily", "weekly", "monthly", "yearly"],
        "supported_geography_levels": ["country", "subregion", "additional_levels_may_vary"],
        "consistently_scaled_data": "available_in_alpha_if_authorized",
        "network_attempted": False,
        "reason": (
            "No public endpoint or authentication contract is configured; no URL was invented."
        ),
    }


def pilot_sample(catalog_dir: Path, output_root: Path, rules_path: Path) -> dict[str, Path]:
    rules = load_yaml(rules_path)
    episodes_path = catalog_dir / "episodes.parquet"
    if not episodes_path.exists():
        raise TrendsError(f"Catalog episodes are missing: {episodes_path}")
    rows = [row for row in _read_rows(episodes_path) if _pilot_eligible(row, rules)]
    per_year = int(rules["pilot_per_year"])
    seed = int(rules["selection_seed"])
    selected: list[dict[str, Any]] = []
    for year in [int(item) for item in cast(Sequence[Any], rules["pilot_years"])]:
        candidates = [row for row in rows if int(str(row["start_date"])[:4]) == year]
        if len(candidates) < per_year:
            raise TrendsError(f"Only {len(candidates)} eligible episodes exist for {year}.")
        selected.extend(_select_diverse(candidates, per_year, seed))
    required_states = [
        str(item).upper() for item in cast(Sequence[Any], rules.get("required_states", []))
    ]
    minimum_required = int(rules.get("minimum_required_state_episodes", 1))
    selected, replacements = _enforce_required_states(
        selected,
        rows,
        required_states,
        minimum_required,
        seed,
    )
    output = output_root / "manifests"
    output.mkdir(parents=True, exist_ok=True)
    pilot_rows = []
    for rank, row in enumerate(selected, 1):
        state = str(row["primary_state"])
        pilot_rows.append(
            {
                "provisional_episode_id": str(row["provisional_episode_id"]),
                "start_date": str(row["start_date"]),
                "end_date": str(row["end_date"]),
                "candidate_split": str(row["candidate_split"]),
                "primary_state": state,
                "states": str(row["states"]),
                "representative_latitude": float(row["representative_latitude"]),
                "representative_longitude": float(row["representative_longitude"]),
                "member_count": int(row["member_count"]),
                "duration_days": int(row["duration_days"]),
                "selection_stratum": _selection_stratum(row),
                "selection_rank": rank,
                "selection_seed": seed,
                "geography_mapping_status": "mapped" if state in VALID_STATES else "manual_review",
                "manual_review_flag": bool(row["manual_review_flag"]),
            }
        )
    pilot_path = output / "trends_pilot_episodes.parquet"
    _write_parquet(pilot_path, pilot_rows)
    years = Counter(str(row["start_date"])[:4] for row in pilot_rows)
    summary = {
        "pilot_episode_count": len(pilot_rows),
        "eligible_episode_count": len(rows),
        "episodes_by_year": dict(sorted(years.items())),
        "state_count": len({row["primary_state"] for row in pilot_rows}),
        "region_count": len(
            {REGIONS.get(str(row["primary_state"]), "unknown") for row in pilot_rows}
        ),
        "selection_seed": seed,
        "required_states": required_states,
        "minimum_required_state_episodes": minimum_required,
        "required_state_counts": {
            state: sum(str(row["primary_state"]) == state for row in pilot_rows)
            for state in required_states
        },
        "replacement_count": len(replacements),
    }
    summary_path = output / "trends_pilot_summary.json"
    _write_json(summary_path, summary)
    manifest_path = output / "trends_pilot_manifest.json"
    _write_json(
        manifest_path,
        {
            "package_version": __version__,
            "rules_sha256": _sha256_file(rules_path),
            "catalog_episodes_sha256": _sha256_file(episodes_path),
            "pilot_sha256": _sha256_file(pilot_path),
            "summary_sha256": _sha256_file(summary_path),
            "required_state_inclusion": {
                "required_states": required_states,
                "minimum_per_state": minimum_required,
                "replacements": replacements,
                "eligibility_relaxations": [],
            },
        },
    )
    overall_manifest = output / "manifest.json"
    _write_json(
        overall_manifest,
        {
            "package_version": __version__,
            "stage": "pilot_generated",
            "catalog_episodes_sha256": _sha256_file(episodes_path),
            "rules_sha256": _sha256_file(rules_path),
            "pilot_sha256": _sha256_file(pilot_path),
        },
    )
    return {
        "pilot": pilot_path,
        "summary": summary_path,
        "manifest": manifest_path,
        "overall_manifest": overall_manifest,
    }


def map_geographies(pilot_path: Path, output_root: Path) -> dict[str, Path]:
    mappings = []
    reviews = []
    for row in _read_rows(pilot_path):
        primary = str(row.get("primary_state", "")).upper()
        states = sorted({item for item in str(row.get("states", "")).split(";") if item})
        valid = primary in VALID_STATES
        mapping = {
            "provisional_episode_id": row["provisional_episode_id"],
            "country_code": "US",
            "state_abbreviation": primary if valid else None,
            "iso_subdivision_code": f"US-{primary}" if valid else None,
            "trends_geography_identifier": f"US-{primary}" if valid else None,
            "metro_dma_identifier": None,
            "geography_level": "state" if valid else "unavailable",
            "mapping_method": "catalog_primary_state" if valid else "manual_review",
            "mapping_confidence": "high"
            if valid and len(states) <= 1
            else "moderate"
            if valid
            else "none",
            "multi_state_flag": len(states) > 1,
            "secondary_states": ";".join(item for item in states if item != primary),
            "fallback_reason": "multistate_primary_state_only"
            if len(states) > 1
            else ""
            if valid
            else "invalid_or_missing_primary_state",
            "geography_mapping_status": "mapped" if valid else "manual_review",
        }
        mappings.append(mapping)
        if not valid:
            reviews.append(mapping)
    output = output_root / "geography"
    mapping_path = output / "geography_mapping.parquet"
    review_path = output / "geography_mapping_review.csv"
    summary_path = output / "geography_mapping_summary.json"
    _write_parquet(mapping_path, mappings)
    _write_csv(review_path, reviews, list(mappings[0]) if mappings else [])
    _write_json(
        summary_path,
        {
            "episode_count": len(mappings),
            "mapped_count": sum(row["geography_mapping_status"] == "mapped" for row in mappings),
            "manual_review_count": len(reviews),
            "multistate_count": sum(bool(row["multi_state_flag"]) for row in mappings),
            "state_count": len(
                {row["state_abbreviation"] for row in mappings if row["state_abbreviation"]}
            ),
        },
    )
    return {"mapping": mapping_path, "review": review_path, "summary": summary_path}


def plan_requests(  # noqa: PLR0912, PLR0915
    pilot_path: Path,
    geography_path: Path,
    terms_path: Path,
    rules_path: Path,
    output_root: Path,
    backend: Backend = "manual_csv",
    include_behavioral_state: bool = False,
    include_national: bool = False,
    controls_path: Path | None = None,
) -> dict[str, Path]:
    rules = load_yaml(rules_path)
    terms = load_terms_config(terms_path)
    concepts = [row for row in cast(list[dict[str, Any]], terms["concepts"]) if row["active"]]
    concept_by_id = {str(row["concept_id"]): row for row in concepts}
    batches = cast(list[dict[str, Any]], terms["standardized_batches"])
    terminology_version = str(terms["dictionary_version"])
    request_rules = cast(Mapping[str, Any], rules["request"])
    context_id = str(request_rules["comparison_context_concept_id"])
    normalization_anchor = request_rules.get("normalization_anchor_concept_id")
    if context_id not in concept_by_id or not concept_by_id[context_id]["context_candidate"]:
        raise TrendsError(f"Configured comparison context is invalid: {context_id}")
    geography = {str(row["provisional_episode_id"]): row for row in _read_rows(geography_path)}
    plans: list[dict[str, Any]] = []
    pilot_rows = _read_rows(pilot_path)
    for episode in sorted(pilot_rows, key=lambda row: str(row["provisional_episode_id"])):
        episode_id = str(episode["provisional_episode_id"])
        mapping = geography.get(episode_id)
        if mapping is None:
            raise TrendsError(f"Geography mapping is missing for episode {episode_id}.")
        window = _request_window(
            _as_date(episode["start_date"]), _as_date(episode["end_date"]), rules
        )
        for batch_index, batch in enumerate(batches, 1):
            plans.append(
                _plan_row(
                    episode_id,
                    mapping,
                    window,
                    batch,
                    batch_index,
                    request_rules,
                    concept_by_id,
                    context_id,
                    normalization_anchor,
                    terminology_version,
                    backend,
                    "full_pilot",
                    "initial",
                )
            )
    output = output_root / "planning"
    parquet_path = output / "trends_request_plan.parquet"
    csv_path = output / "trends_request_plan.csv"
    summary_path = output / "trends_request_summary.json"
    instructions_path = output / "manual_export_instructions.md"
    mini_parquet_path = output / "terminology_mini_pilot_plan.parquet"
    mini_csv_path = output / "terminology_mini_pilot_plan.csv"
    mini_summary_path = output / "terminology_mini_pilot_summary.json"
    terminology_parquet_path = output / "trends_terminology.parquet"
    terminology_csv_path = output / "trends_terminology.csv"
    _write_parquet(parquet_path, plans)
    _write_csv(csv_path, plans)
    _write_parquet(terminology_parquet_path, concepts)
    _write_csv(terminology_csv_path, concepts)
    _write_json(
        summary_path,
        {
            "request_count": len(plans),
            "episode_count": len({row["episode_id"] for row in plans}),
            "maximum_group_count": max((int(row["group_count"]) for row in plans), default=0),
            "backend": backend,
            "comparison_context_concept": context_id,
            "normalization_anchor_concept": normalization_anchor,
            "standardized_batch_count": len(batches),
            "terminology_version": terminology_version,
            "active_concept_count": len(concepts),
        },
    )
    instructions_path.parent.mkdir(parents=True, exist_ok=True)
    instructions_path.write_text(_manual_instructions(), encoding="utf-8")
    mini_rows = []
    mini_episodes = _select_mini_episodes(pilot_rows)
    for episode in mini_episodes:
        episode_id = str(episode["provisional_episode_id"])
        mapping = geography[episode_id]
        window = _request_window(
            _as_date(episode["start_date"]), _as_date(episode["end_date"]), rules
        )
        for batch_index, batch in enumerate(batches, 1):
            row = _plan_row(
                episode_id,
                mapping,
                window,
                batch,
                batch_index,
                request_rules,
                concept_by_id,
                context_id,
                normalization_anchor,
                terminology_version,
                backend,
                "terminology_mini_pilot",
                "repeat_1",
            )
            mini_rows.append(row)
            if str(mapping["state_abbreviation"]) == "FL" and batch_index == 1:
                mini_rows.append({**row, **_repeat_filenames(str(row["request_id"]), "repeat_2")})
    comparison_batches = cast(list[dict[str, Any]], terms.get("national_comparison_batches", []))
    treated_comparison_rows = (
        _comparison_plan_rows(
            mini_episodes,
            geography,
            comparison_batches,
            rules,
            request_rules,
            concept_by_id,
            context_id,
            normalization_anchor,
            terminology_version,
            backend,
            "treated_state_comparison",
        )
        if include_behavioral_state
        else []
    )
    national_rows = (
        _comparison_plan_rows(
            mini_episodes,
            geography,
            comparison_batches,
            rules,
            request_rules,
            concept_by_id,
            context_id,
            normalization_anchor,
            terminology_version,
            backend,
            "national_comparison",
        )
        if include_national
        else []
    )
    control_rows = (
        _control_plan_rows(
            mini_episodes,
            controls_path,
            comparison_batches,
            rules,
            request_rules,
            concept_by_id,
            context_id,
            normalization_anchor,
            terminology_version,
            backend,
        )
        if controls_path is not None
        else []
    )
    treated_comparison_path = output / "treated_state_comparison_plan.parquet"
    national_path = output / "national_comparison_plan.parquet"
    control_path = output / "control_state_request_plan.parquet"
    combined_event_study_path = output / "event_study_request_plan.parquet"
    event_summary_path = output / "event_study_request_summary.json"
    if treated_comparison_rows:
        _write_parquet(treated_comparison_path, treated_comparison_rows)
        _write_csv(treated_comparison_path.with_suffix(".csv"), treated_comparison_rows)
    if national_rows:
        _write_parquet(national_path, national_rows)
        _write_csv(national_path.with_suffix(".csv"), national_rows)
    if control_rows:
        _write_parquet(control_path, control_rows)
        _write_csv(control_path.with_suffix(".csv"), control_rows)
    optional_rows = [*treated_comparison_rows, *national_rows, *control_rows]
    if optional_rows:
        _write_parquet(combined_event_study_path, optional_rows)
        _write_csv(combined_event_study_path.with_suffix(".csv"), optional_rows)
    _write_json(
        event_summary_path,
        {
            "mini_pilot_episode_count": len(mini_episodes),
            "treated_request_count": len(treated_comparison_rows),
            "national_request_count": len(national_rows),
            "control_request_count": len(control_rows),
            "deduplicated_total_request_count": len(
                {row["request_id"] for row in [*mini_rows, *optional_rows]}
            ),
            "standard_mini_pilot_unique_request_count": len(
                {row["request_id"] for row in mini_rows}
            ),
            "optional_generation": True,
        },
    )
    _write_parquet(mini_parquet_path, mini_rows)
    _write_csv(mini_csv_path, mini_rows)
    _write_sidecar_templates(
        output / "sidecars", [*plans, *mini_rows, *optional_rows], concept_by_id
    )
    _write_json(
        mini_summary_path,
        {
            "request_count": len(mini_rows),
            "unique_request_definition_count": len({row["request_id"] for row in mini_rows}),
            "episode_count": len({row["episode_id"] for row in mini_rows}),
            "state_count": len({row["geography"] for row in mini_rows}),
            "year_count": len({str(row["event_start_date"])[:4] for row in mini_rows}),
            "standardized_batch_count": len(batches),
            "repeated_request_count": len(mini_rows)
            - len({row["request_id"] for row in mini_rows}),
            "florida_episode_count": len(
                {row["episode_id"] for row in mini_rows if row["geography"] == "US-FL"}
            ),
            "terminology_version": terminology_version,
            "execution_status": "manual_exports_pending",
        },
    )
    overall_manifest_path = output_root / "manifests" / "manifest.json"
    overall_manifest = (
        json.loads(overall_manifest_path.read_text(encoding="utf-8"))
        if overall_manifest_path.exists()
        else {"package_version": __version__}
    )
    overall_manifest.update(
        {
            "stage": "request_plans_generated",
            "terms_sha256": _sha256_file(terms_path),
            "rules_sha256": _sha256_file(rules_path),
            "request_plan_sha256": _sha256_file(parquet_path),
            "mini_pilot_plan_sha256": _sha256_file(mini_parquet_path),
        }
    )
    _write_json(overall_manifest_path, overall_manifest)
    result = {
        "parquet": parquet_path,
        "csv": csv_path,
        "summary": summary_path,
        "instructions": instructions_path,
        "mini_pilot_parquet": mini_parquet_path,
        "mini_pilot_csv": mini_csv_path,
        "mini_pilot_summary": mini_summary_path,
        "terminology_parquet": terminology_parquet_path,
        "terminology_csv": terminology_csv_path,
    }
    if treated_comparison_rows:
        result["treated_state_comparison"] = treated_comparison_path
    if national_rows:
        result["national_comparison"] = national_path
    if control_rows:
        result["control_state_requests"] = control_path
    if optional_rows:
        result["event_study_plan"] = combined_event_study_path
        result["event_study_summary"] = event_summary_path
    return result


def import_csv_export(
    csv_path: Path,
    sidecar_path: Path,
    output_root: Path,
) -> dict[str, Any]:
    if not csv_path.exists():
        raise TrendsError(f"Trends CSV is missing: {csv_path}")
    if not sidecar_path.exists():
        raise TrendsError(f"Required Trends sidecar is missing: {sidecar_path}")
    try:
        sidecar_payload = _load_sidecar(sidecar_path)
        sidecar = TrendsSidecar.model_validate(sidecar_payload)
    except (ValidationError, ValueError, yaml.YAMLError) as exc:
        raise TrendsError(f"Invalid Trends sidecar {sidecar_path}: {exc}") from exc
    if sidecar.source_filename != csv_path.name:
        raise TrendsError("Sidecar source_filename does not match the supplied CSV file.")
    raw_dir = output_root / "raw"
    _preserve_raw(csv_path, raw_dir / csv_path.name)
    _preserve_raw(sidecar_path, raw_dir / sidecar_path.name)
    header, raw_rows = _parse_export(csv_path)
    if len(header) - 1 != len(sidecar.requested_concepts):
        raise TrendsError("CSV series count does not match sidecar requested concepts.")
    for index, label in enumerate(sidecar.requested_labels, 1):
        if label.casefold() not in header[index].casefold():
            raise TrendsError(
                f"CSV label {header[index]!r} does not match sidecar label {label!r}."
            )
    repeat_id = sidecar.repeat_id or _sha256_file(csv_path)[:16]
    observations: list[dict[str, Any]] = []
    seen: set[tuple[str, date]] = set()
    for raw in raw_rows:
        observation_date = _parse_export_date(raw[0])
        for index, concept_id in enumerate(sidecar.requested_concepts):
            key = (concept_id, observation_date)
            if key in seen:
                raise TrendsError(f"Duplicate date for concept {concept_id}: {observation_date}")
            seen.add(key)
            interest, partial, zero_classification = _parse_interest(raw[index + 1])
            observations.append(
                {
                    "request_id": sidecar.request_id,
                    "repeat_id": repeat_id,
                    "episode_id": sidecar.episode_id,
                    "request_role": sidecar.request_role,
                    "treated_geography": sidecar.treated_geography or sidecar.geography,
                    "batch_id": sidecar.batch_id,
                    "comparison_batch_id": sidecar.comparison_batch_id or sidecar.batch_id,
                    "backend": sidecar.backend,
                    "source_file": sidecar.source_filename,
                    "concept_id": concept_id,
                    "query_text": sidecar.requested_labels[index],
                    "query_type": sidecar.query_types[index],
                    "semantic_family": sidecar.semantic_families[index],
                    "geography": sidecar.geography,
                    "geography_level": sidecar.geography_level,
                    "date": observation_date,
                    "interest": interest,
                    "is_partial": partial,
                    "zero_classification": zero_classification,
                    "category": str(sidecar.category),
                    "search_property": sidecar.search_property,
                    "anchor_concept_id": sidecar.anchor_concept_id,
                    "terminology_version": sidecar.terminology_version,
                    "imported_at": sidecar.export_date.isoformat(),
                    "parser_version": PARSER_VERSION,
                }
            )
    processed = output_root / "processed"
    output_path = processed / "trends_observations.parquet"
    existing = _read_rows(output_path) if output_path.exists() else []
    for row in existing:
        row.setdefault("terminology_version", "unknown_legacy")
    identity = {
        (row["request_id"], row["repeat_id"], row["concept_id"], str(row["date"]))
        for row in existing
    }
    additions = [
        row
        for row in observations
        if (row["request_id"], row["repeat_id"], row["concept_id"], str(row["date"]))
        not in identity
    ]
    _write_parquet(output_path, [*existing, *additions])
    manifest_dir = output_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "manifest.json"
    manifest = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.exists()
        else {"package_version": __version__}
    )
    manifest.update(
        {
            "last_import_timestamp_utc": datetime.now(UTC).isoformat(),
            "source_csv_sha256": _sha256_file(csv_path),
            "sidecar_sha256": _sha256_file(sidecar_path),
            "output_sha256": _sha256_file(output_path),
            "imported_row_count": len(additions),
            "terminology_versions": sorted(
                {str(row["terminology_version"]) for row in [*existing, *additions]}
            ),
        }
    )
    _write_json(manifest_path, manifest)
    return {"observations": output_path, "imported_rows": len(additions), "repeat_id": repeat_id}


def validate_imports(observations_path: Path, plan_path: Path | None = None) -> dict[str, Any]:
    if not observations_path.exists():
        raise TrendsError(f"Trends observations are missing: {observations_path}")
    rows = _read_rows(observations_path)
    identities = [
        (row["request_id"], row["repeat_id"], row["concept_id"], str(row["date"])) for row in rows
    ]
    planned = (
        {str(row["request_id"]) for row in _read_rows(plan_path)}
        if plan_path is not None
        else set()
    )
    imported = {str(row["request_id"]) for row in rows}
    unknown = sorted(imported - planned) if planned else []
    payload = {
        "valid": len(identities) == len(set(identities)) and not unknown,
        "observation_count": len(rows),
        "request_count": len(imported),
        "repeat_count": len({(row["request_id"], row["repeat_id"]) for row in rows}),
        "duplicate_observation_count": len(identities) - len(set(identities)),
        "unknown_request_ids": unknown,
        "partial_observation_count": sum(bool(row["is_partial"]) for row in rows),
    }
    if not payload["valid"]:
        raise TrendsError("Imported observations failed validation: " + _canonical_json(payload))
    return payload


def calculate_metrics(
    observations_path: Path,
    plan_path: Path,
    rules_path: Path,
    output_root: Path,
) -> dict[str, Path]:
    rules = load_yaml(rules_path)
    observations = _read_rows(observations_path)
    plans = {str(row["request_id"]): row for row in _read_rows(plan_path)}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    repeat_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        grouped[(str(row["request_id"]), str(row["concept_id"]))].append(row)
        repeat_groups[
            (str(row["request_id"]), str(row["concept_id"]), str(row["repeat_id"]))
        ].append(row)
    repeat_diagnostics = _repeat_diagnostics(repeat_groups, rules)
    repeat_lookup = {
        (str(row["request_id"]), str(row["concept_id"])): row for row in repeat_diagnostics
    }
    series_lookup = _mean_series(grouped)
    metric_rows: list[dict[str, Any]] = []
    suppression_rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        request_id, concept_id = key
        plan = plans.get(request_id)
        if plan is None:
            raise TrendsError(f"Imported request is absent from plan: {request_id}")
        rows = grouped[key]
        series = series_lookup[key]
        metric, suppression = _metric_row(rows, series, plan, rules, repeat_lookup.get(key))
        anchor_id = rows[0].get("anchor_concept_id")
        if anchor_id and concept_id != anchor_id:
            anchor = series_lookup.get((request_id, str(anchor_id)), {})
            metric["anchor_ratio_peak"] = _anchor_ratio_peak(series, anchor, rules)
            if metric["anchor_ratio_peak"] is None:
                metric["metric_quality_reasons"] = _join_reasons(
                    str(metric["metric_quality_reasons"]), "unstable_or_zero_anchor"
                )
        metric_rows.append(metric)
        suppression_rows.append(suppression)
    normalization_rows = _normalization_diagnostics(metric_rows, series_lookup, plans, rules)
    processed = output_root / "processed"
    diagnostics = output_root / "diagnostics"
    metrics_path = processed / "trends_episode_response_metrics.parquet"
    suppression_path = diagnostics / "suppression_diagnostics.csv"
    normalization_path = diagnostics / "normalization_diagnostics.csv"
    repeat_path = diagnostics / "repeat_stability.csv"
    _write_parquet(metrics_path, metric_rows)
    _write_csv(suppression_path, suppression_rows)
    _write_csv(normalization_path, normalization_rows)
    _write_csv(repeat_path, repeat_diagnostics)
    return {
        "metrics": metrics_path,
        "suppression": suppression_path,
        "normalization": normalization_path,
        "repeat_stability": repeat_path,
    }


def evaluate_terms(  # noqa: PLR0915
    metrics_path: Path,
    plan_path: Path,
    terms_path: Path,
    rules_path: Path,
    output_root: Path,
) -> dict[str, Path]:
    metrics = _read_rows(metrics_path)
    plans = _read_rows(plan_path)
    terms_config = load_terms_config(terms_path)
    terms = cast(list[dict[str, Any]], terms_config["concepts"])
    rules = load_yaml(rules_path)
    plan_by_request = {str(row["request_id"]): row for row in plans}
    metrics_by_concept: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in metrics:
        metrics_by_concept[str(row["concept_id"])].append(row)
    rows: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    thresholds = cast(Mapping[str, Any], rules["terminology"])
    normalization = cast(Mapping[str, Any], rules["normalization"])
    for term in sorted(terms, key=lambda row: str(row["concept_id"])):
        concept_id = str(term["concept_id"])
        concept_metrics = metrics_by_concept.get(concept_id, [])
        requested_plans = [row for row in plans if concept_id in str(row["concept_ids"]).split(";")]
        usable = [
            row for row in concept_metrics if str(row["metric_quality_status"]).startswith("usable")
        ]
        imported_count = len({row["provisional_episode_id"] for row in concept_metrics})
        requested_count = len({row["episode_id"] for row in requested_plans})
        usable_fraction = len(usable) / imported_count if imported_count else 0.0
        event_lifts = [
            (float(row["event_maximum"]) - float(row["baseline_mean"]))
            / float(row["baseline_mean"])
            for row in concept_metrics
            if row.get("event_maximum") is not None
            and row.get("baseline_mean") is not None
            and float(row["baseline_mean"]) > 0
        ]
        event_lift = _median(event_lifts)
        repeat_stability = _median(
            [1.0 if row["repeat_stability_flag"] else 0.0 for row in concept_metrics]
        )
        zero_fraction = _median([row["zero_fraction"] for row in concept_metrics])
        nonzero_coverage = 1.0 - zero_fraction if zero_fraction is not None else None
        baseline_sd = _median([row["baseline_standard_deviation"] for row in concept_metrics])
        baseline_stability = 1.0 / (1.0 + baseline_sd) if baseline_sd is not None else None
        seasonal_medians: dict[str, list[float]] = defaultdict(list)
        for row in concept_metrics:
            plan = plan_by_request.get(str(row["request_id"]))
            if plan and row.get("baseline_median") is not None:
                seasonal_medians[_season(_as_date(plan["event_start_date"]))].append(
                    float(row["baseline_median"])
                )
        seasonal_values = [statistics.fmean(values) for values in seasonal_medians.values()]
        seasonal_variability = (
            statistics.pstdev(seasonal_values) if len(seasonal_values) > 1 else None
        )
        event_sensitive = (
            bool(term["anchor_candidate"])
            and event_lift is not None
            and (event_lift >= float(normalization["event_sensitive_anchor_lift_fraction"]))
        )
        tier = str(term["terminology_tier"])
        classification = "insufficient_evidence"
        if not concept_metrics:
            classification = "insufficient_evidence"
        elif all(row["metric_quality_status"] == "all_zero" for row in concept_metrics):
            classification = "low_volume_only"
        elif event_sensitive:
            classification = "event_sensitive_anchor"
        elif bool(term["anchor_candidate"]):
            classification = (
                "anchor_candidate"
                if usable_fraction >= float(thresholds["secondary_minimum_usable_fraction"])
                else "insufficient_evidence"
            )
        elif bool(term["context_candidate"]):
            classification = (
                "retain_context_only"
                if usable_fraction >= float(thresholds["secondary_minimum_usable_fraction"])
                else "insufficient_evidence"
            )
        elif tier == "primary_broad" and usable_fraction >= float(
            thresholds["primary_minimum_usable_fraction"]
        ):
            classification = "retain_primary"
        elif tier in {"secondary_specific", "exploratory_low_volume"} and usable_fraction >= float(
            thresholds["secondary_minimum_usable_fraction"]
        ):
            classification = "retain_secondary"
        else:
            classification = "low_volume_only"
        geographies = {
            str(plan_by_request[str(row["request_id"])]["geography"])
            for row in concept_metrics
            if str(row["request_id"]) in plan_by_request
        }
        years = {
            str(plan_by_request[str(row["request_id"])]["event_start_date"])[:4]
            for row in concept_metrics
            if str(row["request_id"]) in plan_by_request
        }
        result = {
            "concept_id": concept_id,
            "semantic_family": term["semantic_family"],
            "terminology_tier": tier,
            "episode_count_requested": requested_count,
            "episode_count_imported": imported_count,
            "usable_episode_count": len(usable),
            "all_zero_count": sum(
                row["metric_quality_status"] == "all_zero" for row in concept_metrics
            ),
            "low_volume_count": sum(
                row["metric_quality_status"] == "low_volume" for row in concept_metrics
            ),
            "median_zero_fraction": zero_fraction,
            "nonzero_coverage": nonzero_coverage,
            "median_baseline_interest": _median(
                [row["baseline_median"] for row in concept_metrics]
            ),
            "median_event_maximum": _median([row["event_maximum"] for row in concept_metrics]),
            "baseline_stability": baseline_stability,
            "seasonal_variability": seasonal_variability,
            "event_period_lift": event_lift,
            "repeat_stability": repeat_stability,
            "anchor_compatibility": bool(term["anchor_candidate"]) and not event_sensitive,
            "state_coverage": len(geographies),
            "year_coverage": len(years),
            "season_coverage": len(
                {
                    _season(_as_date(plan_by_request[str(row["request_id"])]["event_start_date"]))
                    for row in concept_metrics
                    if str(row["request_id"]) in plan_by_request
                }
            ),
            "classification": classification,
        }
        rows.append(result)
        decisions.append(
            {
                "concept_id": concept_id,
                "decision": classification,
                "decision_reason": "deterministic_feasibility_thresholds",
                "reviewer": "",
                "review_notes": "",
            }
        )
    processed = output_root / "processed"
    summaries = output_root / "summaries"
    feasibility_path = processed / "terminology_feasibility.parquet"
    decisions_path = processed / "terminology_decisions.csv"
    pair_parquet_path = processed / "broad_specific_feasibility.parquet"
    pair_csv_path = processed / "broad_specific_feasibility.csv"
    summary_path = summaries / "terminology_summary.json"
    pair_rows = _broad_specific_rows(
        cast(list[dict[str, Any]], terms_config["broad_specific_pairs"]), metrics_by_concept
    )
    _write_parquet(feasibility_path, rows)
    _write_csv(decisions_path, decisions)
    _write_parquet(pair_parquet_path, pair_rows)
    _write_csv(pair_csv_path, pair_rows)
    _write_json(
        summary_path,
        {
            "concept_count": len(rows),
            "classifications": dict(
                sorted(Counter(str(row["classification"]) for row in rows).items())
            ),
            "imported_concept_count": sum(int(row["episode_count_imported"]) > 0 for row in rows),
            "comparison_pair_count": len(pair_rows),
            "terminology_version": terms_config["dictionary_version"],
        },
    )
    return {
        "feasibility": feasibility_path,
        "decisions": decisions_path,
        "broad_specific_parquet": pair_parquet_path,
        "broad_specific_csv": pair_csv_path,
        "summary": summary_path,
    }


def _broad_specific_rows(
    pairs: Sequence[Mapping[str, Any]],
    metrics_by_concept: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    output = []
    for pair in pairs:
        broad_id = str(pair["broad_concept_id"])
        specific_id = str(pair["specific_concept_id"])
        broad = {
            str(row["provisional_episode_id"]): row for row in metrics_by_concept.get(broad_id, [])
        }
        specific = {
            str(row["provisional_episode_id"]): row
            for row in metrics_by_concept.get(specific_id, [])
        }
        shared = sorted(set(broad) & set(specific))
        broad_usable = sum(
            str(broad[item]["metric_quality_status"]).startswith("usable") for item in shared
        )
        specific_usable = sum(
            str(specific[item]["metric_quality_status"]).startswith("usable") for item in shared
        )
        insufficient = not shared
        broad_preferred = bool(shared) and broad_usable > specific_usable
        specific_preferred = bool(shared) and specific_usable > broad_usable
        retain_both = bool(shared) and broad_usable == specific_usable and broad_usable > 0
        output.append(
            {
                "pair_id": pair["pair_id"],
                "broad_concept_id": broad_id,
                "specific_concept_id": specific_id,
                "compared_episode_count": len(shared),
                "usable_episode_count": sum(
                    str(broad[item]["metric_quality_status"]).startswith("usable")
                    and str(specific[item]["metric_quality_status"]).startswith("usable")
                    for item in shared
                ),
                "broad_usable_episode_count": broad_usable,
                "specific_usable_episode_count": specific_usable,
                "all_zero_episode_count": sum(
                    broad[item]["metric_quality_status"] == "all_zero"
                    or specific[item]["metric_quality_status"] == "all_zero"
                    for item in shared
                ),
                "broad_median_zero_fraction": _median(
                    [broad[item]["zero_fraction"] for item in shared]
                ),
                "specific_median_zero_fraction": _median(
                    [specific[item]["zero_fraction"] for item in shared]
                ),
                "broad_median_baseline_value": _median(
                    [broad[item]["baseline_median"] for item in shared]
                ),
                "specific_median_baseline_value": _median(
                    [specific[item]["baseline_median"] for item in shared]
                ),
                "broad_median_event_maximum": _median(
                    [broad[item]["event_maximum"] for item in shared]
                ),
                "specific_median_event_maximum": _median(
                    [specific[item]["event_maximum"] for item in shared]
                ),
                "event_lift_availability": sum(
                    broad[item].get("absolute_peak_lift") is not None
                    and specific[item].get("absolute_peak_lift") is not None
                    for item in shared
                ),
                "broad_repeat_stability": _median(
                    [1.0 if broad[item]["repeat_stability_flag"] else 0.0 for item in shared]
                ),
                "specific_repeat_stability": _median(
                    [1.0 if specific[item]["repeat_stability_flag"] else 0.0 for item in shared]
                ),
                "broad_term_preferred": broad_preferred,
                "specific_term_preferred": specific_preferred,
                "retain_both": retain_both,
                "insufficient_evidence": insufficient,
            }
        )
    return output


def assess_episodes(
    pilot_path: Path,
    geography_path: Path,
    plan_path: Path,
    metrics_path: Path | None,
    output_root: Path,
) -> dict[str, Path]:
    geography = {str(row["provisional_episode_id"]): row for row in _read_rows(geography_path)}
    plans_by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _read_rows(plan_path):
        plans_by_episode[str(row["episode_id"])].append(row)
    metrics_by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if metrics_path and metrics_path.exists():
        for row in _read_rows(metrics_path):
            metrics_by_episode[str(row["provisional_episode_id"])].append(row)
    rows = []
    for episode in _read_rows(pilot_path):
        episode_id = str(episode["provisional_episode_id"])
        mapping = geography[episode_id]
        plans = plans_by_episode.get(episode_id, [])
        metrics = metrics_by_episode.get(episode_id, [])
        usable = [row for row in metrics if str(row["metric_quality_status"]).startswith("usable")]
        status = "not_tested"
        reasons = []
        if mapping["geography_mapping_status"] != "mapped":
            status = "geography_unavailable"
            reasons.append("geography_mapping_review")
        elif metrics:
            if usable:
                status = "feasible_with_state_level_limitations"
                reasons.append("state_level_not_event_footprint")
            elif all(row["metric_quality_status"] == "all_zero" for row in metrics):
                status = "low_volume"
                reasons.append("all_imported_series_zero")
            else:
                status = "insufficient_data"
                reasons.append("no_usable_concepts")
        else:
            reasons.append("manual_exports_not_imported")
        rows.append(
            {
                "provisional_episode_id": episode_id,
                "geography_mapping_status": mapping["geography_mapping_status"],
                "request_count": len(plans),
                "imported_request_count": len({row["request_id"] for row in metrics}),
                "usable_concept_count": len(usable),
                "low_volume_concept_count": sum(
                    row["metric_quality_status"] == "low_volume" for row in metrics
                ),
                "suppressed_concept_count": sum(bool(row["suppression_flag"]) for row in metrics),
                "stable_anchor_available": any(
                    row["anchor_ratio_peak"] is not None for row in metrics
                ),
                "repeat_stability_available": any(row["repeat_stability_flag"] for row in metrics),
                "best_awareness_concept": _best_concept(usable, "flood_awareness"),
                "best_immediate_need_concept": _best_concept(usable, "immediate_safety"),
                "best_recovery_concept": _best_concept(
                    usable, "damage_cleanup", "assistance_recovery"
                ),
                "episode_trends_status": status,
                "review_reasons": ";".join(reasons),
            }
        )
    output_path = output_root / "processed" / "trends_episode_feasibility.parquet"
    summary_path = output_root / "summaries" / "trends_feasibility_summary.json"
    _write_parquet(output_path, rows)
    _write_json(
        summary_path,
        {
            "episode_count": len(rows),
            "status_counts": dict(
                sorted(Counter(str(row["episode_trends_status"]) for row in rows).items())
            ),
            "imported_episode_count": sum(int(row["imported_request_count"]) > 0 for row in rows),
            "usable_episode_count": sum(int(row["usable_concept_count"]) > 0 for row in rows),
        },
    )
    return {"feasibility": output_path, "summary": summary_path}


def write_quicklooks(
    observations_path: Path,
    metrics_path: Path,
    plan_path: Path,
    output_root: Path,
) -> list[Path]:
    observations = _read_rows(observations_path)
    metrics = _read_rows(metrics_path)
    plans = {str(row["request_id"]): row for row in _read_rows(plan_path)}
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        groups[(str(row["request_id"]), str(row["concept_id"]))].append(row)
    selectors: dict[str, Callable[[Mapping[str, Any]], float]] = {
        "strong_response": lambda row: float(row.get("absolute_peak_lift") or -math.inf),
        "low_volume": lambda row: float(row.get("zero_fraction") or 0.0),
        "all_zero": lambda row: 1.0 if row.get("metric_quality_status") == "all_zero" else 0.0,
        "unstable_repeat": lambda row: 1.0 if not row.get("repeat_stability_flag") else 0.0,
        "anchor_normalization": lambda row: (
            1.0 if row.get("anchor_ratio_peak") is not None else 0.0
        ),
        "awareness_recovery": lambda row: (
            1.0
            if row.get("semantic_family")
            in {"flood_awareness", "damage_cleanup", "assistance_recovery"}
            else 0.0
        ),
        "state_geography_limit": lambda row: 1.0,
    }
    output = output_root / "quicklooks"
    output.mkdir(parents=True, exist_ok=True)
    paths = []
    for label, score in selectors.items():
        if not metrics:
            break
        selected = max(
            metrics,
            key=lambda row: (
                score(row),
                str(row["provisional_episode_id"]),
                str(row["concept_id"]),
            ),
        )
        key = (str(selected["request_id"]), str(selected["concept_id"]))
        series = sorted(groups.get(key, []), key=lambda row: str(row["date"]))
        path = output / f"{label}.svg"
        path.write_text(
            _quicklook_svg(label, series, selected, plans.get(key[0])), encoding="utf-8"
        )
        paths.append(path)
    return paths


def summarize_trends(output_root: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    paths = {
        "pilot": output_root / "manifests" / "trends_pilot_summary.json",
        "geography": output_root / "geography" / "geography_mapping_summary.json",
        "requests": output_root / "planning" / "trends_request_summary.json",
        "terminology": output_root / "summaries" / "terminology_summary.json",
        "feasibility": output_root / "summaries" / "trends_feasibility_summary.json",
    }
    for name, path in paths.items():
        summary[name] = (
            json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"status": "pending"}
        )
    observations = output_root / "processed" / "trends_observations.parquet"
    summary["manual_exports_imported"] = (
        len({row["source_file"] for row in _read_rows(observations)})
        if observations.exists()
        else 0
    )
    summary["milestone_acceptance_status"] = "evidence_pending"
    _write_json(output_root / "summaries" / "trends_overall_summary.json", summary)
    return summary


def _pilot_eligible(row: Mapping[str, Any], rules: Mapping[str, Any]) -> bool:
    eligibility = cast(Mapping[str, Any], rules["eligibility"])
    state = str(row.get("primary_state", ""))
    start = str(row.get("start_date", ""))
    return (
        str(row.get("provisional_catalog_status")) in eligibility["statuses"]
        and not bool(row.get("manual_review_flag"))
        and not bool(row.get("split_boundary_flag"))
        and int(row.get("duration_days", 999)) <= int(eligibility["maximum_duration_days"])
        and int(row.get("member_count", 999)) <= int(eligibility["maximum_member_count"])
        and state in VALID_STATES
        and "2022-01-01" <= start <= "2025-12-31"
    )


def _select_diverse(rows: Sequence[dict[str, Any]], count: int, seed: int) -> list[dict[str, Any]]:
    remaining = sorted(rows, key=lambda row: _selection_hash(row, seed))
    selected: list[dict[str, Any]] = []
    represented: Counter[str] = Counter()
    while len(selected) < count:
        scored = []
        for row in remaining:
            dimensions = _selection_dimensions(row)
            novelty = sum(1 / (1 + represented[item]) for item in dimensions)
            scored.append((novelty, _selection_hash(row, seed), row))
        _, _, chosen = max(scored, key=lambda item: (item[0], item[1]))
        selected.append(chosen)
        remaining.remove(chosen)
        represented.update(_selection_dimensions(chosen))
    return selected


def _enforce_required_states(
    selected: list[dict[str, Any]],
    eligible: Sequence[dict[str, Any]],
    required_states: Sequence[str],
    minimum_per_state: int,
    seed: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if minimum_per_state < 1:
        raise TrendsError("minimum_required_state_episodes must be at least one.")
    result = list(selected)
    replacements: list[dict[str, Any]] = []
    selected_ids = {str(row["provisional_episode_id"]) for row in result}
    for state in required_states:
        candidates = [row for row in eligible if str(row["primary_state"]) == state]
        if len(candidates) < minimum_per_state:
            raise TrendsError(
                f"Required state {state} has only {len(candidates)} eligible episodes; "
                f"{minimum_per_state} are required."
            )
        while sum(str(row["primary_state"]) == state for row in result) < minimum_per_state:
            existing = [row for row in result if str(row["primary_state"]) == state]
            available = [
                row for row in candidates if str(row["provisional_episode_id"]) not in selected_ids
            ]
            if not available:
                raise TrendsError(f"No unused eligible episodes remain for required state {state}.")
            state_counts = Counter(str(row["primary_state"]) for row in result)
            pairs: list[tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]] = []
            need_compact = not any(_is_compact_proxy(row) for row in existing)
            for candidate in available:
                candidate_year = str(candidate["start_date"])[:4]
                for removed in result:
                    removed_state = str(removed["primary_state"])
                    if removed_state in required_states:
                        continue
                    if str(removed["start_date"])[:4] != candidate_year:
                        continue
                    matched = _matched_replacement_attributes(candidate, removed)
                    exact_balance = all(
                        item in matched
                        for item in ("coastal_class", "member_class", "footprint_class")
                    )
                    diversity = sum(
                        int(str(candidate["start_date"])[:4] != str(item["start_date"])[:4])
                        + int(
                            _season(_as_date(candidate["start_date"]))
                            != _season(_as_date(item["start_date"]))
                        )
                        + int(_member_class(candidate) != _member_class(item))
                        for item in existing
                    )
                    preferred_removal = (
                        2 if removed_state == "TX" else 1 if removed_state == "NY" else 0
                    )
                    score = (
                        int(not need_compact or _is_compact_proxy(candidate)),
                        int(exact_balance),
                        diversity,
                        len(matched),
                        state_counts[removed_state],
                        preferred_removal,
                        _selection_hash(candidate, seed),
                        _selection_hash(removed, seed),
                    )
                    pairs.append((score, candidate, removed))
            if not pairs:
                raise TrendsError(
                    f"Required state {state} cannot be inserted while preserving year quotas."
                )
            _, candidate, removed = max(pairs, key=lambda item: item[0])
            index = result.index(removed)
            result[index] = candidate
            selected_ids.remove(str(removed["provisional_episode_id"]))
            selected_ids.add(str(candidate["provisional_episode_id"]))
            replacements.append(
                {
                    "required_state": state,
                    "added_episode_id": str(candidate["provisional_episode_id"]),
                    "removed_episode_id": str(removed["provisional_episode_id"]),
                    "removed_state": str(removed["primary_state"]),
                    "year": str(candidate["start_date"])[:4],
                    "matched_attributes": _matched_replacement_attributes(candidate, removed),
                    "added_stratum": _selection_stratum(candidate),
                    "removed_stratum": _selection_stratum(removed),
                }
            )
    return result, replacements


def _matched_replacement_attributes(
    candidate: Mapping[str, Any], removed: Mapping[str, Any]
) -> list[str]:
    comparisons = {
        "year": str(candidate["start_date"])[:4] == str(removed["start_date"])[:4],
        "season": _season(_as_date(candidate["start_date"]))
        == _season(_as_date(removed["start_date"])),
        "coastal_class": _coastal_class(candidate) == _coastal_class(removed),
        "member_class": _member_class(candidate) == _member_class(removed),
        "footprint_class": _footprint_class(candidate) == _footprint_class(removed),
        "urban_proxy": _is_compact_proxy(candidate) == _is_compact_proxy(removed),
    }
    return [name for name, matched in comparisons.items() if matched]


def _coastal_class(row: Mapping[str, Any]) -> str:
    return "coastal" if str(row["primary_state"]) in COASTAL else "inland"


def _member_class(row: Mapping[str, Any]) -> str:
    return "singleton" if int(row["member_count"]) == 1 else "multi_member"


def _footprint_class(row: Mapping[str, Any]) -> str:
    return "small" if float(row["union_area_km2"]) <= SMALL_FOOTPRINT_KM2 else "moderate"


def _is_compact_proxy(row: Mapping[str, Any]) -> bool:
    return float(row["union_area_km2"]) <= COMPACT_FOOTPRINT_PROXY_KM2


def _selection_dimensions(row: Mapping[str, Any]) -> list[str]:
    state = str(row["primary_state"])
    start = _as_date(row["start_date"])
    area = float(row["union_area_km2"])
    return [
        f"season:{_season(start)}",
        f"region:{REGIONS.get(state, 'unknown')}",
        f"state:{state}",
        f"coast:{'coastal' if state in COASTAL else 'inland'}",
        f"members:{'singleton' if int(row['member_count']) == 1 else 'multi_member'}",
        f"footprint:{'small' if area <= SMALL_FOOTPRINT_KM2 else 'moderate'}",
        f"urban_proxy:{'compact' if area <= COMPACT_FOOTPRINT_PROXY_KM2 else 'noncompact'}",
    ]


def _selection_stratum(row: Mapping[str, Any]) -> str:
    return ";".join(_selection_dimensions(row))


def _selection_hash(row: Mapping[str, Any], seed: int) -> str:
    return hashlib.sha256(f"{seed}:{row['provisional_episode_id']}".encode()).hexdigest()


def _select_mini_episodes(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    remaining = sorted(rows, key=lambda row: int(row["selection_rank"]))
    florida = [row for row in remaining if str(row["primary_state"]) == "FL"]
    if not florida:
        raise TrendsError("The terminology mini-pilot requires a Florida episode.")
    selected: list[dict[str, Any]] = [florida[0]]
    remaining.remove(florida[0])
    years = {str(florida[0]["start_date"])[:4]}
    states = {"FL"}
    member_classes = {_member_class(florida[0])}
    while len(selected) < MINI_PILOT_EPISODE_COUNT:
        chosen = max(
            remaining,
            key=lambda row: (
                int(str(row["start_date"])[:4] not in years)
                + int(str(row["primary_state"]) not in states)
                + int(_member_class(row) not in member_classes),
                -int(row["selection_rank"]),
            ),
        )
        selected.append(chosen)
        remaining.remove(chosen)
        years.add(str(chosen["start_date"])[:4])
        states.add(str(chosen["primary_state"]))
        member_classes.add(_member_class(chosen))
    if (
        len(years) < MINI_PILOT_MINIMUM_YEARS
        or len(states) < MINI_PILOT_MINIMUM_STATES
        or len(member_classes) < MINI_PILOT_MINIMUM_MEMBER_CLASSES
    ):
        raise TrendsError(
            "Mini-pilot cannot satisfy Florida, three-state, three-year, and member-class rules."
        )
    return sorted(selected, key=lambda row: int(row["selection_rank"]))


def _season(value: date) -> str:
    if value.month in {12, 1, 2}:
        return "winter"
    if value.month in {3, 4, 5}:
        return "spring"
    if value.month in {6, 7, 8}:
        return "summer"
    return "autumn"


def _request_window(start: date, end: date, rules: Mapping[str, Any]) -> dict[str, str]:
    request = cast(Mapping[str, Any], rules["request"])
    baseline_days = int(request["baseline_days"])
    post_days = int(request["post_days"])
    baseline_start = start - timedelta(days=baseline_days)
    baseline_end = start - timedelta(days=1)
    post_start = end + timedelta(days=1)
    post_end = end + timedelta(days=post_days)
    request_start = baseline_start
    request_end = post_end
    minimum = int(request["minimum_window_days"])
    current_days = (request_end - request_start).days + 1
    if current_days < minimum:
        request_end += timedelta(days=minimum - current_days)
        post_end = request_end
    return {
        "baseline_start_date": baseline_start.isoformat(),
        "baseline_end_date": baseline_end.isoformat(),
        "event_start_date": start.isoformat(),
        "event_end_date": end.isoformat(),
        "post_start_date": post_start.isoformat(),
        "post_end_date": post_end.isoformat(),
        "request_start_date": request_start.isoformat(),
        "request_end_date": request_end.isoformat(),
    }


def _comparison_plan_rows(  # noqa: PLR0913
    episodes: Sequence[Mapping[str, Any]],
    geography: Mapping[str, Mapping[str, Any]],
    batches: Sequence[Mapping[str, Any]],
    rules: Mapping[str, Any],
    request_rules: Mapping[str, Any],
    concepts: Mapping[str, Mapping[str, Any]],
    context_id: str,
    normalization_anchor: Any,
    terminology_version: str,
    backend: Backend,
    request_role: str,
) -> list[dict[str, Any]]:
    rows = []
    for episode in sorted(episodes, key=lambda row: str(row["provisional_episode_id"])):
        episode_id = str(episode["provisional_episode_id"])
        treated_mapping = geography[episode_id]
        request_mapping = (
            {"trends_geography_identifier": "US", "geography_level": "country"}
            if request_role == "national_comparison"
            else treated_mapping
        )
        window = _request_window(
            _as_date(episode["start_date"]), _as_date(episode["end_date"]), rules
        )
        for index, batch in enumerate(batches, 1):
            rows.append(
                _plan_row(
                    episode_id,
                    request_mapping,
                    window,
                    batch,
                    index,
                    request_rules,
                    concepts,
                    context_id,
                    normalization_anchor,
                    terminology_version,
                    backend,
                    "event_study_mini_pilot",
                    "initial",
                    request_role,
                    str(treated_mapping["trends_geography_identifier"]),
                    str(batch["batch_id"]),
                )
            )
    return rows


def _control_plan_rows(  # noqa: PLR0913
    episodes: Sequence[Mapping[str, Any]],
    controls_path: Path,
    batches: Sequence[Mapping[str, Any]],
    rules: Mapping[str, Any],
    request_rules: Mapping[str, Any],
    concepts: Mapping[str, Mapping[str, Any]],
    context_id: str,
    normalization_anchor: Any,
    terminology_version: str,
    backend: Backend,
) -> list[dict[str, Any]]:
    if not controls_path.exists():
        raise TrendsError(f"Control-state selection is missing: {controls_path}")
    controls_by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _read_rows(controls_path):
        controls_by_episode[str(row["treated_episode"])].append(row)
    rows = []
    for episode in sorted(episodes, key=lambda row: str(row["provisional_episode_id"])):
        episode_id = str(episode["provisional_episode_id"])
        treated_geography = f"US-{episode['primary_state']}"
        window = _request_window(
            _as_date(episode["start_date"]), _as_date(episode["end_date"]), rules
        )
        selected = sorted(
            controls_by_episode.get(episode_id, []),
            key=lambda row: (int(row["control_rank"]), str(row["control_state"])),
        )
        for control in selected:
            mapping = {
                "trends_geography_identifier": f"US-{control['control_state']}",
                "geography_level": "state",
            }
            for index, batch in enumerate(batches, 1):
                rows.append(
                    _plan_row(
                        episode_id,
                        mapping,
                        window,
                        batch,
                        index,
                        request_rules,
                        concepts,
                        context_id,
                        normalization_anchor,
                        terminology_version,
                        backend,
                        "event_study_mini_pilot",
                        "initial",
                        "control_state",
                        treated_geography,
                        str(batch["batch_id"]),
                    )
                )
    return rows


def _explore_url(labels: Sequence[str], geography: str, window: Mapping[str, str]) -> str:
    query = quote(",".join(labels), safe="")
    timeframe = quote(f"{window['request_start_date']} {window['request_end_date']}", safe="")
    return f"https://trends.google.com/trends/explore?date={timeframe}&geo={quote(geography)}&q={query}"


def _manual_instructions() -> str:
    return """# Manual Google Trends exports

These plans are reproducibility aids, not a stable API contract. Open the
Explore URL, confirm every term/topic, geography, date, category, and search
property, then download the Interest over time chart as CSV. Place the unchanged
CSV in `raw/` with its required JSON or YAML sidecar. Values are request-relative
0-to-100 search interest, not absolute search counts. Repeat exports must use a
new `repeat_id`; never overwrite an earlier export.

Copy the matching file from `planning/sidecars/` beside the raw export. Replace
`REQUIRED_AFTER_EXPORT` with the actual export date and set a unique repeat ID.
The placeholder intentionally fails import validation until completed.

`weather` is a shared context and positive-control candidate, not an accepted
normalization anchor. Do not populate `anchor_concept_id` unless empirical
anchor evaluation supports a configured candidate. Export both Florida Batch 1
sidecars unchanged apart from export date and their distinct repeat IDs.

Interpret broad disruption terms cautiously. `outage` may describe internet,
cellular, electricity, or another service and is not proof of a power outage.
`traffic` is not flood-specific without supporting flood or weather-term
increases. `road closed` and `school closed` are natural-language alternatives
to the formal noun phrases retained in the terminology comparison set.
"""


def _plan_row(
    episode_id: str,
    mapping: Mapping[str, Any],
    window: Mapping[str, str],
    batch: Mapping[str, Any],
    batch_index: int,
    request_rules: Mapping[str, Any],
    concepts: Mapping[str, Mapping[str, Any]],
    context_id: str,
    normalization_anchor: Any,
    terminology_version: str,
    backend: Backend,
    stage: str,
    planned_repeat_id: str,
    request_role: str = "treated_state",
    treated_geography: str | None = None,
    comparison_batch_id: str | None = None,
) -> dict[str, Any]:
    concept_ids = [str(item) for item in cast(Sequence[Any], batch["concepts"])]
    if concept_ids[0] != context_id:
        raise TrendsError(f"Batch {batch['batch_id']} must begin with context {context_id}.")
    payload = {
        "episode_id": episode_id,
        "geography": mapping["trends_geography_identifier"],
        "start": window["request_start_date"],
        "end": window["request_end_date"],
        "category": request_rules["category"],
        "search_property": request_rules["search_property"],
        "backend": backend,
        "concepts": concept_ids,
        "batch_id": batch["batch_id"],
        "terminology_version": terminology_version,
    }
    request_id = hashlib.sha256(_canonical_json(payload).encode()).hexdigest()
    filenames = _repeat_filenames(request_id, planned_repeat_id)
    labels = [str(concepts[item]["query_text"]) for item in concept_ids]
    return {
        "request_id": request_id,
        "episode_id": episode_id,
        "request_role": request_role,
        "treated_geography": treated_geography or mapping["trends_geography_identifier"],
        "geography": mapping["trends_geography_identifier"],
        "geography_level": mapping["geography_level"],
        **window,
        "timeframe": f"{window['request_start_date']} {window['request_end_date']}",
        "category": request_rules["category"],
        "search_property": request_rules["search_property"],
        "context_concept": context_id,
        "anchor_concept": normalization_anchor,
        "concept_ids": ";".join(concept_ids),
        "target_concepts": ";".join(concept_ids[1:]),
        "batch_id": batch["batch_id"],
        "comparison_batch_id": comparison_batch_id or batch["batch_id"],
        "batch_index": batch_index,
        "group_count": len(concept_ids),
        "terminology_version": terminology_version,
        "plan_stage": stage,
        **filenames,
        "backend": backend,
        "request_status": "manual_export_pending" if backend == "manual_csv" else "planned",
        "explore_url": _explore_url(labels, str(mapping["trends_geography_identifier"]), window),
    }


def _repeat_filenames(request_id: str, repeat_id: str) -> dict[str, str]:
    suffix = "" if repeat_id == "initial" else f"-{repeat_id.replace('_', '-')}"
    return {
        "planned_repeat_id": repeat_id,
        "expected_output_filename": f"{request_id}{suffix}.csv",
        "sidecar_metadata_filename": f"{request_id}{suffix}.json",
    }


def _write_sidecar_templates(
    output_dir: Path,
    plans: Sequence[Mapping[str, Any]],
    concepts: Mapping[str, Mapping[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_names = {str(plan["sidecar_metadata_filename"]) for plan in plans}
    for existing in output_dir.glob("*.json"):
        if existing.name not in expected_names:
            existing.unlink()
    for plan in plans:
        concept_ids = [item for item in str(plan["concept_ids"]).split(";") if item]
        request_id = str(plan["request_id"])
        _write_json(
            output_dir / str(plan["sidecar_metadata_filename"]),
            {
                "request_id": request_id,
                "backend": plan["backend"],
                "requested_concepts": concept_ids,
                "requested_labels": [concepts[item]["query_text"] for item in concept_ids],
                "query_types": [concepts[item]["query_type"] for item in concept_ids],
                "semantic_families": [concepts[item]["semantic_family"] for item in concept_ids],
                "geography": plan["geography"],
                "geography_level": plan["geography_level"],
                "start_date": plan["request_start_date"],
                "end_date": plan["request_end_date"],
                "category": plan["category"],
                "search_property": plan["search_property"],
                "export_date": "REQUIRED_AFTER_EXPORT",
                "source_filename": plan["expected_output_filename"],
                "episode_id": plan["episode_id"],
                "request_role": plan.get("request_role", "treated_state"),
                "treated_geography": plan.get("treated_geography", plan["geography"]),
                "batch_id": plan["batch_id"],
                "comparison_batch_id": plan.get("comparison_batch_id", plan["batch_id"]),
                "anchor_concept_id": plan["anchor_concept"],
                "repeat_id": plan["planned_repeat_id"],
                "terminology_version": plan["terminology_version"],
                "notes": "Complete export_date and repeat_id before import.",
            },
        )


def _load_sidecar(path: Path) -> dict[str, Any]:
    if path.suffix.lower() in {".yaml", ".yml"}:
        return load_yaml(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TrendsError("Sidecar must contain an object.")
    return payload


def _preserve_raw(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if _sha256_file(source) != _sha256_file(target):
            raise TrendsError(f"Raw artifact already exists with different content: {target.name}")
        return
    shutil.copy2(source, target)


def _parse_export(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = [[cell.strip() for cell in row] for row in csv.reader(handle)]
    header_index = next(
        (
            index
            for index, row in enumerate(rows)
            if row and row[0].lower() in {"date", "day", "week", "month"}
        ),
        None,
    )
    if header_index is None:
        raise TrendsError("CSV does not contain a recognized Interest over time header.")
    header = rows[header_index]
    data = [row for row in rows[header_index + 1 :] if row and any(cell for cell in row)]
    if any(len(row) != len(header) for row in data):
        raise TrendsError("CSV rows do not match the Interest over time header width.")
    return header, data


def _parse_export_date(value: str) -> date:
    text = value.strip()
    if " - " in text:
        text = text.split(" - ", maxsplit=1)[0]
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise TrendsError(f"Invalid Trends observation date: {value}") from exc


def _parse_interest(value: str) -> tuple[float | None, bool, str]:
    text = value.strip()
    partial = text.endswith("isPartial") or text.endswith("(partial)")
    text = text.removesuffix("isPartial").removesuffix("(partial)").strip()
    if text in {"", "N/A", "NA", "null"}:
        return None, partial, "partial" if partial else "missing"
    if text.startswith("<"):
        return 0.0, partial, "reported_zero_low_volume"
    try:
        number = float(text)
    except ValueError as exc:
        raise TrendsError(f"Invalid Trends interest value: {value}") from exc
    if not 0 <= number <= MAX_INTEREST:
        raise TrendsError(f"Trends interest must be in [0, 100], got {number}.")
    return number, partial, "reported_zero_relative_scale" if number == 0 else "true_zero_unknown"


def _mean_series(
    grouped: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
) -> dict[tuple[str, str], dict[date, float]]:
    output = {}
    for key, rows in grouped.items():
        by_date: dict[date, list[float]] = defaultdict(list)
        for row in rows:
            if row["interest"] is not None and not bool(row["is_partial"]):
                by_date[_as_date(row["date"])].append(float(row["interest"]))
        output[key] = {day: statistics.fmean(values) for day, values in by_date.items()}
    return output


def _metric_row(
    rows: Sequence[Mapping[str, Any]],
    series: Mapping[date, float],
    plan: Mapping[str, Any],
    rules: Mapping[str, Any],
    repeat: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    baseline = _period_values(series, plan["baseline_start_date"], plan["baseline_end_date"])
    event = _period_values(series, plan["event_start_date"], plan["event_end_date"])
    post = _period_values(series, plan["post_start_date"], plan["post_end_date"])
    all_values = list(series.values())
    zero_fraction = sum(value == 0 for value in all_values) / len(all_values) if all_values else 1.0
    baseline_mean = statistics.fmean(baseline) if baseline else None
    baseline_median = statistics.median(baseline) if baseline else None
    baseline_sd = statistics.pstdev(baseline) if len(baseline) > 1 else None
    baseline_mad = _mad(baseline)
    event_max = max(event) if event else None
    post_max = max(post) if post else None
    peak = max([value for value in [event_max, post_max] if value is not None], default=None)
    peak_dates = [day for day, value in series.items() if peak is not None and value == peak]
    reasons = []
    normalization = cast(Mapping[str, Any], rules["normalization"])
    if not all_values:
        status = "missing"
        reasons.append("no_valid_observations")
    elif all(value == 0 for value in all_values):
        status = "all_zero"
        reasons.append("all_zero_series")
    elif len(baseline) < int(normalization["minimum_baseline_days"]):
        status = "insufficient_baseline"
        reasons.append("insufficient_baseline")
    elif len(post) < int(normalization["minimum_post_days"]):
        status = "insufficient_post_period"
        reasons.append("insufficient_post_period")
    elif zero_fraction >= float(
        cast(Mapping[str, Any], rules["suppression"])["low_volume_zero_fraction"]
    ):
        status = "low_volume"
        reasons.append("high_zero_fraction")
    else:
        status = "usable"
    if repeat and not bool(repeat["stable"]):
        status = "usable_with_caution" if status == "usable" else status
        reasons.append("repeat_instability")
    baseline_safe = baseline_mean is not None and baseline_mean > 0
    variance_safe = baseline_sd is not None and baseline_sd > float(
        normalization["near_zero_standard_deviation"]
    )
    mad_safe = baseline_mad is not None and baseline_mad > 0
    absolute_lift = peak - baseline_mean if peak is not None and baseline_mean is not None else None
    result = {
        "provisional_episode_id": rows[0]["episode_id"],
        "concept_id": rows[0]["concept_id"],
        "semantic_family": rows[0]["semantic_family"],
        "geography": rows[0]["geography"],
        "request_id": rows[0]["request_id"],
        "baseline_valid_day_count": len(baseline),
        "event_valid_day_count": len(event),
        "post_valid_day_count": len(post),
        "baseline_mean": baseline_mean,
        "baseline_median": baseline_median,
        "baseline_standard_deviation": baseline_sd,
        "baseline_mad": baseline_mad,
        "event_mean": statistics.fmean(event) if event else None,
        "event_maximum": event_max,
        "post_mean": statistics.fmean(post) if post else None,
        "post_maximum": post_max,
        "absolute_peak_lift": absolute_lift,
        "ratio_peak_lift": peak / baseline_mean
        if peak is not None and baseline_mean is not None and baseline_safe
        else None,
        "z_score_peak_lift": absolute_lift / baseline_sd
        if absolute_lift is not None and baseline_sd is not None and variance_safe
        else None,
        "robust_peak_lift": (peak - baseline_median) / baseline_mad
        if peak is not None
        and baseline_median is not None
        and baseline_mad is not None
        and mad_safe
        else None,
        "time_to_peak_days": (min(peak_dates) - _as_date(plan["event_start_date"])).days
        if peak_dates
        else None,
        "peak_date": min(peak_dates).isoformat() if peak_dates else None,
        "response_duration_days": sum(
            day >= _as_date(plan["event_start_date"])
            and baseline_mean is not None
            and value > baseline_mean
            for day, value in series.items()
        ),
        "positive_excess_area": sum(max(0.0, value - baseline_mean) for value in [*event, *post])
        if baseline_mean is not None
        else None,
        "anchor_ratio_peak": None,
        "zero_fraction": zero_fraction,
        "suppression_flag": zero_fraction
        >= float(cast(Mapping[str, Any], rules["suppression"])["low_volume_zero_fraction"]),
        "repeat_stability_flag": bool(repeat["stable"]) if repeat else False,
        "metric_quality_status": status,
        "metric_quality_reasons": ";".join(sorted(set(reasons))),
    }
    suppression = {
        "request_id": rows[0]["request_id"],
        "episode_id": rows[0]["episode_id"],
        "concept_id": rows[0]["concept_id"],
        "all_zero_series": bool(all_values) and all(value == 0 for value in all_values),
        "zero_fraction": zero_fraction,
        "longest_zero_run": _longest_zero_run(series),
        "baseline_zero_fraction": _zero_fraction(baseline),
        "event_zero_fraction": _zero_fraction(event),
        "isolated_spike_count": _isolated_spikes(series, rules),
        "low_volume_instability": result["suppression_flag"],
        "insufficient_baseline": len(baseline) < int(normalization["minimum_baseline_days"]),
        "insufficient_post_event_period": len(post) < int(normalization["minimum_post_days"]),
    }
    return result, suppression


def _repeat_diagnostics(
    groups: Mapping[tuple[str, str, str], Sequence[Mapping[str, Any]]],
    rules: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_request: dict[tuple[str, str], list[dict[date, float]]] = defaultdict(list)
    for (request_id, concept_id, _), rows in groups.items():
        by_request[(request_id, concept_id)].append(
            {
                _as_date(row["date"]): float(row["interest"])
                for row in rows
                if row["interest"] is not None and not bool(row["is_partial"])
            }
        )
    threshold = cast(Mapping[str, Any], rules["normalization"])
    output = []
    for (request_id, concept_id), repeats in sorted(by_request.items()):
        correlations: list[float] = []
        differences: list[float] = []
        peak_dates: list[date] = []
        for series in repeats:
            if series:
                peak_dates.append(
                    min(day for day, value in series.items() if value == max(series.values()))
                )
        for index, left in enumerate(repeats):
            for right in repeats[index + 1 :]:
                overlap = sorted(set(left) & set(right))
                if overlap:
                    differences.extend(abs(left[day] - right[day]) for day in overlap)
                    correlation_value = _correlation(
                        [left[day] for day in overlap], [right[day] for day in overlap]
                    )
                    if correlation_value is not None:
                        correlations.append(correlation_value)
        correlation = min(correlations) if correlations else None
        peak_range = (max(peak_dates) - min(peak_dates)).days if len(peak_dates) > 1 else None
        stable = (
            len(repeats) > 1
            and correlation is not None
            and correlation >= float(threshold["stable_repeat_correlation"])
            and (peak_range or 0) <= int(threshold["stable_repeat_peak_tolerance_days"])
        )
        output.append(
            {
                "request_id": request_id,
                "concept_id": concept_id,
                "repeat_count": len(repeats),
                "mean_value_across_repeats": _mean(
                    [value for series in repeats for value in series.values()]
                ),
                "standard_deviation_across_repeats": statistics.pstdev(
                    [value for series in repeats for value in series.values()]
                )
                if sum(len(series) for series in repeats) > 1
                else None,
                "maximum_absolute_difference": max(differences) if differences else None,
                "correlation_across_repeats": correlation,
                "peak_date_range_days": peak_range,
                "event_lift_stability": None,
                "stable": stable,
            }
        )
    return output


def _normalization_diagnostics(
    metrics: Sequence[Mapping[str, Any]],
    series: Mapping[tuple[str, str], Mapping[date, float]],
    plans: Mapping[str, Mapping[str, Any]],
    rules: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_episode: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in metrics:
        by_episode[str(row["provisional_episode_id"])].append(row)
    output = []
    minimum_overlap = int(
        cast(Mapping[str, Any], rules["normalization"])["minimum_anchor_overlap_days"]
    )
    for episode_id, rows in sorted(by_episode.items()):
        anchors = []
        for row in rows:
            request_id = str(row["request_id"])
            anchor_id = str(plans[request_id]["anchor_concept"])
            anchor = series.get((request_id, anchor_id))
            if anchor:
                anchors.append((request_id, anchor))
        for index, (left_id, left) in enumerate(anchors):
            for right_id, right in anchors[index + 1 :]:
                overlap = sorted(set(left) & set(right))
                valid = [day for day in overlap if left[day] > 0 and right[day] > 0]
                ratios = [left[day] / right[day] for day in valid]
                output.append(
                    {
                        "episode_id": episode_id,
                        "left_request_id": left_id,
                        "right_request_id": right_id,
                        "anchor_overlap_count": len(valid),
                        "scale_factor": statistics.median(ratios)
                        if len(valid) >= minimum_overlap
                        else None,
                        "scale_stability": statistics.pstdev(ratios) if len(ratios) > 1 else None,
                        "residual_error": _mean(
                            [abs(value - statistics.median(ratios)) for value in ratios]
                        )
                        if ratios
                        else None,
                        "failed_rescaling_reason": ""
                        if len(valid) >= minimum_overlap
                        else "insufficient_nonzero_anchor_overlap",
                    }
                )
    return output


def _anchor_ratio_peak(
    target: Mapping[date, float], anchor: Mapping[date, float], rules: Mapping[str, Any]
) -> float | None:
    minimum = float(cast(Mapping[str, Any], rules["normalization"])["minimum_anchor_interest"])
    ratios = [
        target[day] / anchor[day]
        for day in sorted(set(target) & set(anchor))
        if anchor[day] >= minimum
    ]
    return max(ratios) if ratios else None


def _period_values(series: Mapping[date, float], start: Any, end: Any) -> list[float]:
    lower, upper = _as_date(start), _as_date(end)
    return [value for day, value in sorted(series.items()) if lower <= day <= upper]


def _mad(values: Sequence[float]) -> float | None:
    if not values:
        return None
    median = statistics.median(values)
    return statistics.median(abs(value - median) for value in values)


def _longest_zero_run(series: Mapping[date, float]) -> int:
    longest = current = 0
    for _, value in sorted(series.items()):
        current = current + 1 if value == 0 else 0
        longest = max(longest, current)
    return longest


def _zero_fraction(values: Sequence[float]) -> float | None:
    return sum(value == 0 for value in values) / len(values) if values else None


def _isolated_spikes(series: Mapping[date, float], rules: Mapping[str, Any]) -> int:
    values = [value for _, value in sorted(series.items())]
    multiplier = float(cast(Mapping[str, Any], rules["suppression"])["isolated_spike_multiplier"])
    count = 0
    for index in range(1, len(values) - 1):
        neighbors = max(values[index - 1], values[index + 1], 1.0)
        count += values[index] >= multiplier * neighbors
    return count


def _correlation(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) < MIN_CORRELATION_VALUES or len(right) != len(left):
        return None
    left_mean, right_mean = statistics.fmean(left), statistics.fmean(right)
    numerator = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right, strict=True))
    denominator = math.sqrt(
        sum((a - left_mean) ** 2 for a in left) * sum((b - right_mean) ** 2 for b in right)
    )
    return numerator / denominator if denominator else None


def _best_concept(rows: Sequence[Mapping[str, Any]], *families: str) -> str | None:
    candidates = [row for row in rows if row["semantic_family"] in families]
    if not candidates:
        return None
    return str(
        max(
            candidates,
            key=lambda row: (
                float(row.get("robust_peak_lift") or -math.inf),
                str(row["concept_id"]),
            ),
        )["concept_id"]
    )


def _quicklook_svg(
    label: str,
    rows: Sequence[Mapping[str, Any]],
    metric: Mapping[str, Any],
    plan: Mapping[str, Any] | None,
) -> str:
    width, height = 900, 360
    values = [float(row["interest"]) for row in rows if row["interest"] is not None]
    points = []
    for index, value in enumerate(values):
        x = 50 + index * 800 / max(1, len(values) - 1)
        y = 300 - value * 2.5
        points.append(f"{x:.1f},{y:.1f}")
    event_text = (
        f"Episode {plan['event_start_date']} to {plan['event_end_date']}"
        if plan
        else "Episode window unavailable"
    )
    title = label.replace("_", " ").title()
    detail = (
        f"{event_text}; concept {metric['concept_id']}; quality {metric['metric_quality_status']}"
    )
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
            '<rect width="100%" height="100%" fill="white"/>',
            f'<text x="50" y="28" font-family="sans-serif" font-size="18">{title}</text>',
            '<text x="50" y="50" font-family="sans-serif" font-size="12">'
            "Relative Google Trends search interest (0-100), not absolute volume</text>",
            '<line x1="50" y1="300" x2="850" y2="300" stroke="#555"/>',
            '<line x1="50" y1="50" x2="50" y2="300" stroke="#555"/>',
            f'<polyline fill="none" stroke="#1769aa" stroke-width="2" '
            f'points="{" ".join(points)}"/>',
            f'<text x="50" y="330" font-family="sans-serif" font-size="12">{detail}</text>',
            "</svg>",
        ]
    )


def _join_reasons(existing: str, reason: str) -> str:
    return ";".join(sorted({item for item in [*existing.split(";"), reason] if item}))


def _median(values: Sequence[Any]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    return statistics.median(cleaned) if cleaned else None


def _mean(values: Sequence[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _read_rows(path: Path) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _row_count(path: Path) -> int:
    return int(pq.ParquetFile(path).metadata.num_rows)


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = (
        pa.Table.from_pylist([dict(row) for row in rows])
        if rows
        else pa.table({"request_id": pa.array([], type=pa.string())})
    )
    pq.write_table(table, path, compression="zstd")


def _write_csv(
    path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str] | None = None
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(fields or (list(rows[0]) if rows else []))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )


def _canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
