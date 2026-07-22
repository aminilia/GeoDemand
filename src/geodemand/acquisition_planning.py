from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from geodemand.trends import (
    REGIONS,
    Backend,
    TrendsError,
    _as_date,
    _canonical_json,
    _plan_row,
    _request_window,
    _select_mini_episodes,
    _write_sidecar_templates,
    load_terms_config,
    load_yaml,
)

SELECTION_FIELDS = (
    "selection_version",
    "selection_set",
    "selection_step",
    "execution_selection_rank",
    "episode_id",
    "event_year",
    "event_start_date",
    "event_end_date",
    "treated_geography",
    "treated_state",
    "selection_reason",
    "selection_reason_detail",
    "selection_fallback_reason",
    "included_in_10_episode_set",
    "included_in_20_episode_set",
    "source_pilot_rank",
)
EXECUTION_LINEAGE_FIELDS = (
    "execution_batch_id",
    "matched_pair_id",
    "selection_version",
    "execution_selection_rank",
    "planned_repeat_ordinal",
    "control_rank",
)
VALID_EPISODE_LIMITS = {10, 20, 40}
TEN_EPISODES = 10
TWENTY_EPISODES = 20
MASTER_EPISODES = 40
SELECTION_SCHEMA_VERSION = "1.0C-2-v1"
EXECUTION_PLAN_SCHEMA_VERSION = "1.0C-2-v1"
ACQUISITION_EPISODE_SELECTION_SCHEMA = pa.schema(
    [
        pa.field("selection_version", pa.string(), nullable=False),
        pa.field("selection_set", pa.string(), nullable=False),
        pa.field("selection_step", pa.int64(), nullable=True),
        pa.field("execution_selection_rank", pa.int64(), nullable=True),
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("event_year", pa.int64(), nullable=False),
        pa.field("event_start_date", pa.string(), nullable=False),
        pa.field("event_end_date", pa.string(), nullable=False),
        pa.field("treated_geography", pa.string(), nullable=False),
        pa.field("treated_state", pa.string(), nullable=False),
        pa.field("selection_reason", pa.string(), nullable=False),
        pa.field("selection_reason_detail", pa.string(), nullable=False),
        pa.field("selection_fallback_reason", pa.string(), nullable=False),
        pa.field("included_in_10_episode_set", pa.bool_(), nullable=False),
        pa.field("included_in_20_episode_set", pa.bool_(), nullable=False),
        pa.field("source_pilot_rank", pa.int64(), nullable=False),
    ],
    metadata={b"schema_name": b"acquisition_episode_selection", b"schema_version": b"1.0C-2-v1"},
)
ACQUISITION_EXECUTION_PLAN_SCHEMA = pa.schema(
    [
        pa.field(name, pa.string(), nullable=nullable)
        for name, nullable in (
            ("request_id", False),
            ("episode_id", False),
            ("request_role", False),
            ("treated_geography", False),
            ("geography", False),
            ("geography_level", False),
            ("baseline_start_date", False),
            ("baseline_end_date", False),
            ("event_start_date", False),
            ("event_end_date", False),
            ("post_start_date", False),
            ("post_end_date", False),
            ("request_start_date", False),
            ("request_end_date", False),
            ("timeframe", False),
        )
    ]
    + [pa.field("category", pa.int64(), nullable=False)]
    + [
        pa.field(name, pa.string(), nullable=nullable)
        for name, nullable in (
            ("search_property", False),
            ("context_concept", False),
            ("anchor_concept", True),
            ("concept_ids", False),
            ("target_concepts", False),
            ("batch_id", False),
            ("comparison_batch_id", False),
        )
    ]
    + [
        pa.field("batch_index", pa.int64(), nullable=False),
        pa.field("group_count", pa.int64(), nullable=False),
    ]
    + [
        pa.field(name, pa.string(), nullable=False)
        for name in (
            "terminology_version",
            "plan_stage",
            "planned_repeat_id",
            "expected_output_filename",
            "sidecar_metadata_filename",
            "backend",
            "request_status",
            "explore_url",
            "execution_batch_id",
            "matched_pair_id",
            "selection_version",
        )
    ]
    + [
        pa.field("execution_selection_rank", pa.int64(), nullable=False),
        pa.field("planned_repeat_ordinal", pa.int64(), nullable=False),
        pa.field("control_rank", pa.int64(), nullable=True),
    ],
    metadata={b"schema_name": b"acquisition_execution_plan", b"schema_version": b"1.0C-2-v1"},
)


def select_execution_episodes(
    pilot_rows: Sequence[Mapping[str, Any]],
    output_root: Path,
    episode_limit: int,
    selection_version: str,
    required_states: Sequence[str] = ("FL",),
    minimum_required_state_episodes: int = 2,
) -> tuple[list[dict[str, Any]], dict[str, Path]]:
    if episode_limit not in VALID_EPISODE_LIMITS:
        raise TrendsError(
            f"invalid_episode_limit: expected one of [10, 20, 40], got {episode_limit}"
        )
    if episode_limit == MASTER_EPISODES:
        return [dict(row) for row in pilot_rows], {}
    if len(pilot_rows) < TWENTY_EPISODES:
        raise TrendsError(
            f"selection_balance_unavailable: 20 episodes required, got {len(pilot_rows)}"
        )
    required_fields = {
        "provisional_episode_id",
        "start_date",
        "end_date",
        "primary_state",
        "selection_rank",
        "member_count",
    }
    malformed = [
        index
        for index, row in enumerate(pilot_rows, 1)
        if required_fields - set(row)
        or any(not str(row.get(field) or "").strip() for field in required_fields)
    ]
    if malformed:
        raise TrendsError(f"selection_output_build_error: malformed_rows={malformed[:20]}")
    ordered = sorted(
        (dict(row) for row in pilot_rows),
        key=lambda row: (
            int(row.get("selection_rank") or 10**9),
            str(row["provisional_episode_id"]),
        ),
    )
    normalized_required = tuple(sorted({str(state).upper() for state in required_states}))
    for state in normalized_required:
        capacity = sum(str(row["primary_state"]).upper() == state for row in ordered)
        if capacity < minimum_required_state_episodes:
            raise TrendsError(
                "selection_balance_unavailable: "
                f"required_state={state} required={minimum_required_state_episodes} "
                f"available={capacity}"
            )
    twenty, reasons_20 = _balanced_subset(
        ordered,
        TWENTY_EPISODES,
        normalized_required,
        minimum_required_state_episodes,
    )
    ten, reasons_10 = _balanced_subset(twenty, TEN_EPISODES, normalized_required, 1)
    ten_ids = {str(row["provisional_episode_id"]) for row in ten}
    twenty_ids = {str(row["provisional_episode_id"]) for row in twenty}
    rank_20 = {str(row["provisional_episode_id"]): index for index, row in enumerate(twenty, 1)}
    rank_10 = {str(row["provisional_episode_id"]): index for index, row in enumerate(ten, 1)}
    audit_rows = []
    for row in ordered:
        episode_id = str(row["provisional_episode_id"])
        in_ten = episode_id in ten_ids
        in_twenty = episode_id in twenty_ids
        reason = reasons_10.get(episode_id) if in_ten else reasons_20.get(episode_id)
        audit_rows.append(
            {
                "selection_version": selection_version,
                "selection_set": "10_episode"
                if in_ten
                else "20_episode"
                if in_twenty
                else "master_40",
                "selection_step": reason[0] if reason else None,
                "execution_selection_rank": rank_10.get(episode_id)
                if in_ten
                else rank_20.get(episode_id),
                "episode_id": episode_id,
                "event_year": int(str(row["start_date"])[:4]),
                "event_start_date": str(row["start_date"]),
                "event_end_date": str(row["end_date"]),
                "treated_geography": f"US-{row['primary_state']}",
                "treated_state": str(row["primary_state"]),
                "selection_reason": reason[1] if reason else "not_selected",
                "selection_reason_detail": reason[2] if reason else "not in requested cohort",
                "selection_fallback_reason": "",
                "included_in_10_episode_set": in_ten,
                "included_in_20_episode_set": in_twenty,
                "source_pilot_rank": int(row.get("selection_rank") or 0),
            }
        )
    selected = ten if episode_limit == TEN_EPISODES else twenty
    rank = rank_10 if episode_limit == TEN_EPISODES else rank_20
    reasons = reasons_10 if episode_limit == TEN_EPISODES else reasons_20
    for row in selected:
        row["selection_version"] = selection_version
        row["execution_selection_rank"] = rank[str(row["provisional_episode_id"])]
        row["selection_step"] = reasons[str(row["provisional_episode_id"])][0]
    planning = output_root / "planning"
    csv_path = planning / "acquisition_episode_selection.csv"
    parquet_path = planning / "acquisition_episode_selection.parquet"
    summary_path = planning / "acquisition_selection_summary.json"
    summary = {
        "selection_version": selection_version,
        "requested_episode_limit": episode_limit,
        "source_episode_count": len(ordered),
        "ten_episode_count": len(ten),
        "twenty_episode_count": len(twenty),
        "nested_subset_status": ten_ids < twenty_ids,
        "selected_episode_ids": [str(row["provisional_episode_id"]) for row in selected],
        "selected_by_year": dict(
            sorted(Counter(str(row["start_date"])[:4] for row in selected).items())
        ),
        "selected_by_state": dict(
            sorted(Counter(str(row["primary_state"]) for row in selected).items())
        ),
        "selected_by_region": dict(
            sorted(
                Counter(
                    REGIONS.get(str(row["primary_state"]), "unknown") for row in selected
                ).items()
            )
        ),
        "required_states": list(normalized_required),
        "required_state_minimum_counts": dict.fromkeys(
            normalized_required,
            1 if episode_limit == TEN_EPISODES else minimum_required_state_episodes,
        ),
        "required_state_actual_counts": {
            state: sum(str(row["primary_state"]).upper() == state for row in selected)
            for state in normalized_required
        },
        "required_state_continuity_status": "passed",
        "selection_reason_counts": dict(
            sorted(Counter(reason[1] for reason in reasons.values()).items())
        ),
        "selection_algorithm": (
            "required-mini-first balanced greedy by year, state, region, source rank, episode_id"
        ),
        "fallbacks": [],
    }
    try:
        _write_csv(csv_path, audit_rows, ACQUISITION_EPISODE_SELECTION_SCHEMA.names)
        _write_parquet(parquet_path, audit_rows, ACQUISITION_EPISODE_SELECTION_SCHEMA)
        _write_json(summary_path, summary)
    except (OSError, TypeError, ValueError, pa.ArrowException) as exc:
        raise TrendsError(f"selection_output_write_error: {exc}") from exc
    return selected, {
        "selection_csv": csv_path,
        "selection_parquet": parquet_path,
        "selection_summary": summary_path,
    }


def build_execution_plan(  # noqa: PLR0912, PLR0913, PLR0915
    pilot_rows: Sequence[Mapping[str, Any]],
    geography_rows: Sequence[Mapping[str, Any]],
    controls_path: Path | None,
    terms_path: Path,
    rules_path: Path,
    output_root: Path,
    backend: Backend,
    batch_ids: Sequence[str],
    planned_repeats: int,
    execution_batch_id: str,
    controls_per_episode: int,
) -> dict[str, Path]:
    if planned_repeats < 1:
        raise TrendsError(f"invalid_planned_repeat_count: got {planned_repeats}")
    if controls_per_episode != 1:
        raise TrendsError(
            f"rank_one_control_unavailable: controls_per_episode={controls_per_episode}"
        )
    if controls_path is None:
        raise TrendsError("rank_one_control_unavailable: --controls is required")
    if not str(execution_batch_id).strip():
        raise TrendsError("execution_plan_conflicting_lineage: execution_batch_id is required")
    if not pilot_rows:
        raise TrendsError("execution_plan_count_mismatch: no selected episodes")
    if len(pilot_rows) not in {TEN_EPISODES, TWENTY_EPISODES} or any(
        not str(row.get("selection_version") or "").strip()
        or row.get("execution_selection_rank") is None
        for row in pilot_rows
    ):
        raise TrendsError(
            "execution_plan_conflicting_lineage: execution-only planning requires a "
            "nested 10- or 20-episode cohort with complete selection lineage"
        )
    terms = load_terms_config(terms_path)
    rules = load_yaml(rules_path)
    all_batches = {
        str(row["batch_id"]): row
        for row in cast(list[dict[str, Any]], terms["standardized_batches"])
    }
    unknown = sorted(set(batch_ids) - set(all_batches))
    if not batch_ids or unknown:
        raise TrendsError(
            f"execution_plan_invalid_panel: unsupported={unknown} selected={list(batch_ids)}"
        )
    batches = [all_batches[item] for item in batch_ids]
    concepts = {
        str(row["concept_id"]): row
        for row in cast(list[dict[str, Any]], terms["concepts"])
        if row["active"]
    }
    request_rules = cast(Mapping[str, Any], rules["request"])
    context_id = str(request_rules["comparison_context_concept_id"])
    geography = {str(row["provisional_episode_id"]): row for row in geography_rows}
    controls = _rank_one_controls(controls_path)
    rows: list[dict[str, Any]] = []
    pair_geographies: dict[str, tuple[str, str]] = {}
    for episode in sorted(pilot_rows, key=lambda row: int(row["execution_selection_rank"])):
        episode_id = str(episode["provisional_episode_id"])
        treated = geography.get(episode_id)
        control = controls.get(episode_id)
        if treated is None:
            raise TrendsError(
                f"execution_plan_conflicting_lineage: missing geography episode={episode_id}"
            )
        treated_geo = str(treated["trends_geography_identifier"])
        if control is None or control == treated_geo:
            raise TrendsError(f"rank_one_control_unavailable: episode_id={episode_id}")
        pair_id = (
            "pair_"
            + hashlib.sha256(
                _canonical_json(
                    {"episode_id": episode_id, "treated": treated_geo, "control": control}
                ).encode()
            ).hexdigest()[:20]
        )
        pair = (treated_geo, control)
        if pair_id in pair_geographies and pair_geographies[pair_id] != pair:
            raise TrendsError(f"execution_plan_conflicting_lineage: matched_pair_id={pair_id}")
        pair_geographies[pair_id] = pair
        window = _request_window(
            _as_date(episode["start_date"]), _as_date(episode["end_date"]), rules
        )
        role_mappings = (
            ("treated_state", treated, None),
            (
                "control_state",
                {"trends_geography_identifier": control, "geography_level": "state"},
                1,
            ),
        )
        for role, mapping, control_rank in role_mappings:
            for batch_index, batch in enumerate(batches, 1):
                for repeat_ordinal in range(1, planned_repeats + 1):
                    row = _plan_row(
                        episode_id,
                        mapping,
                        window,
                        batch,
                        batch_index,
                        request_rules,
                        concepts,
                        context_id,
                        request_rules.get("normalization_anchor_concept_id"),
                        str(terms["dictionary_version"]),
                        backend,
                        "execution",
                        f"repeat_{repeat_ordinal}",
                        role,
                        treated_geo,
                        str(batch["batch_id"]),
                    )
                    row.update(
                        {
                            "execution_batch_id": execution_batch_id,
                            "matched_pair_id": pair_id,
                            "selection_version": str(episode["selection_version"]),
                            "execution_selection_rank": int(episode["execution_selection_rank"]),
                            "planned_repeat_ordinal": repeat_ordinal,
                            "control_rank": control_rank,
                        }
                    )
                    rows.append(row)
    rows.sort(
        key=lambda row: (
            int(row["execution_selection_rank"]),
            str(row["request_role"]),
            str(row["batch_id"]),
            int(row["planned_repeat_ordinal"]),
        )
    )
    _validate_execution_rows(rows, len(pilot_rows), len(batches), planned_repeats)
    output = output_root / "planning"
    parquet_path = output / "acquisition_execution_plan.parquet"
    csv_path = output / "acquisition_execution_plan.csv"
    summary_path = output / "acquisition_execution_summary.json"
    summary = _execution_summary(rows, execution_batch_id, batch_ids, controls_per_episode)
    try:
        _write_parquet(parquet_path, rows, ACQUISITION_EXECUTION_PLAN_SCHEMA)
        _write_csv(csv_path, rows, ACQUISITION_EXECUTION_PLAN_SCHEMA.names)
        _write_json(summary_path, summary)
        _write_sidecar_templates(output / "sidecars", rows, concepts)
    except (OSError, TypeError, ValueError, pa.ArrowException) as exc:
        raise TrendsError(f"selection_output_write_error: {exc}") from exc
    return {"parquet": parquet_path, "csv": csv_path, "summary": summary_path}


def _balanced_subset(
    rows: Sequence[dict[str, Any]],
    count: int,
    required_states: Sequence[str],
    required_count: int,
) -> tuple[list[dict[str, Any]], dict[str, tuple[int, str, str]]]:
    # The established mini-pilot is the required scientific continuity set.
    required = _legacy_mini(rows)
    selected = list(required[:count])
    reasons = {
        str(row["provisional_episode_id"]): (
            step,
            "legacy_mini_pilot",
            "included by the established five-episode mini-pilot contract",
        )
        for step, row in enumerate(selected, 1)
    }
    remaining = [row for row in rows if row not in selected]
    for state in required_states:
        while sum(str(row["primary_state"]).upper() == state for row in selected) < required_count:
            candidates = [row for row in remaining if str(row["primary_state"]).upper() == state]
            if not candidates:
                raise TrendsError(
                    "selection_balance_unavailable: "
                    f"required_state={state} required={required_count}"
                )
            candidate = min(candidates, key=_source_order)
            selected.append(candidate)
            remaining.remove(candidate)
            reasons[str(candidate["provisional_episode_id"])] = (
                len(selected),
                "required_state_continuity",
                f"selected to reach required {state} continuity count {required_count}",
            )
    while len(selected) < count:
        year_counts = Counter(str(row["start_date"])[:4] for row in selected)
        state_counts = Counter(str(row["primary_state"]) for row in selected)
        region_counts = Counter(
            REGIONS.get(str(row["primary_state"]), "unknown") for row in selected
        )
        candidate = min(
            remaining,
            key=lambda row: (
                year_counts[str(row["start_date"])[:4]],
                region_counts[REGIONS.get(str(row["primary_state"]), "unknown")],
                state_counts[str(row["primary_state"])],
                int(row.get("selection_rank") or 10**9),
                str(row["provisional_episode_id"]),
            ),
        )
        selected.append(candidate)
        remaining.remove(candidate)
        reason = _balance_reason(candidate, selected[:-1], remaining)
        reasons[str(candidate["provisional_episode_id"])] = (len(selected), reason[0], reason[1])
    return sorted(selected, key=_source_order), reasons


def _source_order(row: Mapping[str, Any]) -> tuple[int, str]:
    return int(row.get("selection_rank") or 10**9), str(row["provisional_episode_id"])


def _balance_reason(
    candidate: Mapping[str, Any],
    selected: Sequence[Mapping[str, Any]],
    remaining: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    year_counts = Counter(str(row["start_date"])[:4] for row in selected)
    region_counts = Counter(REGIONS.get(str(row["primary_state"]), "unknown") for row in selected)
    state_counts = Counter(str(row["primary_state"]) for row in selected)
    year = str(candidate["start_date"])[:4]
    region = REGIONS.get(str(candidate["primary_state"]), "unknown")
    state = str(candidate["primary_state"])
    if remaining and year_counts[year] == min(
        year_counts[str(row["start_date"])[:4]] for row in remaining
    ):
        return "underrepresented_year", f"year={year} count_before={year_counts[year]}"
    if remaining and region_counts[region] == min(
        region_counts[REGIONS.get(str(row["primary_state"]), "unknown")] for row in remaining
    ):
        return "underrepresented_region", f"region={region} count_before={region_counts[region]}"
    if remaining and state_counts[state] == min(
        state_counts[str(row["primary_state"])] for row in remaining
    ):
        return "underrepresented_state", f"state={state} count_before={state_counts[state]}"
    return "stable_source_rank_tiebreak", f"source_pilot_rank={candidate['selection_rank']}"


def _legacy_mini(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    # Mirrors the legacy contract without depending on input order.
    return [dict(row) for row in _select_mini_episodes(rows)]


def _rank_one_controls(path: Path) -> dict[str, str]:
    try:
        records = cast(list[dict[str, Any]], pq.read_table(path).to_pylist())
    except (OSError, pa.ArrowException) as exc:
        raise TrendsError(f"rank_one_control_unavailable: path={path} detail={exc}") from exc
    grouped: dict[str, list[str]] = defaultdict(list)
    for row in records:
        try:
            if int(row["control_rank"]) == 1:
                grouped[str(row["treated_episode"])].append(f"US-{row['control_state']}")
        except (KeyError, TypeError, ValueError) as exc:
            raise TrendsError(f"rank_one_control_unavailable: malformed control row={row}") from exc
    conflicts = sorted(
        key for key, values in grouped.items() if len(set(values)) != 1 or len(values) != 1
    )
    if conflicts:
        raise TrendsError(f"rank_one_control_unavailable: episodes={conflicts}")
    return {key: values[0] for key, values in grouped.items()}


def _validate_execution_rows(
    rows: Sequence[Mapping[str, Any]], episodes: int, panels: int, repeats: int
) -> None:
    expected = episodes * 2 * panels * repeats
    if len(rows) != expected:
        raise TrendsError(f"execution_plan_count_mismatch: expected={expected} actual={len(rows)}")
    grain = [(str(row["request_id"]), str(row["planned_repeat_id"])) for row in rows]
    if len(grain) != len(set(grain)):
        raise TrendsError("execution_plan_duplicate_grain: request_id + planned_repeat_id")
    if any(not str(row.get("matched_pair_id") or "").strip() for row in rows):
        raise TrendsError("execution_plan_missing_pair")


def _execution_summary(
    rows: Sequence[Mapping[str, Any]],
    execution_batch_id: str,
    batch_ids: Sequence[str],
    controls: int,
) -> dict[str, Any]:
    request_ids = {str(row["request_id"]) for row in rows}
    return {
        "execution_batch_id": execution_batch_id,
        "selection_version": sorted({str(row["selection_version"]) for row in rows})[0],
        "episode_count": len({str(row["episode_id"]) for row in rows}),
        "treated_request_count": len(
            {str(row["request_id"]) for row in rows if row["request_role"] == "treated_state"}
        ),
        "control_request_count": len(
            {str(row["request_id"]) for row in rows if row["request_role"] == "control_state"}
        ),
        "matched_pair_count": len({str(row["matched_pair_id"]) for row in rows}),
        "panel_count": len(batch_ids),
        "panel_ids": list(batch_ids),
        "logical_request_count": len(request_ids),
        "planned_repeat_count": len({str(row["planned_repeat_id"]) for row in rows}),
        "planned_repeat_row_count": len(rows),
        "plan_row_count": len(rows),
        "planned_repeat_ids": sorted({str(row["planned_repeat_id"]) for row in rows}),
        "controls_per_episode": controls,
        "duplicate_grain_count": 0,
        "lineage_conflict_count": 0,
        "request_hash_invariant_status": "passed",
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]], schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [{field: row.get(field) for field in schema.names} for row in rows], schema=schema
        ),
        path,
        compression="zstd",
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
