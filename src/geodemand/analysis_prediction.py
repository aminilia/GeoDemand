"""Fixed, episode-balanced ridge baseline; no tuning or inferential statistics."""

from __future__ import annotations

import hashlib
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

PREDICTION_FIELDS = (
    "episode_id",
    "event_group",
    "concept_id",
    "fold",
    "observed",
    "prediction",
    "naive_prediction",
)
METRIC_FIELDS = ("fold", "model", "episode_count", "row_count", "mae", "rmse", "reason")
COEFFICIENT_FIELDS = (
    "fold",
    "feature",
    "coefficient",
    "training_median",
    "training_mean",
    "training_sd",
    "reason",
    "interpretation",
)


def model_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in rows
        if r["eligible"] and r["primary_unit"] and r["request_role"] == "treated_state"
    ]


def grouped_folds(rows: Sequence[Mapping[str, Any]], folds: int, seed: int) -> dict[str, int]:
    groups = {str(r["event_group"]) for r in rows}
    ordered = sorted(
        groups, key=lambda group: hashlib.sha256(f"{seed}:{group}".encode()).hexdigest()
    )
    return {group: index % folds for index, group in enumerate(ordered)}


def _weights(rows: Sequence[Mapping[str, Any]]) -> Any:
    counts = Counter(str(r["episode_id"]) for r in rows)
    return np.array([1.0 / counts[str(r["episode_id"])] for r in rows])


def _numeric(row: Mapping[str, Any], name: str) -> float:
    value = row.get(name)
    if value is None:
        return float("nan")
    return math.log1p(float(value)) if name == "union_area_km2" else float(value)


def _design(
    train: list[dict[str, Any]], test: list[dict[str, Any]], fold: str
) -> tuple[Any, Any, list[dict[str, Any]]]:
    """All categories, imputations and scaling are learned from training rows only."""
    left: list[Any] = [np.ones(len(train))]
    right: list[Any] = [np.ones(len(test))]
    specs: list[dict[str, Any]] = [{"feature": "intercept"}]
    omitted: list[dict[str, Any]] = []
    weights = _weights(train)
    for name in ("union_area_km2", "duration_days", "season_sin", "season_cos"):
        values = np.array([_numeric(r, name) for r in train])
        finite = values[np.isfinite(values)]
        if not len(finite):
            omitted.append({"feature": name, "reason": "unavailable_in_training"})
            continue
        median = float(np.median(finite))
        values = np.where(np.isfinite(values), values, median)
        mean = float(np.average(values, weights=weights))
        sd = float(np.sqrt(np.average((values - mean) ** 2, weights=weights)))
        if sd == 0:
            omitted.append({"feature": name, "reason": "constant_in_training"})
            continue
        other = np.array([_numeric(r, name) for r in test])
        other = np.where(np.isfinite(other), other, median)
        left.append((values - mean) / sd)
        right.append((other - mean) / sd)
        specs.append(
            {
                "feature": "log1p_area" if name == "union_area_km2" else name,
                "training_median": median,
                "training_mean": mean,
                "training_sd": sd,
            }
        )
    concepts = sorted({str(r["concept_id"]) for r in train})
    for concept in concepts[1:]:
        left.append(np.array([float(r["concept_id"] == concept) for r in train]))
        right.append(np.array([float(r["concept_id"] == concept) for r in test]))
        specs.append({"feature": f"concept:{concept} (reference={concepts[0]})"})
    for spec in specs + omitted:
        spec.update(fold=fold, interpretation="descriptive coefficient; no significance test")
    return np.column_stack(left), np.column_stack(right), specs + omitted


def predict(  # noqa: PLR0912
    rows: Sequence[Mapping[str, Any]], config: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    data = model_rows(rows)
    minimum = int(config["minimum_prediction_episodes"])
    reasons = []
    if len({r["event_group"] for r in data}) < minimum:
        reasons.append(f"fewer_than_{minimum}_independent_event_groups")
    for concept in sorted({str(r["concept_id"]) for r in data}):
        if len({r["event_group"] for r in data if r["concept_id"] == concept}) < minimum:
            reasons.append(f"insufficient_independent_coverage:{concept}")
    if reasons or not data:
        reasons = reasons or ["no_eligible_outcomes"]
        return (
            [],
            [{"fold": "all", "model": "not_estimable", "reason": ";".join(reasons)}],
            [],
            reasons,
        )
    folds = grouped_folds(data, int(config["folds"]), int(config["seed"]))
    for fold in range(int(config["folds"])):
        training_groups = {r["event_group"] for r in data if folds[str(r["event_group"])] != fold}
        if len(training_groups) < int(config["minimum_training_episodes"]):
            reasons.append("insufficient_training_groups")
        train = [r for r in data if folds[str(r["event_group"])] != fold]
        design, _, _ = _design(train, [], str(fold))
        if design.shape[1] <= 1:  # intercept alone cannot test predictive descriptors
            reasons.append("no_varying_predictors_in_training")
    if reasons:
        return (
            [],
            [{"fold": "all", "model": "not_estimable", "reason": ";".join(reasons)}],
            [],
            reasons,
        )
    predictions: list[dict[str, Any]] = []
    coefficients: list[dict[str, Any]] = []
    for fold in [*range(int(config["folds"])), "all"]:
        train = [r for r in data if fold == "all" or folds[str(r["event_group"])] != fold]
        test = [r for r in data if fold != "all" and folds[str(r["event_group"])] == fold]
        x, xt, specs = _design(train, test, str(fold))
        y = np.array([r["standardized_peak_lift"] for r in train], dtype=float)
        weights = _weights(train)
        penalty = np.eye(x.shape[1]) * float(config["ridge_alpha"])
        penalty[0, 0] = 0
        beta = np.linalg.solve(x.T @ (weights[:, None] * x) + penalty, x.T @ (weights * y))
        active = [s for s in specs if not s.get("reason")]
        for spec, value in zip(active, beta, strict=True):
            spec["coefficient"] = float(value)
        coefficients.extend(specs)
        naive = float(np.average(y, weights=weights))
        for row, value in zip(test, xt @ beta, strict=True):
            predictions.append(
                {
                    "episode_id": row["episode_id"],
                    "event_group": row["event_group"],
                    "concept_id": row["concept_id"],
                    "fold": fold,
                    "observed": row["standardized_peak_lift"],
                    "prediction": float(value),
                    "naive_prediction": naive,
                }
            )
    predictions.sort(key=lambda r: (r["episode_id"], r["concept_id"]))
    metrics = []
    for fold in [*range(int(config["folds"])), "pooled"]:
        subset = [r for r in predictions if fold == "pooled" or r["fold"] == fold]
        weights = _weights(subset)
        for name, field in (("ridge", "prediction"), ("training_mean", "naive_prediction")):
            errors = np.array([r[field] - r["observed"] for r in subset])
            metrics.append(
                {
                    "fold": str(fold),
                    "model": name,
                    "episode_count": len({r["episode_id"] for r in subset}),
                    "row_count": len(subset),
                    "mae": float(np.average(abs(errors), weights=weights)),
                    "rmse": float(np.sqrt(np.average(errors**2, weights=weights))),
                }
            )
    return predictions, metrics, coefficients, []
