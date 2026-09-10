"""Deterministic tables, figures, and a complete non-inferential final report."""
# Report prose deliberately keeps paragraphs intact in the template.
# ruff: noqa: E501

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

CURVE_BOUND = 28


def _mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def curve_data(
    repeats: list[dict[str, Any]], series: Mapping[tuple[str, str, str], Mapping[date, float]]
) -> dict[str, dict[int, list[float]]]:
    units: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    for row in repeats:
        if not row["eligible"] or not row["primary_unit"] or row["request_role"] != "treated_state":
            continue
        start = date.fromisoformat(row["event_start_date"])
        for day, value in series[(row["request_id"], row["repeat_id"], row["concept_id"])].items():
            lag = (day - start).days
            if -CURVE_BOUND <= lag <= CURVE_BOUND:
                units[(row["episode_id"], row["concept_id"], lag)].append(
                    (value - row["baseline_mean"]) / row["baseline_standard_deviation"]
                )
    result: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for (_, concept, lag), values in units.items():
        result[concept][lag].append(statistics.fmean(values))
    return {c: dict(days) for c, days in result.items()}


def _markdown(rows: list[dict[str, Any]], fields: list[str]) -> str:
    def value(item: Any) -> str:
        if item is None:
            return "—"
        if isinstance(item, float):
            return f"{item:.4g}"
        return str(item).replace("|", "/").replace("\n", " ")

    return "\n".join(
        [
            "| " + " | ".join(fields) + " |",
            "| " + " | ".join("---" for _ in fields) + " |",
            *["| " + " | ".join(value(r.get(f)) for f in fields) + " |" for r in rows],
        ]
    )


def _save(fig: Any, directory: Path, name: str, origin: str) -> None:
    label = (
        "SYNTHETIC FIXTURE — no empirical findings"
        if origin == "synthetic_fixture"
        else "Observed official CSV subset · descriptive analysis"
    )
    fig.text(0.5, 0.008, label, ha="center", fontsize=9, color="#555555")
    fig.tight_layout(rect=(0, 0.035, 1, 0.96))
    fig.savefig(directory / f"{name}.svg", metadata={"Date": None}, bbox_inches="tight")
    fig.savefig(
        directory / f"{name}.png",
        dpi=300,
        metadata={"Software": "GeoDemand 1.1"},
        bbox_inches="tight",
    )
    plt.close(fig)


def build_report(  # noqa: PLR0912, PLR0915
    output: Path,
    readiness: dict[str, Any],
    repeats: list[dict[str, Any]],
    events: list[dict[str, Any]],
    series: Mapping[tuple[str, str, str], Mapping[date, float]],
    predictions: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    sensitivities: list[dict[str, Any]],
    command: str,
) -> None:
    from geodemand.analysis_finalize import (  # noqa: PLC0415
        eligible_treated_control_pairs,
        write_csv,
    )

    tables, figures = output / "tables", output / "figures"
    tables.mkdir()
    figures.mkdir()
    flow = []
    for stage, flag in (
        ("planned", None),
        ("acquired", "acquired"),
        ("verified", "verified"),
        ("eligible", "eligible"),
    ):
        for role in ("treated_state", "control_state"):
            subset = [r for r in repeats if r["request_role"] == role and (flag is None or r[flag])]
            flow.append(
                {
                    "stage": stage,
                    "role": role,
                    "episode_count": len({r["episode_id"] for r in subset}),
                    "request_repeat_count": len(
                        {(r["request_id"], r["repeat_id"]) for r in subset}
                    ),
                    "concept_repeat_rows": len(subset),
                    "panel_count": len({r["batch_id"] for r in subset}),
                }
            )
    write_csv(tables / "sample_flow.csv", flow, list(flow[0]))
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        if row["eligible"] and row["primary_unit"]:
            grouped[(row["concept_id"], row["batch_id"], row["request_role"])].append(row)
    response = []
    for (concept, panel, role), rows in sorted(grouped.items()):
        # One selected panel per concept; backend identity is retained in the datasets.
        by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_episode[row["episode_id"]].append(row)
        if any(len(v) != 1 for v in by_episode.values()):
            raise ValueError(
                "Multiple primary acquisition backends for one episode/concept; select one before finalization."
            )
        contrasts = {
            (treated["matched_pair_id"], treated["concept_id"], treated["batch_id"]): (
                treated,
                control,
            )
            for treated, control in eligible_treated_control_pairs(events)
        }
        differences = [
            treated["standardized_peak_lift"] - control["standardized_peak_lift"]
            for treated, control in contrasts.values()
            if role == "treated_state"
            and str(treated["concept_id"]) == concept
            and str(treated["batch_id"]) == panel
        ]
        response.append(
            {
                "concept": concept,
                "panel": panel,
                "role": role,
                "episodes": len(by_episode),
                "mean_lift": _mean([r["standardized_peak_lift"] for r in rows]),
                "median_lag_days": statistics.median(r["peak_lag_days"] for r in rows),
                "paired_episodes": len(differences),
                "mean_treated_minus_control": _mean(differences),
            }
        )
    response_fields = [
        "concept",
        "panel",
        "role",
        "episodes",
        "mean_lift",
        "median_lag_days",
        "paired_episodes",
        "mean_treated_minus_control",
    ]
    write_csv(tables / "response_summary.csv", response, response_fields)
    robustness = [
        {
            "analysis": r["model"],
            "fold": r["fold"],
            "episodes": r.get("episode_count"),
            "mae": r.get("mae"),
            "rmse": r.get("rmse"),
            "reason": r.get("reason", ""),
        }
        for r in metrics
    ]
    robustness.extend(
        {
            "analysis": r["check"],
            "fold": "descriptive",
            "episodes": r["eligible_episodes"],
            "mean_lift": r["mean_lift"],
            "paired_mean_change": r["paired_mean_change"],
            "reason": r["reason"],
        }
        for r in sensitivities
    )
    robustness_fields = [
        "analysis",
        "fold",
        "episodes",
        "mae",
        "rmse",
        "mean_lift",
        "paired_mean_change",
        "reason",
    ]
    write_csv(tables / "prediction_robustness.csv", robustness, robustness_fields)
    origin = str(readiness["data_origin"])
    primary = [
        r
        for r in events
        if r["eligible"] and r["primary_unit"] and r["request_role"] == "treated_state"
    ]
    concepts = sorted({r["concept_id"] for r in repeats if r["primary_unit"]})
    with plt.rc_context(
        {"svg.hashsalt": "geodemand-1.1", "font.size": 9, "axes.spines.top": False}
    ):
        curves = curve_data(repeats, series)
        fig, axes = plt.subplots(
            math.ceil(len(concepts) / 3),
            3,
            figsize=(12, 3 * math.ceil(len(concepts) / 3)),
            squeeze=False,
        )
        for ax, concept in zip(axes.flat, concepts, strict=False):
            days = curves.get(concept, {})
            x = sorted(days)
            ax.axvline(0, color="0.6", lw=0.8)
            ax.axhline(0, color="0.8", lw=0.8)
            ax.set_title(concept.replace("_", " "))
            ax.set_xlabel("Days from episode onset")
            ax.set_xlim(-CURVE_BOUND, CURVE_BOUND)
            ax.set_ylabel("Mean standardized interest")
            if x:
                ax.plot(x, [statistics.fmean(days[d]) for d in x], color="#1864ab")
                count = ax.twinx()
                count.step(x, [len(days[d]) for d in x], color="#b66a00", alpha=0.55, linestyle=":")
                count.set_ylabel("Episodes/day (dotted)", color="#925400")
                count.set_ylim(0, max(len(v) for v in days.values()) + 1)
            else:
                ax.set_yticks([])
                ax.text(0.5, 0.5, "No eligible observations", ha="center", transform=ax.transAxes)
        for ax in list(axes.flat)[len(concepts) :]:
            ax.set_visible(False)
        fig.suptitle(
            "Event-centered response · repeat means then episode means · no confidence bands"
        )
        _save(fig, figures, "01_response_curves", origin)
        fig, ax = plt.subplots(figsize=(10, 5))
        present = sorted({r["concept_id"] for r in primary})
        for index, concept in enumerate(present):
            lags = [r["peak_lag_days"] for r in primary if r["concept_id"] == concept]
            ax.scatter(lags, [index] * len(lags), s=55, alpha=0.5, label=None)
        ax.set_yticks(range(len(present)), [c.replace("_", " ") for c in present])
        ax.set_xlabel("Response-window peak lag (days; repeat-mean per episode/concept)")
        ax.axvline(0, color="0.7")
        ax.set_title("Peak lag distribution · each dot is an eligible episode/concept")
        if not primary:
            ax.text(0.5, 0.5, "No eligible responses", ha="center", transform=ax.transAxes)
        _save(fig, figures, "02_peak_lag", origin)
        fig, ax = plt.subplots(figsize=(10, 5))
        for concept in present:
            points = [
                r
                for r in primary
                if r["concept_id"] == concept and r.get("union_area_km2") is not None
            ]
            ax.scatter(
                [r["union_area_km2"] for r in points],
                [r["standardized_peak_lift"] for r in points],
                label=concept.replace("_", " "),
                alpha=0.65,
            )
        ax.set_xscale("symlog", linthresh=1)
        ax.set_xlabel("Reported union footprint area (km²; symmetric-log scale)")
        ax.set_ylabel("Response-window standardized peak lift")
        ax.set_title("Response versus reported footprint · area is not validated flood severity")
        if present:
            ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
        else:
            ax.text(0.5, 0.5, "No eligible responses", ha="center", transform=ax.transAxes)
        _save(fig, figures, "03_reported_footprint", origin)
        fig, ax = plt.subplots(figsize=(10, 5))
        if predictions:
            ax.scatter(
                [r["observed"] for r in predictions],
                [r["prediction"] for r in predictions],
                alpha=0.65,
                label="Ridge",
            )
            ax.scatter(
                [r["observed"] for r in predictions],
                [r["naive_prediction"] for r in predictions],
                alpha=0.4,
                marker="x",
                label="Training mean",
            )
            bounds = [
                r[f] for r in predictions for f in ("observed", "prediction", "naive_prediction")
            ]
            ax.plot([min(bounds), max(bounds)], [min(bounds), max(bounds)], color="0.6")
            ax.set_xlabel("Observed standardized peak lift")
            ax.set_ylabel("Held-out predicted lift")
            ax.legend()
            scores = [
                f"{r['model']}: MAE {r['mae']:.3g}, RMSE {r['rmse']:.3g}"
                for r in metrics
                if r["fold"] == "pooled"
            ]
            ax.set_title("Held-out episodes · " + " | ".join(scores), fontsize=10)
            fourth = "04_held_out_predictions"
        else:
            counts = [
                len({r["episode_id"] for r in primary if r["concept_id"] == c}) for c in concepts
            ]
            ax.barh([c.replace("_", " ") for c in concepts], counts, color="#1864ab")
            for i, n in enumerate(counts):
                ax.text(n + 0.15, i, str(n), va="center")
            ax.axvline(20, color="#b54b3b", linestyle="--", label="20-group admission rule")
            ax.set_xlim(0, max(22, max(counts, default=0) + 2))
            ax.set_xlabel("Eligible treated episodes")
            ax.set_title("Prediction not estimable · usable outcome coverage")
            ax.legend(loc="lower right")
            fourth = "04_eligibility_coverage"
        _save(fig, figures, fourth, origin)
    text = f"""# GeoDemand 1.1 final report

Data origin: **{origin}**. Status: **{readiness["status"]}**. Prediction: **{readiness["prediction_status"]}**.

## Results and sample

The supplied plan contains {readiness["planned_episode_count"]} provisional episodes. Observations exist for {readiness["observed_episode_count"]}; raw provenance verifies {readiness["verified_episode_count"]}. The primary response is eligible in {readiness["eligible_episode_count"]} episodes ({readiness["eligible_independent_event_groups"]} independent event groups), yielding {readiness["eligible_primary_unit_count"]} treated episode/concept units. There are {readiness["missing_count"]} missing and {readiness["excluded_count"]} excluded request/repeat/concept records. Missing or ineligible pair records: {readiness["missing_pair_count"]}. See eligibility.csv for every planned record and exclusion reason. This is the available subset, not a claim of complete acquisition.

{_markdown(flow, list(flow[0]))}

Eligible concepts: {", ".join(readiness["eligible_primary_concepts"]) or "none"}. If only weather/context concepts survive admission, these results do not establish a flood-specific search-demand response. Missing or excluded flood concepts cannot be interpreted as evidence that no public response occurred.

## Frozen methods

Only official/manual Explore CSV observations are admitted. Manual and browser-assisted official exports retain acquisition labels; PyTrends is excluded. Original CSV hashes, sidecar metadata, query definitions, daily values and partial flags are checked against observations and the authoritative plan. Unknown timestamps are not inferred. Episode IDs join many-to-one to the provisional catalog. Reported footprint area and duration are descriptors of reports, not physical severity or confirmed flood labels.

Baseline: onset −28 through −8 inclusive (21 possible days, at least 14 valid). Lead: −7 through −1. Immediate: onset through event end +2. Early recovery: end +3 through +7. Extended: end +8 through +28. Each response phase requires at least 80% valid daily coverage. Partial/missing values are excluded. Zero fraction ≥0.5 is low volume; all-zero series are reported separately. Baseline population SD must exceed 0.001. No denominator epsilon or outcome imputation is used.

The primary outcome is the maximum over lead through end +28, minus that baseline mean, divided by that baseline SD. Ties choose the earliest response date. Global peak timing is a separate diagnostic. Phase lifts use phase means and the same effective baseline. Eligible repeats are averaged within a request before episode summaries. Weather from the recovery panel is retained for lineage but excluded from principal summaries to avoid duplicate context weighting. Different panels/backends are not pooled. Figure 1 plots onset −28 through +28; the metrics retain the full end-anchored window. Episode counts are shown for each curve day; no confidence bands are claimed.

## Response summaries

{_markdown(response, response_fields)}

## Prediction and two sensitivity checks

Reasons for a prediction no-go: {"; ".join(readiness["prediction_reasons"]) or "none"}.

Admission requires 20 independent event groups overall and per included concept, five event-grouped folds, and at least 12 training groups per fold. This is a project rule, not a power or representativeness guarantee. Fixed ridge alpha=1 is compared with a training-only episode-balanced mean. Features are log1p(reported area), duration, sine/cosine season and concept. Imputation, scaling, category levels and constant/unavailable-feature exclusion use training data only. Fold coefficients and preprocessing appear in model_coefficients.csv; no p-values are computed. Errors give each episode equal weight. A final full-data fit is labeled separately from held-out predictions.

Cross-validation is retrospective unseen-episode evaluation, not operational forecasting. Existing chronological development/validation/test labels are retained as metadata; this analysis reuses the cohort and does not call the historical test set untouched. Explicit shared-event groups stay in one fold. Geographic/temporal dependence beyond known groups remains a limitation.

Only two sensitivities are run: baseline ending at −1 (otherwise identical definition), and complete two-repeat groups. Comparisons report episode-balanced mean lift and paired changes on shared eligible units; unavailable comparisons are labeled. There is no backend substitution or tuning search.

{_markdown(robustness, robustness_fields)}

## Figures

![Event-centered response](figures/01_response_curves.png)

![Peak lag](figures/02_peak_lag.png)

![Reported footprint](figures/03_reported_footprint.png)

![Prediction or coverage](figures/{fourth}.png)

## Limitations and final scope

All findings are descriptive and conditional on reported episodes, the available acquisition subset, selected concepts, query-relative normalization and quality exclusions. One-control contrasts are observational diagnostics; they do not satisfy the older two-control attribution rule. Zero/low-volume exclusions and missing normalized outcomes may select the analyzed sample. Search interest is not a measured count of demand. No causality, representative population effects, significance, or general repeat reliability is claimed. Unavailable predictors: {", ".join(readiness["unavailable_descriptors"]) or "none at dataset level; fold-level omissions are recorded"}.

If data origin is synthetic_fixture, all numerical results above are software smoke-test output, not empirical findings. If status is empirical_blocked, no verified real data support this report. Otherwise a descriptive feasibility report is the final endpoint even when prediction is not estimable. No new acquisitions, data sources, dashboards, or additional model families are part of this release. Raw data remain local; publication rights are governed separately by DATA_LICENSES.md.

## Reproduction

Use the source snapshot and locked environment recorded in provenance.json. Execute from the repository after `uv sync --locked --extra dev`. Replace the output placeholder with an empty directory.

```text
uv run {command}
```

Provenance records every input/config/lockfile hash, raw source hashes and software source snapshot. Scientific outputs omit volatile timestamps; byte identity is checked in the same locked environment and Parquet content hashes support portable comparisons. PNG/SVG rendering may vary across software platforms.
"""
    (output / "report.md").write_text(text, encoding="utf-8")
