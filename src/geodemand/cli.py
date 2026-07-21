from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import pyarrow.parquet as pq
import typer

from geodemand.analysis import AnalysisError, build_analysis_dataset
from geodemand.boundaries import BoundaryError, inspect_boundaries, prepare_boundaries
from geodemand.catalog import (
    CatalogError,
    apply_catalog_decisions,
    build_provisional_catalog,
    discover_catalog_inputs,
    inspect_catalog_inputs,
    summarize_catalog,
    validate_catalog,
)
from geodemand.cohort import CohortError, build_cohort, inspect_cohort_inputs
from geodemand.episodes import (
    EpisodeError,
    PolicyName,
    build_episodes,
    compare_episode_policies,
    inspect_episode_inputs,
)
from geodemand.ingestion.groundsource import (
    DuplicatePolicy,
    GroundsourceError,
    GroundsourceFieldMapping,
    audit_groundsource,
    filter_groundsource,
    inspect_groundsource,
    profile_groundsource,
    validate_groundsource,
)
from geodemand.logging import configure_logging
from geodemand.spatial import SpatialEnrichmentError, enrich_spatial
from geodemand.trends import (
    Backend,
    TrendsError,
    assess_episodes,
    calculate_metrics,
    evaluate_terms,
    import_csv_export,
    inspect_trends,
    map_geographies,
    official_api_selfcheck,
    plan_requests,
    summarize_trends,
    validate_imports,
)
from geodemand.trends import (
    pilot_sample as trends_pilot_sample,
)
from geodemand.trends import (
    write_quicklooks as write_trends_quicklooks,
)
from geodemand.trends_browser import (
    BrowserName,
    ExportOptions,
    browser_selfcheck,
    export_browser,
    export_retry,
    export_status,
    export_validate,
)
from geodemand.trends_event_study import (
    attribute_peaks,
    calculate_concurrence,
    calculate_control_adjusted_metrics,
    calculate_phase_metrics,
    select_controls,
)

app = typer.Typer(help="GeoDemand-FF research pipeline CLI.")
data_app = typer.Typer(help="Data inspection, filtering, and profiling commands.")
boundaries_app = typer.Typer(help="Boundary inspection and preparation commands.")
cohort_app = typer.Typer(help="Candidate event cohort commands.")
episodes_app = typer.Typer(help="Candidate episode clustering commands.")
catalog_app = typer.Typer(help="Provisional reported-event catalog commands.")
trends_app = typer.Typer(help="Google Trends feasibility and manual-export commands.")
analysis_app = typer.Typer(help="Integrated analytical dataset commands.")
app.add_typer(data_app, name="data")
app.add_typer(boundaries_app, name="boundaries")
app.add_typer(cohort_app, name="cohort")
app.add_typer(episodes_app, name="episodes")
app.add_typer(catalog_app, name="catalog")
app.add_typer(trends_app, name="trends")
app.add_typer(analysis_app, name="analysis")


@app.callback()
def main() -> None:
    configure_logging()


@data_app.command("inspect-groundsource")
def inspect_groundsource_command(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True, file_okay=True, dir_okay=False),
    ],
    mapping_path: Annotated[
        Path | None,
        typer.Option("--mapping", exists=True, file_okay=True, dir_okay=False),
    ] = None,
) -> None:
    try:
        inspection = inspect_groundsource(input_path, mapping=_load_mapping(mapping_path))
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(inspection.to_dict())


@data_app.command("audit-groundsource")
def audit_groundsource_command(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True, file_okay=True, dir_okay=False),
    ],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False, dir_okay=True)],
    mapping_path: Annotated[
        Path | None,
        typer.Option("--mapping", exists=True, file_okay=True, dir_okay=False),
    ] = None,
    preserve_raw_fields: Annotated[bool, typer.Option("--preserve-raw-fields")] = False,
    duplicate_policy: Annotated[
        DuplicatePolicy,
        typer.Option("--duplicate-policy"),
    ] = "quarantine",
    preserve_invalid_geometries: Annotated[
        bool,
        typer.Option("--preserve-invalid-geometries"),
    ] = False,
    batch_size: Annotated[int, typer.Option("--batch-size", min=1)] = 10_000,
    max_rows: Annotated[int | None, typer.Option("--max-rows", min=1)] = None,
    max_row_groups: Annotated[int | None, typer.Option("--max-row-groups", min=1)] = None,
) -> None:
    try:
        artifacts = audit_groundsource(
            input_path=input_path,
            output_dir=output_dir,
            mapping=_load_mapping(mapping_path),
            preserve_raw_fields=preserve_raw_fields,
            duplicate_policy=duplicate_policy,
            preserve_invalid_geometries=preserve_invalid_geometries,
            batch_size=batch_size,
            max_rows=max_rows,
            max_row_groups=max_row_groups,
        )
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@data_app.command("filter-groundsource")
def filter_groundsource_command(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True, file_okay=True, dir_okay=False),
    ],
    output_path: Annotated[Path, typer.Option("--output", file_okay=False, dir_okay=True)],
    country: Annotated[str, typer.Option("--country")],
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    mapping_path: Annotated[
        Path | None,
        typer.Option("--mapping", exists=True, file_okay=True, dir_okay=False),
    ] = None,
    preserve_raw_fields: Annotated[bool, typer.Option("--preserve-raw-fields")] = False,
    country_boundaries: Annotated[
        Path | None,
        typer.Option("--country-boundaries", exists=True, file_okay=True, dir_okay=False),
    ] = None,
) -> None:
    try:
        result = filter_groundsource(
            input_path=input_path,
            output_path=output_path,
            country=country,
            start_date=_parse_date_option(start_date, "--start-date"),
            end_date=_parse_date_option(end_date, "--end-date"),
            mapping=_load_mapping(mapping_path),
            preserve_raw_fields=preserve_raw_fields,
            country_boundaries=country_boundaries,
        )
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(
        f"Wrote filtered Groundsource output to {result.output_path} ({result.output_rows} rows)"
    )


@data_app.command("profile")
def profile_command(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True, file_okay=True, dir_okay=False),
    ],
    output_path: Annotated[Path, typer.Option("--output", dir_okay=False)],
    mapping_path: Annotated[
        Path | None,
        typer.Option("--mapping", exists=True, file_okay=True, dir_okay=False),
    ] = None,
) -> None:
    try:
        profile_groundsource(
            input_path=input_path,
            output_path=output_path,
            mapping=_load_mapping(mapping_path),
        )
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"Wrote Groundsource profile to {output_path}")


@data_app.command("validate-groundsource")
def validate_groundsource_command(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True, file_okay=True, dir_okay=False),
    ],
    mapping_path: Annotated[
        Path | None,
        typer.Option("--mapping", exists=True, file_okay=True, dir_okay=False),
    ] = None,
) -> None:
    try:
        summary = validate_groundsource(input_path=input_path, mapping=_load_mapping(mapping_path))
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(summary)


@data_app.command("enrich-spatial")
def enrich_spatial_command(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True, file_okay=True, dir_okay=False),
    ],
    countries_path: Annotated[
        Path,
        typer.Option("--countries", exists=True, file_okay=True, dir_okay=False),
    ],
    states_path: Annotated[
        Path,
        typer.Option("--states", exists=True, file_okay=True, dir_okay=False),
    ],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False, dir_okay=True)],
    batch_size: Annotated[int, typer.Option("--batch-size", min=1)] = 10_000,
    max_rows: Annotated[int | None, typer.Option("--max-rows", min=1)] = None,
    max_row_groups: Annotated[int | None, typer.Option("--max-row-groups", min=1)] = None,
) -> None:
    try:
        artifacts = enrich_spatial(
            input_path=input_path,
            countries_path=countries_path,
            states_path=states_path,
            output_dir=output_dir,
            batch_size=batch_size,
            max_rows=max_rows,
            max_row_groups=max_row_groups,
        )
    except (GroundsourceError, SpatialEnrichmentError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@boundaries_app.command("inspect")
def inspect_boundaries_command(
    boundary_root: Annotated[
        Path,
        typer.Option("--boundary-root", exists=True, file_okay=False, dir_okay=True),
    ],
) -> None:
    typer.echo(inspect_boundaries(boundary_root))


@boundaries_app.command("prepare")
def prepare_boundaries_command(
    boundary_root: Annotated[
        Path,
        typer.Option("--boundary-root", exists=True, file_okay=False, dir_okay=True),
    ],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False, dir_okay=True)],
) -> None:
    try:
        paths = prepare_boundaries(boundary_root, output_dir)
    except BoundaryError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(
        {
            "countries": str(paths.countries),
            "states": str(paths.states),
            "manifest": str(paths.manifest),
        }
    )


@cohort_app.command("inspect")
def inspect_cohort_command(
    events_path: Annotated[
        Path,
        typer.Option("--events", exists=True, file_okay=True, dir_okay=True),
    ],
    state_overlaps_path: Annotated[
        Path,
        typer.Option("--state-overlaps", exists=True, file_okay=True, dir_okay=True),
    ],
) -> None:
    try:
        typer.echo(inspect_cohort_inputs(events_path, state_overlaps_path))
    except CohortError as exc:
        raise typer.BadParameter(str(exc)) from exc


@cohort_app.command("build")
def build_cohort_command(
    events_path: Annotated[
        Path,
        typer.Option("--events", exists=True, file_okay=True, dir_okay=True),
    ],
    state_overlaps_path: Annotated[
        Path,
        typer.Option("--state-overlaps", exists=True, file_okay=True, dir_okay=True),
    ],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False, dir_okay=True)],
    start_date: Annotated[str, typer.Option("--start-date")] = "2022-01-01",
    end_date: Annotated[str, typer.Option("--end-date")] = "2025-12-31",
    primary_domain: Annotated[str, typer.Option("--primary-domain")] = "conus",
    batch_size: Annotated[int, typer.Option("--batch-size", min=1)] = 10_000,
    max_rows: Annotated[int | None, typer.Option("--max-rows", min=1)] = None,
) -> None:
    try:
        artifacts = build_cohort(
            events_path=events_path,
            state_overlaps_path=state_overlaps_path,
            output_dir=output_dir,
            start_date=_parse_date_option(start_date, "--start-date") or date(2022, 1, 1),
            end_date=_parse_date_option(end_date, "--end-date") or date(2025, 12, 31),
            primary_domain=primary_domain,
            batch_size=batch_size,
            max_rows=max_rows,
        )
    except CohortError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@episodes_app.command("inspect")
def inspect_episodes_command(
    events_path: Annotated[
        Path,
        typer.Option("--events", exists=True, file_okay=True, dir_okay=True),
    ],
    event_states_path: Annotated[
        Path,
        typer.Option("--event-states", exists=True, file_okay=True, dir_okay=True),
    ],
) -> None:
    try:
        typer.echo(inspect_episode_inputs(events_path, event_states_path))
    except EpisodeError as exc:
        raise typer.BadParameter(str(exc)) from exc


@episodes_app.command("build")
def build_episodes_command(
    events_path: Annotated[
        Path,
        typer.Option("--events", exists=True, file_okay=True, dir_okay=True),
    ],
    event_states_path: Annotated[
        Path,
        typer.Option("--event-states", exists=True, file_okay=True, dir_okay=True),
    ],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False, dir_okay=True)],
    policy: Annotated[PolicyName, typer.Option("--policy")] = "balanced",
    batch_size: Annotated[int, typer.Option("--batch-size", min=1)] = 10_000,
    max_rows: Annotated[int | None, typer.Option("--max-rows", min=1)] = None,
) -> None:
    try:
        artifacts = build_episodes(
            events_path=events_path,
            event_states_path=event_states_path,
            output_dir=output_dir,
            policy_name=policy,
            batch_size=batch_size,
            max_rows=max_rows,
        )
    except EpisodeError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@episodes_app.command("compare")
def compare_episodes_command(
    events_path: Annotated[
        Path,
        typer.Option("--events", exists=True, file_okay=True, dir_okay=True),
    ],
    event_states_path: Annotated[
        Path,
        typer.Option("--event-states", exists=True, file_okay=True, dir_okay=True),
    ],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False, dir_okay=True)],
    batch_size: Annotated[int, typer.Option("--batch-size", min=1)] = 10_000,
    max_rows: Annotated[int | None, typer.Option("--max-rows", min=1)] = None,
) -> None:
    try:
        artifacts = compare_episode_policies(
            events_path=events_path,
            event_states_path=event_states_path,
            output_dir=output_dir,
            batch_size=batch_size,
            max_rows=max_rows,
        )
    except EpisodeError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@catalog_app.command("inspect")
def catalog_inspect_command(
    cohort_root: Annotated[Path, typer.Option("--cohort-root", exists=True)],
    episode_root: Annotated[Path, typer.Option("--episode-root", exists=True)],
    comparison_root: Annotated[Path, typer.Option("--comparison-root", exists=True)],
) -> None:
    """Discover required cohort and clustering inputs."""
    try:
        inputs = discover_catalog_inputs(cohort_root, episode_root, comparison_root)
        _echo_json(inspect_catalog_inputs(inputs))
    except CatalogError as exc:
        raise typer.BadParameter(str(exc)) from exc


@catalog_app.command("build-provisional")
def catalog_build_command(
    cohort_root: Annotated[Path, typer.Option("--cohort-root", exists=True)],
    episode_root: Annotated[Path, typer.Option("--episode-root", exists=True)],
    comparison_root: Annotated[Path, typer.Option("--comparison-root", exists=True)],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False)],
    rules_path: Annotated[
        Path, typer.Option("--rules", exists=True, file_okay=True, dir_okay=False)
    ],
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    """Build the deterministic conservative-base reported-episode catalog."""
    try:
        inputs = discover_catalog_inputs(cohort_root, episode_root, comparison_root)
        if dry_run:
            _echo_json(inspect_catalog_inputs(inputs))
            return
        artifacts = build_provisional_catalog(inputs, output_dir, rules_path)
        _echo_json({name: str(path) for name, path in artifacts.items()})
    except CatalogError as exc:
        raise typer.BadParameter(str(exc)) from exc


@catalog_app.command("review-queue")
def catalog_review_queue_command(
    catalog_dir: Annotated[Path, typer.Option("--catalog-dir", exists=True, file_okay=False)],
) -> None:
    """Report the generated deterministic review queue and editable CSV template."""
    queue = catalog_dir / "manual_review_queue.parquet"
    template = catalog_dir / "manual_review_template.csv"
    if not queue.exists() or not template.exists():
        raise typer.BadParameter("Catalog review queue is missing; run build-provisional first.")
    _echo_json(
        {
            "review_count": pq.ParquetFile(queue).metadata.num_rows,
            "manual_review_queue": str(queue),
            "manual_review_template": str(template),
        }
    )


@catalog_app.command("apply-decisions")
def catalog_apply_decisions_command(
    catalog_dir: Annotated[Path, typer.Option("--catalog-dir", exists=True, file_okay=False)],
    decisions_csv: Annotated[
        Path, typer.Option("--decisions", exists=True, file_okay=True, dir_okay=False)
    ],
    rule_version: Annotated[str, typer.Option("--rule-version")],
    output_dir: Annotated[Path, typer.Option("--output-dir", file_okay=False)],
) -> None:
    """Validate manual review decisions and write immutable decision records."""
    try:
        output = apply_catalog_decisions(catalog_dir, decisions_csv, rule_version, output_dir)
        _echo_json({"clustering_decisions": str(output)})
    except CatalogError as exc:
        raise typer.BadParameter(str(exc)) from exc


@catalog_app.command("validate")
def catalog_validate_command(
    catalog_dir: Annotated[Path, typer.Option("--catalog-dir", exists=True, file_okay=False)],
) -> None:
    """Validate accounting, stable IDs, lineage, and split integrity."""
    try:
        _echo_json(validate_catalog(catalog_dir))
    except CatalogError as exc:
        raise typer.BadParameter(str(exc)) from exc


@catalog_app.command("summarize")
def catalog_summarize_command(
    catalog_dir: Annotated[Path, typer.Option("--catalog-dir", exists=True, file_okay=False)],
) -> None:
    """Regenerate deterministic scientific catalog summaries."""
    try:
        _echo_json(summarize_catalog(catalog_dir))
    except CatalogError as exc:
        raise typer.BadParameter(str(exc)) from exc


@trends_app.command("inspect")
def trends_inspect_command(
    catalog_dir: Annotated[Path, typer.Option("--catalog-dir", exists=True, file_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Inspect catalog inputs and existing local Trends artifacts without modification."""
    _run_trends(lambda: inspect_trends(catalog_dir, output_root))


@trends_app.command("official-api-selfcheck")
def trends_official_api_selfcheck_command(
    config_path: Annotated[
        Path | None,
        typer.Option("--config", exists=True, file_okay=True, dir_okay=False),
    ] = None,
) -> None:
    """Report official alpha API availability without exposing secrets or inventing endpoints."""
    _echo_json(official_api_selfcheck(config_path))


@trends_app.command("browser-selfcheck")
def trends_browser_selfcheck_command(
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    download_dir: Annotated[Path, typer.Option("--download-dir", file_okay=False)],
    browser: Annotated[BrowserName, typer.Option("--browser")] = "chromium",
    headless: Annotated[bool, typer.Option("--headless/--visible")] = False,
    profile_dir: Annotated[Path | None, typer.Option("--profile-dir", file_okay=False)] = None,
    request_id: Annotated[str | None, typer.Option("--request-id")] = None,
) -> None:
    """Check a visible browser and one planned Explore page without downloading."""
    _run_trends(
        lambda: browser_selfcheck(
            plan_path,
            download_dir,
            browser=browser,
            headless=headless,
            profile_dir=profile_dir,
            request_id=request_id,
        )
    )


@trends_app.command("export-browser")
def trends_export_browser_command(  # noqa: PLR0913
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    browser: Annotated[BrowserName, typer.Option("--browser")] = "chromium",
    headless: Annotated[bool, typer.Option("--headless/--visible")] = False,
    profile_dir: Annotated[Path | None, typer.Option("--profile-dir", file_okay=False)] = None,
    max_requests: Annotated[int, typer.Option("--max-requests", min=1)] = 1,
    request_ids: Annotated[list[str] | None, typer.Option("--request-id")] = None,
    episode_id: Annotated[str | None, typer.Option("--episode-id")] = None,
    geography: Annotated[str | None, typer.Option("--geography")] = None,
    batch_id: Annotated[str | None, typer.Option("--batch-id")] = None,
    delay_seconds: Annotated[float, typer.Option("--delay-seconds", min=15.0)] = 15.0,
    supervised: Annotated[bool, typer.Option("--supervised")] = False,
    resume: Annotated[bool, typer.Option("--resume")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    stop_on_error: Annotated[bool, typer.Option("--stop-on-error")] = False,
    stop_on_captcha: Annotated[
        bool, typer.Option("--stop-on-captcha/--continue-on-captcha")
    ] = True,
    overwrite: Annotated[bool, typer.Option("--overwrite")] = False,
    mini_pilot: Annotated[bool, typer.Option("--mini-pilot")] = False,
) -> None:
    """Export bounded official Explore CSVs through one controlled browser page."""
    options = ExportOptions(
        browser=browser,
        headless=headless,
        profile_dir=profile_dir,
        max_requests=max_requests,
        request_ids=set(request_ids or []),
        episode_id=episode_id,
        geography=geography,
        batch_id=batch_id,
        delay_seconds=delay_seconds,
        supervised=supervised,
        resume=resume,
        dry_run=dry_run,
        stop_on_error=stop_on_error,
        stop_on_captcha=stop_on_captcha,
        overwrite=overwrite,
        mini_pilot=mini_pilot,
    )
    _run_trends(lambda: export_browser(plan_path, output_root, options))


@trends_app.command("export-status")
def trends_export_status_command(
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    mini_pilot: Annotated[bool, typer.Option("--mini-pilot")] = False,
) -> None:
    """Report latest browser-export outcomes and remaining planned requests."""
    _run_trends(lambda: export_status(plan_path, output_root, mini_pilot))


@trends_app.command("export-retry")
def trends_export_retry_command(  # noqa: PLR0913
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    request_ids: Annotated[list[str], typer.Option("--request-id")],
    browser: Annotated[BrowserName, typer.Option("--browser")] = "chromium",
    headless: Annotated[bool, typer.Option("--headless/--visible")] = False,
    profile_dir: Annotated[Path | None, typer.Option("--profile-dir", file_okay=False)] = None,
    max_requests: Annotated[int, typer.Option("--max-requests", min=1)] = 1,
    delay_seconds: Annotated[float, typer.Option("--delay-seconds", min=15.0)] = 15.0,
    supervised: Annotated[bool, typer.Option("--supervised")] = False,
    stop_on_error: Annotated[bool, typer.Option("--stop-on-error")] = False,
    overwrite: Annotated[bool, typer.Option("--overwrite")] = False,
) -> None:
    """Retry only explicitly named requests whose latest attempt is retryable."""
    selected = set(request_ids)
    options = ExportOptions(
        browser=browser,
        headless=headless,
        profile_dir=profile_dir,
        max_requests=max_requests,
        request_ids=selected,
        delay_seconds=delay_seconds,
        supervised=supervised,
        stop_on_error=stop_on_error,
        overwrite=overwrite,
    )
    _run_trends(lambda: export_retry(plan_path, output_root, selected, options))


@trends_app.command("export-validate")
def trends_export_validate_command(
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    request_ids: Annotated[list[str] | None, typer.Option("--request-id")] = None,
    mini_pilot: Annotated[bool, typer.Option("--mini-pilot")] = False,
) -> None:
    """Validate downloaded CSVs and browser sidecars without opening a browser."""
    _run_trends(lambda: export_validate(plan_path, output_root, set(request_ids or []), mini_pilot))


@trends_app.command("pilot-sample")
def trends_pilot_sample_command(
    catalog_dir: Annotated[Path, typer.Option("--catalog-dir", exists=True, file_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    """Select the deterministic 40-episode state-level feasibility pilot."""
    if dry_run:
        _run_trends(lambda: inspect_trends(catalog_dir, output_root))
        return
    _run_trends(lambda: trends_pilot_sample(catalog_dir, output_root, rules_path))


@trends_app.command("map-geographies")
def trends_map_geographies_command(
    pilot_path: Annotated[Path, typer.Option("--pilot", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Map pilot episodes to deterministic U.S. state Trends geographies."""
    _run_trends(lambda: map_geographies(pilot_path, output_root))


@trends_app.command("plan")
def trends_plan_command(
    pilot_path: Annotated[Path, typer.Option("--pilot", exists=True, dir_okay=False)],
    geography_path: Annotated[Path, typer.Option("--geography", exists=True, dir_okay=False)],
    terms_path: Annotated[Path, typer.Option("--terms", exists=True, dir_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    backend: Annotated[Backend, typer.Option("--backend")] = "manual_csv",
    include_behavioral_state: Annotated[bool, typer.Option("--include-behavioral-state")] = False,
    include_national: Annotated[bool, typer.Option("--include-national")] = False,
    controls_path: Annotated[
        Path | None, typer.Option("--controls", exists=True, dir_okay=False)
    ] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    """Generate bounded request batches and manual Explore links without fetching data."""
    if dry_run:
        _echo_json({"backend": backend, "fetch_performed": False})
        return
    _run_trends(
        lambda: plan_requests(
            pilot_path,
            geography_path,
            terms_path,
            rules_path,
            output_root,
            backend,
            include_behavioral_state,
            include_national,
            controls_path,
        )
    )


@trends_app.command("import-csv")
def trends_import_csv_command(
    csv_path: Annotated[Path, typer.Option("--csv", exists=True, dir_okay=False)],
    sidecar_path: Annotated[Path, typer.Option("--sidecar", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Import one unchanged official Interest over time CSV using required sidecar metadata."""
    _run_trends(lambda: import_csv_export(csv_path, sidecar_path, output_root))


@trends_app.command("validate-imports")
def trends_validate_imports_command(
    observations_path: Annotated[Path, typer.Option("--observations", exists=True, dir_okay=False)],
    plan_path: Annotated[Path | None, typer.Option("--plan", exists=True, dir_okay=False)] = None,
) -> None:
    """Validate observation identity, repeat preservation, and request-plan lineage."""
    _run_trends(lambda: validate_imports(observations_path, plan_path))


@trends_app.command("metrics")
def trends_metrics_command(
    observations_path: Annotated[Path, typer.Option("--observations", exists=True, dir_okay=False)],
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Calculate guarded baseline, peak, anchor, suppression, and repeat metrics."""
    _run_trends(lambda: calculate_metrics(observations_path, plan_path, rules_path, output_root))


@trends_app.command("evaluate-terms")
def trends_evaluate_terms_command(
    metrics_path: Annotated[Path, typer.Option("--metrics", exists=True, dir_okay=False)],
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    terms_path: Annotated[Path, typer.Option("--terms", exists=True, dir_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Classify terminology using coverage, volume, stability, and geographic consistency."""
    _run_trends(
        lambda: evaluate_terms(metrics_path, plan_path, terms_path, rules_path, output_root)
    )


@trends_app.command("select-controls")
def trends_select_controls_command(
    pilot_path: Annotated[Path, typer.Option("--pilot", exists=True, dir_okay=False)],
    catalog_path: Annotated[Path, typer.Option("--catalog", exists=True, dir_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    metadata_path: Annotated[
        Path | None, typer.Option("--state-metadata", exists=True, dir_okay=False)
    ] = None,
) -> None:
    """Select deterministic flood-free candidate control states for pilot episodes."""
    _run_trends(
        lambda: select_controls(pilot_path, catalog_path, rules_path, output_root, metadata_path)
    )


@trends_app.command("phase-metrics")
def trends_phase_metrics_command(
    observations_path: Annotated[Path, typer.Option("--observations", exists=True, dir_okay=False)],
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    terms_path: Annotated[Path, typer.Option("--terms", exists=True, dir_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Calculate baseline-separated anticipatory and recovery response metrics."""
    _run_trends(
        lambda: calculate_phase_metrics(
            observations_path, plan_path, terms_path, rules_path, output_root
        )
    )


@trends_app.command("concurrence")
def trends_concurrence_command(
    observations_path: Annotated[Path, typer.Option("--observations", exists=True, dir_okay=False)],
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    terms_path: Annotated[Path, typer.Option("--terms", exists=True, dir_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    phase_metrics_path: Annotated[
        Path | None, typer.Option("--phase-metrics", exists=True, dir_okay=False)
    ] = None,
) -> None:
    """Measure flood/weather concurrence and standardized national spike diagnostics."""
    _run_trends(
        lambda: calculate_concurrence(
            observations_path,
            plan_path,
            terms_path,
            rules_path,
            output_root,
            phase_metrics_path,
        )
    )


@trends_app.command("control-adjusted-metrics")
def trends_control_adjusted_metrics_command(
    phase_metrics_path: Annotated[
        Path, typer.Option("--phase-metrics", exists=True, dir_okay=False)
    ],
    controls_path: Annotated[Path, typer.Option("--controls", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Compare treated and control within-series standardized phase lifts."""
    _run_trends(
        lambda: calculate_control_adjusted_metrics(phase_metrics_path, controls_path, output_root)
    )


@trends_app.command("attribute-peaks")
def trends_attribute_peaks_command(
    phase_metrics_path: Annotated[
        Path, typer.Option("--phase-metrics", exists=True, dir_okay=False)
    ],
    concurrence_path: Annotated[Path, typer.Option("--concurrence", exists=True, dir_okay=False)],
    rules_path: Annotated[Path, typer.Option("--rules", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    control_adjusted_path: Annotated[
        Path | None, typer.Option("--control-adjusted", exists=True, dir_okay=False)
    ] = None,
    national_diagnostics_path: Annotated[
        Path | None, typer.Option("--national-diagnostics", exists=True, dir_okay=False)
    ] = None,
) -> None:
    """Assign provisional peak-attribution categories while preserving evidence."""
    _run_trends(
        lambda: attribute_peaks(
            phase_metrics_path,
            concurrence_path,
            output_root,
            rules_path,
            control_adjusted_path,
            national_diagnostics_path,
        )
    )


@trends_app.command("assess")
def trends_assess_command(
    pilot_path: Annotated[Path, typer.Option("--pilot", exists=True, dir_okay=False)],
    geography_path: Annotated[Path, typer.Option("--geography", exists=True, dir_okay=False)],
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
    metrics_path: Annotated[
        Path | None, typer.Option("--metrics", exists=True, dir_okay=False)
    ] = None,
) -> None:
    """Assess per-episode Trends feasibility without treating the catalog as ground truth."""
    _run_trends(
        lambda: assess_episodes(pilot_path, geography_path, plan_path, metrics_path, output_root)
    )


@trends_app.command("quicklooks")
def trends_quicklooks_command(
    observations_path: Annotated[Path, typer.Option("--observations", exists=True, dir_okay=False)],
    metrics_path: Annotated[Path, typer.Option("--metrics", exists=True, dir_okay=False)],
    plan_path: Annotated[Path, typer.Option("--plan", exists=True, dir_okay=False)],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Write deterministic relative-interest SVG quicklooks for review categories."""
    _run_trends(
        lambda: write_trends_quicklooks(observations_path, metrics_path, plan_path, output_root)
    )


@trends_app.command("summarize")
def trends_summarize_command(
    output_root: Annotated[Path, typer.Option("--output-root", exists=True, file_okay=False)],
) -> None:
    """Combine deterministic Trends feasibility summaries and acceptance status."""
    _run_trends(lambda: summarize_trends(output_root))


@analysis_app.command("build-dataset")
def analysis_build_dataset_command(
    request_plan_path: Annotated[Path, typer.Option("--request-plan", exists=True, dir_okay=False)],
    episode_metrics_path: Annotated[
        Path, typer.Option("--episode-metrics", exists=True, dir_okay=False)
    ],
    repeat_metrics_path: Annotated[
        Path, typer.Option("--repeat-metrics", exists=True, dir_okay=False)
    ],
    phase_metrics_path: Annotated[
        Path, typer.Option("--phase-metrics", exists=True, dir_okay=False)
    ],
    output_root: Annotated[Path, typer.Option("--output-root", file_okay=False)],
) -> None:
    """Build the deterministic integrated Trends analytical dataset."""
    try:
        artifacts = build_analysis_dataset(
            request_plan_path=request_plan_path,
            episode_metrics_path=episode_metrics_path,
            repeat_metrics_path=repeat_metrics_path,
            phase_metrics_path=phase_metrics_path,
            output_root=output_root,
        )
    except (AnalysisError, TrendsError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    _echo_json({name: str(path) for name, path in artifacts.items()})


def _run_trends(operation: Callable[[], Any]) -> None:
    try:
        result = operation()
        if isinstance(result, dict):
            _echo_json(
                {
                    key: str(value) if isinstance(value, Path) else value
                    for key, value in result.items()
                }
            )
        elif isinstance(result, list):
            _echo_json([str(value) if isinstance(value, Path) else value for value in result])
        else:
            _echo_json(result)
    except TrendsError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _echo_json(payload: object) -> None:
    typer.echo(json.dumps(payload, sort_keys=True, default=str))


def _load_mapping(mapping_path: Path | None) -> GroundsourceFieldMapping:
    return GroundsourceFieldMapping.from_json_file(mapping_path)


def _parse_date_option(value: str | None, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD format.") from exc
