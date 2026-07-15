from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from geodemand.boundaries import BoundaryError, inspect_boundaries, prepare_boundaries
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
from geodemand.mrms import (
    MrmsError,
    assess_mrms,
    discover_episode_outputs,
    extract_mrms,
    fetch_mrms,
    inventory_mrms,
    sample_mrms,
    selfcheck_mrms,
)
from geodemand.spatial import SpatialEnrichmentError, enrich_spatial

app = typer.Typer(help="GeoDemand-FF research pipeline CLI.")
data_app = typer.Typer(help="Data inspection, filtering, and profiling commands.")
boundaries_app = typer.Typer(help="Boundary inspection and preparation commands.")
cohort_app = typer.Typer(help="Candidate event cohort commands.")
episodes_app = typer.Typer(help="Candidate episode clustering commands.")
mrms_app = typer.Typer(help="MRMS feasibility and physical-verification commands.")
app.add_typer(data_app, name="data")
app.add_typer(boundaries_app, name="boundaries")
app.add_typer(cohort_app, name="cohort")
app.add_typer(episodes_app, name="episodes")
app.add_typer(mrms_app, name="mrms")


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


@mrms_app.command("selfcheck")
def mrms_selfcheck_command(
    working_root: Annotated[
        Path, typer.Option("--working-root", file_okay=False, dir_okay=True)
    ] = Path(r"C:\Work\Data\GeoDemand\mrms"),
    sample_grib: Annotated[
        Path | None,
        typer.Option("--sample-grib", exists=True, file_okay=True, dir_okay=False),
    ] = None,
) -> None:
    typer.echo(selfcheck_mrms(working_root, sample_grib))


@mrms_app.command("inspect-episodes")
def mrms_inspect_episodes_command(
    episode_root: Annotated[
        Path,
        typer.Option("--episode-root", exists=True, file_okay=False, dir_okay=True),
    ],
) -> None:
    try:
        outputs = discover_episode_outputs(episode_root)
    except MrmsError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(value) for name, value in outputs.__dict__.items()})


@mrms_app.command("sample")
def mrms_sample_command(
    episode_root: Annotated[
        Path,
        typer.Option("--episode-root", exists=True, file_okay=False, dir_okay=True),
    ],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", file_okay=False, dir_okay=True),
    ] = Path(r"C:\Work\Data\GeoDemand\mrms"),
    seed: Annotated[int, typer.Option("--seed")] = 20260715,
    max_episodes: Annotated[int, typer.Option("--max-episodes", min=1)] = 90,
) -> None:
    try:
        artifacts = sample_mrms(episode_root, output_root, seed=seed, max_episodes=max_episodes)
    except MrmsError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@mrms_app.command("inventory")
def mrms_inventory_command(
    sample_path: Annotated[
        Path,
        typer.Option("--sample", exists=True, file_okay=True, dir_okay=False),
    ],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", file_okay=False, dir_okay=True),
    ] = Path(r"C:\Work\Data\GeoDemand\mrms"),
    max_episodes: Annotated[int | None, typer.Option("--max-episodes", min=1)] = None,
) -> None:
    try:
        artifacts = inventory_mrms(sample_path, output_root, max_episodes=max_episodes)
    except MrmsError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@mrms_app.command("fetch")
def mrms_fetch_command(
    download_plan: Annotated[
        Path,
        typer.Option("--download-plan", exists=True, file_okay=True, dir_okay=False),
    ],
    working_root: Annotated[
        Path,
        typer.Option("--working-root", file_okay=False, dir_okay=True),
    ] = Path(r"C:\Work\Data\GeoDemand\mrms"),
    max_bytes: Annotated[int | None, typer.Option("--max-bytes", min=1)] = None,
    max_episodes: Annotated[int | None, typer.Option("--max-episodes", min=1)] = None,
    workers: Annotated[int, typer.Option("--workers", min=1)] = 2,
) -> None:
    try:
        artifacts = fetch_mrms(
            download_plan,
            working_root,
            max_bytes=max_bytes,
            max_episodes=max_episodes,
            workers=workers,
        )
    except MrmsError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo({name: str(path) for name, path in artifacts.items()})


@mrms_app.command("extract")
def mrms_extract_command(
    sample_path: Annotated[
        Path,
        typer.Option("--sample", exists=True, file_okay=True, dir_okay=False),
    ],
    file_manifest: Annotated[
        Path,
        typer.Option("--file-manifest", exists=True, file_okay=True, dir_okay=False),
    ],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", file_okay=False, dir_okay=True),
    ] = Path(r"C:\Work\Data\GeoDemand\mrms"),
) -> None:
    artifacts = extract_mrms(sample_path, file_manifest, output_root)
    typer.echo({name: str(path) for name, path in artifacts.items()})


@mrms_app.command("assess")
def mrms_assess_command(
    metrics_root: Annotated[
        Path,
        typer.Option("--metrics-root", exists=True, file_okay=False, dir_okay=True),
    ],
) -> None:
    artifacts = assess_mrms(metrics_root)
    typer.echo({name: str(path) for name, path in artifacts.items()})


def _load_mapping(mapping_path: Path | None) -> GroundsourceFieldMapping:
    return GroundsourceFieldMapping.from_json_file(mapping_path)


def _parse_date_option(value: str | None, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD format.") from exc
