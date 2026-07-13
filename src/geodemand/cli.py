from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

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

app = typer.Typer(help="GeoDemand-FF research pipeline CLI.")
data_app = typer.Typer(help="Data inspection, filtering, and profiling commands.")
app.add_typer(data_app, name="data")


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


def _load_mapping(mapping_path: Path | None) -> GroundsourceFieldMapping:
    return GroundsourceFieldMapping.from_json_file(mapping_path)


def _parse_date_option(value: str | None, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD format.") from exc
