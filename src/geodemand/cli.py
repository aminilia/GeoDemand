from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from geodemand.ingestion.groundsource import (
    GroundsourceError,
    filter_groundsource,
    inspect_groundsource,
    profile_groundsource,
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
) -> None:
    try:
        inspection = inspect_groundsource(input_path)
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(inspection.to_dict())


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
) -> None:
    try:
        output = filter_groundsource(
            input_path=input_path,
            output_path=output_path,
            country=country,
            start_date=_parse_date_option(start_date, "--start-date"),
            end_date=_parse_date_option(end_date, "--end-date"),
        )
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"Wrote filtered Groundsource output to {output}")


@data_app.command("profile")
def profile_command(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True, file_okay=True, dir_okay=False),
    ],
    output_path: Annotated[Path, typer.Option("--output", dir_okay=False)],
) -> None:
    try:
        profile_groundsource(input_path=input_path, output_path=output_path)
    except GroundsourceError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"Wrote Groundsource profile to {output_path}")


def _parse_date_option(value: str | None, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD format.") from exc
