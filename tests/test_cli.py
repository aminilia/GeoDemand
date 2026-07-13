from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from geodemand.cli import app


def test_cli_inspect_groundsource(groundsource_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        ["data", "inspect-groundsource", "--input", str(groundsource_path)],
    )

    assert result.exit_code == 0
    assert "missing_columns" in result.output


def test_cli_profile(groundsource_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "profile.json"

    result = CliRunner().invoke(
        app,
        ["data", "profile", "--input", str(groundsource_path), "--output", str(output_path)],
    )

    assert result.exit_code == 0
    assert output_path.exists()


def test_cli_filter_groundsource(groundsource_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "processed"

    result = CliRunner().invoke(
        app,
        [
            "data",
            "filter-groundsource",
            "--input",
            str(groundsource_path),
            "--output",
            str(output_path),
            "--country",
            "US",
        ],
    )

    assert result.exit_code == 0
    assert list(output_path.rglob("*.parquet"))
