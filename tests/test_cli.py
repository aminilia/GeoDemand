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
    assert "geoparquet" in result.output
    assert "primary_column" in result.output


def test_cli_profile(clean_groundsource_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "profile.json"

    result = CliRunner().invoke(
        app,
        ["data", "profile", "--input", str(clean_groundsource_path), "--output", str(output_path)],
    )

    assert result.exit_code == 0
    assert output_path.exists()


def test_cli_filter_groundsource_requires_boundaries(
    clean_groundsource_path: Path,
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "data",
            "filter-groundsource",
            "--input",
            str(clean_groundsource_path),
            "--output",
            str(tmp_path / "processed"),
            "--country",
            "US",
        ],
    )

    assert result.exit_code != 0
    assert "provide --country-boundaries" in result.output


def test_cli_audit_groundsource(groundsource_path: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "audit"

    result = CliRunner().invoke(
        app,
        [
            "data",
            "audit-groundsource",
            "--input",
            str(groundsource_path),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0
    assert (output_dir / "schema.json").exists()
    assert (output_dir / "manifest.json").exists()
    assert (output_dir / "geometry_quality.csv").exists()


def test_cli_validate_groundsource_reports_rejections(groundsource_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        ["data", "validate-groundsource", "--input", str(groundsource_path)],
    )

    assert result.exit_code != 0
    assert "rejected records" in result.output


def test_cli_boundaries_inspect(tmp_path: Path) -> None:
    root = tmp_path / "boundaries"
    root.mkdir()

    result = CliRunner().invoke(app, ["boundaries", "inspect", "--boundary-root", str(root)])

    assert result.exit_code == 0
    assert "natural_earth" in result.output
