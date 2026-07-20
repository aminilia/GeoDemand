from __future__ import annotations

import importlib.util
import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from geodemand.cli import app

ROOT = Path(__file__).parents[1]
RETIRED_NAMES = ("mrms", "imerg", "usgs")


@pytest.mark.parametrize("group", RETIRED_NAMES)
def test_retired_cli_groups_do_not_exist(group: str) -> None:
    result = CliRunner().invoke(app, [group, "--help"])

    assert result.exit_code != 0
    assert "No such command" in result.output


@pytest.mark.parametrize("module", RETIRED_NAMES)
def test_retired_modules_are_not_importable(module: str) -> None:
    assert importlib.util.find_spec(f"geodemand.{module}") is None


def test_package_has_no_retired_optional_dependencies() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extras = pyproject["project"].get("optional-dependencies", {})

    assert all(name not in extras for name in (*RETIRED_NAMES, "verification"))
    serialized = repr(extras).lower()
    for token in ("cfgrib", "eccodes", "s3fs", "earthaccess", "h5py", "dataretrieval"):
        assert token not in serialized


def test_readme_has_no_active_retired_workflow_claims() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8").lower()

    for command in ("geodemand mrms", "geodemand imerg", "geodemand usgs"):
        assert command not in readme
    for phrase in (
        "physical confirmation",
        "hydrologic verification",
        "precipitation validation",
        "gauge validation",
        "flood ground truth",
    ):
        assert phrase not in readme
