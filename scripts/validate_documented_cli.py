from __future__ import annotations

import re
import sys
from pathlib import Path

import click
from click.testing import CliRunner
from typer.main import get_command

from geodemand.cli import app

ROOT = Path(__file__).resolve().parents[1]
COMMAND_PATTERN = re.compile(r"(?:uv run )?geodemand(?:\.exe)?\s+([a-z0-9-]+)(?:\s+([a-z0-9-]+))?")


def main() -> int:
    command = get_command(app)
    paths = _all_command_paths(command)
    documented = _documented_paths()
    failures: list[str] = []
    runner = CliRunner()
    for path in sorted(paths | documented):
        result = runner.invoke(command, [*path, "--help"])
        if result.exit_code != 0:
            failures.append(f"{' '.join(path)}: {result.output.strip()}")
    if failures:
        print("CLI documentation validation failed:\n" + "\n".join(failures), file=sys.stderr)
        return 1
    print(
        f"Validated {len(paths)} CLI command/group help paths and "
        f"{len(documented)} documented paths."
    )
    return 0


def _all_command_paths(root: click.Command) -> set[tuple[str, ...]]:
    paths: set[tuple[str, ...]] = {()}

    def visit(command: click.Command, prefix: tuple[str, ...]) -> None:
        if not hasattr(command, "list_commands") or not hasattr(command, "get_command"):
            return
        context = click.Context(command)
        group = command  # Typer may use its vendored Click group implementation.
        for name in group.list_commands(context):  # type: ignore[attr-defined]
            child = group.get_command(context, name)  # type: ignore[attr-defined]
            if child is None:
                continue
            path = (*prefix, name)
            paths.add(path)
            visit(child, path)

    visit(root, ())
    return paths


def _documented_paths() -> set[tuple[str, ...]]:
    markdown = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
    paths: set[tuple[str, ...]] = set()
    for path in markdown:
        for first, second in COMMAND_PATTERN.findall(path.read_text(encoding="utf-8")):
            paths.add((first, second) if second else (first,))
    return paths


if __name__ == "__main__":
    raise SystemExit(main())
