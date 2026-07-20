from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from click.testing import CliRunner
from typer.main import get_command

from geodemand.cli import app

ROOT = Path(__file__).resolve().parents[1]
TOKEN_PATTERN = re.compile(r'"[^"]*"|\'[^\']*\'|\S+')


@dataclass(frozen=True)
class DocumentedCommand:
    source: str
    line_number: int
    tokens: tuple[str, ...]


def main() -> int:
    root_command = get_command(app)
    paths = _all_command_paths(root_command)
    documented = _documented_commands()
    failures = _validate_help_paths(root_command, paths)
    failures.extend(validate_documented_commands(documented, root_command))
    if failures:
        print("CLI documentation validation failed:\n" + "\n".join(failures), file=sys.stderr)
        return 1
    print(
        f"Validated {len(paths)} CLI command/group help paths and "
        f"{len(documented)} documented command lines."
    )
    return 0


def validate_documented_commands(
    commands: list[DocumentedCommand], root_command: Any | None = None
) -> list[str]:
    root = root_command or get_command(app)
    failures: list[str] = []
    for documented in commands:
        command, path, command_arguments, resolution_error = _resolve_command(
            root, list(documented.tokens)
        )
        location = f"{documented.source}:{documented.line_number}"
        if resolution_error:
            failures.append(f"{location}: {resolution_error}")
            continue
        known_options = {
            option
            for parameter in getattr(command, "params", [])
            for option in getattr(parameter, "opts", []) + getattr(parameter, "secondary_opts", [])
        }
        supplied_options = {
            token.split("=", 1)[0] for token in command_arguments if token.startswith("-")
        }
        unknown = sorted(supplied_options - known_options - {"--help"})
        if unknown:
            failures.append(
                f"{location}: {' '.join(path)} uses unknown options: {', '.join(unknown)}"
            )
        missing = []
        for parameter in getattr(command, "params", []):
            options = set(getattr(parameter, "opts", [])) | set(
                getattr(parameter, "secondary_opts", [])
            )
            if getattr(parameter, "required", False) and options and not options & supplied_options:
                missing.append(sorted(options)[0])
        if missing and "--help" not in supplied_options:
            failures.append(
                f"{location}: {' '.join(path)} omits required options: {', '.join(sorted(missing))}"
            )
    return failures


def commands_from_markdown(text: str, source: str = "<text>") -> list[DocumentedCommand]:
    commands: list[DocumentedCommand] = []
    in_fence = False
    pending = ""
    pending_line = 0
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        starts_command = bool(re.search(r"(?:^|\s)geodemand(?:\.exe)?(?:\s|$)", stripped))
        if not in_fence or (not pending and not starts_command):
            continue
        if not pending:
            pending_line = line_number
        continuation = stripped.endswith(("`", "\\"))
        segment = stripped[:-1].rstrip() if continuation else stripped
        pending = f"{pending} {segment}".strip()
        if continuation:
            continue
        tokens = [token.strip("\"'") for token in TOKEN_PATTERN.findall(pending)]
        executable = next(
            index for index, token in enumerate(tokens) if token in {"geodemand", "geodemand.exe"}
        )
        commands.append(DocumentedCommand(source, pending_line, tuple(tokens[executable + 1 :])))
        pending = ""
    return commands


def _resolve_command(
    root: Any, tokens: list[str]
) -> tuple[Any, tuple[str, ...], list[str], str | None]:
    command = root
    path: list[str] = []
    index = 0
    while index < len(tokens) and not tokens[index].startswith("-"):
        name = tokens[index]
        children = getattr(command, "commands", {})
        if name not in children:
            return (
                command,
                tuple(path),
                tokens[index:],
                f"unknown command: {' '.join([*path, name])}",
            )
        command = children[name]
        path.append(name)
        index += 1
    if not path and tokens and tokens[0] != "--help":
        return command, (), tokens, f"unknown command: {tokens[0]}"
    return command, tuple(path), tokens[index:], None


def _validate_help_paths(root: Any, paths: set[tuple[str, ...]]) -> list[str]:
    runner = CliRunner()
    failures = []
    for path in sorted(paths):
        result = runner.invoke(root, [*path, "--help"])
        if result.exit_code != 0:
            failures.append(f"{' '.join(path) or 'geodemand'} --help: {result.output.strip()}")
    return failures


def _all_command_paths(root: Any) -> set[tuple[str, ...]]:
    paths: set[tuple[str, ...]] = {()}

    def visit(command: Any, prefix: tuple[str, ...]) -> None:
        for name, child in getattr(command, "commands", {}).items():
            path = (*prefix, name)
            paths.add(path)
            visit(child, path)

    visit(root, ())
    return paths


def _documented_commands() -> list[DocumentedCommand]:
    markdown = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
    commands: list[DocumentedCommand] = []
    for path in markdown:
        commands.extend(commands_from_markdown(path.read_text(encoding="utf-8"), str(path)))
    return commands


if __name__ == "__main__":
    raise SystemExit(main())
