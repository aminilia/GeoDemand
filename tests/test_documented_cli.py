from scripts.validate_documented_cli import (
    commands_from_markdown,
    main,
    validate_documented_commands,
)


def test_every_cli_and_documented_command_has_working_help() -> None:
    assert main() == 0


def test_documented_cli_validator_rejects_unknown_and_missing_options() -> None:
    commands = commands_from_markdown(
        """```bash
geodemand trends phase-metrics --observations INPUT --bogus VALUE
```"""
    )
    failures = validate_documented_commands(commands)
    assert any("unknown options: --bogus" in failure for failure in failures)
    assert any("omits required options" in failure and "--plan" in failure for failure in failures)
