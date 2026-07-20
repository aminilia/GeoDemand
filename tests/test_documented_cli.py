from scripts.validate_documented_cli import main


def test_every_cli_and_documented_command_has_working_help() -> None:
    assert main() == 0
