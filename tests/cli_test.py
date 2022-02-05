from click.testing import CliRunner
from rnadnatools.cli import cli

# import os.path as op


def test_basic_cli(request):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--help",
        ],
    )
    assert result.exit_code == 0, result.output
