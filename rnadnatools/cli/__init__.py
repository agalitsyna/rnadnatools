import click
from .. import __version__
from .._logging import get_logger

CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
}


@click.version_option(__version__, "-V", "--version")
@click.group(context_settings=CONTEXT_SETTINGS)
@click.option("--profile", is_flag=True)
def cli(profile: bool):
    """Type -h or --help after subcommand."""

    # Enable profiling based on:
    # https://stackoverflow.com/questions/55880601/how-to-use-profiler-with-click-cli-in-python
    if profile:
        import cProfile
        import pstats
        import io
        import atexit

        print("Profiling...")
        pr = cProfile.Profile()
        pr.enable()

        def exit():
            pr.disable()
            print("Profiling completed")
            s = io.StringIO()
            pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats()
            print(s.getvalue())

        atexit.register(exit)


from . import table, segment, genome, read
