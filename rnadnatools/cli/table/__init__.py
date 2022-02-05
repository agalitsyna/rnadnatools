import click
import click_log
from .. import cli


@cli.group()
def table():
    """Table utils for RNA-DNA interactions."""
    pass


from . import evaluate, convert, merge, align, stack, dump, stats, wc, head
