import click
import click_log
from .. import cli


@cli.group()
def segment():
    """Segment utils for RNA-DNA insteractions. Segment is either DNA or RNA piece represented as a row in TSV file."""
    pass


from . import find_closest, extract_fastq
