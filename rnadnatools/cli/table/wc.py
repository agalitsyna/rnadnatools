#!/usr/bin/env python3
import click
import click_log

# Set up logging:
from .. import get_logger

logger = get_logger(__name__)

from . import table

from ...lib import utils

import sys

# Loading the data:
import pyarrow as pa
import pyarrow.parquet as pq
import h5py
import pandas as pd

# Read the arguments:
@table.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option(
    '-i',
    "--in-format",
    help="Type of input.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5", "AUTO"], case_sensitive=False),
    required=False,
    default="auto"
)
def wc(input_file, in_format):
    """
    Count number of entries in the table.
    """

    # Guess format if not specified:
    if in_format.upper()=='AUTO':
        in_format = utils.guess_format(input_file)

    # Read PARQUET, no chunking:
    if in_format.upper()=='PARQUET':
        data = pa.parquet.read_table(input_file, memory_map=True)
        print(data.num_rows, file=sys.stdout)

    elif in_format.upper() == "TSV" or in_format.upper() == "CSV":
        df = pd.read_csv(input_file, sep="\t" if in_format.upper() == "TSV" else ',', index_col=False)
        print(len(df), file=sys.stdout)

    else: # in_format.upper() == 'HDF5':
        h = h5py.File(input_file, 'r')
        lengths = [h[k].len() for k in h.keys()]
        h.close()

        assert len(set(lengths))==1, "Different number of rows in columns"

        print(lengths[0], file=sys.stdout)

    return 0