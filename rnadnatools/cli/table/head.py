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
    "-i",
    "--in-format",
    help="Type of input.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5", "AUTO"], case_sensitive=False),
    required=False,
    default="auto",
)
@click.option(
    "-n",
    "--nrows",
    help='Modifier for column names (input for python formatting), for example: "{colname}__test". Optional.',
    type=int,
    default=10,
    required=False,
)
def head(input_file, in_format, nrows):
    """
    Convert tables between formats, optionally modifying column names in the tables
    """

    # Guess format if not specified:
    if in_format.upper() == "AUTO":
        in_format = utils.guess_format(input_file)

    # Read PARQUET:
    if in_format.upper() == "PARQUET":
        df = pd.read_parquet(input_file)
        print(df.head(nrows), file=sys.stdout)

    if in_format.upper() == "TSV" or in_format.upper() == "CSV":
        df = pd.read_csv(
            input_file, sep="\t" if in_format.upper() == "TSV" else ",", nrows=nrows
        )
        print(df, file=sys.stdout)

    elif in_format.upper() == "HDF5":
        h = h5py.File(input_file, "r")
        dct = {k: h[k][:nrows] for k in h.keys()}
        h.close()

        df = pd.DataFrame.from_dict(dct)
        print(df, file=sys.stdout)

    return 0
