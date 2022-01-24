#!/usr/bin/env python3
import click
import click_log

# Set up logging:
from .. import get_logger

logger = get_logger(__name__)

from . import table

from ...lib import utils

# Loading the data:
import pyarrow as pa
import pyarrow.parquet as pq
import h5py
import pandas as pd
import numpy as np

# Read the arguments:
@table.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(exists=False))
@click.option(
    '-i',
    "--in-format",
    help="Type of input. Optional.",
    default="PARQUET",
    type=click.Choice(["TSV", "PARQUET", "HDF5"], case_sensitive=False),
    show_default=True,
)
@click.option(
    '-c',
    "--columns",
    help="Comma-separated list of column (=filters, here) to include into output file."
         "If None (default), all columns will be included (any value of int/str column will be counted as True).",
    type=str,
    required=False,
    default=None
)
@click.option(
    "--chunksize",
    help="Chunksize for tables loading. Supported for TSV/CSV and HDF5 input for now.",
    default=1_000_000,
    type=int,
    show_default=True,
)
def stats(input_file,
         output_file,
         in_format,
         columns,
         chunksize):
    """
    Save stats of the specified filters (number of True values per column). Output is always TSV.
    """

    if columns is not None:
        columns = columns.split(',')
        if len(columns)==0:
            logger.warn('No columns selected. Nothing to be written. Exit.')
            return 0

    # Read PARQUET, no chunking:
    if in_format.upper()=='PARQUET':
        # Read:
        df = pd.read_parquet(input_file)

        # Filter rows and select columns:
        if columns is None:
            columns = list(df.columns.values)

        output = pd.DataFrame(
            {col: [np.sum(df.loc[:, col] != False)] for col in columns}
        ).T

    # Write HDF5, no chunking for now:
    elif in_format.upper()=="HDF5": # TODO: check
        logger.warn("Reading HDF5 for conversion, no chunking!")

        h = h5py.File(input_file, 'r')

        # Filter rows and select columns:
        if columns is None:
            columns = list(h.keys())

        output = pd.DataFrame(
            {col: [np.sum(h[col][()] != False)] for col in columns}
        ).T

        h.close()

    # Read:
    elif in_format.upper() == "TSV" or in_format.upper() == "CSV":

        instream = pd.read_csv(input_file,
                               sep="\t" if in_format.upper() == "TSV" else ',',
                               chunksize=chunksize,
                               low_memory=True)

        for i, chunk in enumerate(instream):

            if i==0:
                if columns is None:
                    columns = list(chunk.columns.values)
                output = {col: np.sum(chunk.loc[:, col] != False) for col in columns}
            else:
                output = {col: output[col]+np.sum(chunk.loc[:, col] != False) for col in columns}

        output = pd.DataFrame({col: [output[col]] for col in columns}).T

    output.to_csv(output_file, sep='\t', header=None)

    return 0