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

# Read the arguments:
@table.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(exists=False))
def stack(input_file, output_file):
    """
    Not implemented yet
    """

    return 0