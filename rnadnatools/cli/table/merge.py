#!/usr/bin/env python3
import click
import click_log

# Set up logging:
from .. import get_logger

logger = get_logger(__name__)

from . import table

from ...lib import utils

# Parse the expressions:
import ast

# Loading the data:
import pyarrow as pa
import pyarrow.parquet as pq
import h5py
import pandas as pd

# Read the arguments:
@table.command()
@click.argument("output_file", type=click.Path(exists=False))
@click.argument("in_paths", nargs=-1, type=click.Path(exists=True))
@click.option(
    '-i',
    "--in-format",
    help="Type of input.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5"], case_sensitive=False),
    required=False,
    show_default=True,
    default='PARQUET'
)
@click.option(
    '-o',
    "--out-format",
    help="Type of output. ",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5"], case_sensitive=False),
    required=False,
    show_default=True,
    default='PARQUET'
)
# @click.option(
#     '-c',
#     "--chunksize",
#     help="Chunksize for tables loading. Supported for TSV/CSV and HDF5 input for now.",
#     default=1_000_000,
#     type=int,
#     show_default=True,
# )
@click.option(
    '-m',
    "--col-modifiers",
    help='Comma-separated modifiers for column names (input for python formatting), for example: "{colname}__test,{colname}__another". Optional.',
    type=str,
    default=None,
    required=False
)
def merge(output_file, in_paths, in_format, out_format, col_modifiers):
    """
    Merge multiple tables into single file. Supports only PARQUET input and output
    """

    if col_modifiers is not None:
        col_modifiers = col_modifiers.strip().split(',')
        assert len(in_paths)==len(col_modifiers), "Please, provide the modifiers for all input tables"

    input_tables = utils.load_tables(in_paths, in_format)

    if in_format.upper()=="PARQUET" and out_format.upper()=="PARQUET":
        columns = []
        schema = []
        for i, table in enumerate(input_tables):
            if col_modifiers is not None:
                table = table.rename_columns([col_modifiers[i].format(col_name=x) for x in table.column_names])
            columns += table.columns
            schema.append(table.schema)

        parquet_schema = pa.unify_schemas(schema)
        pq_merged = pa.Table.from_arrays(columns, schema=parquet_schema)
        parquet_writer = pq.ParquetWriter(output_file, parquet_schema, compression="snappy")
        parquet_writer.write_table(pq_merged)
        parquet_writer.close()

    return 0