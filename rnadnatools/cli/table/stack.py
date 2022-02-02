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
@click.option('--validate-columns/--no-validate-columns',
              help="Flag for validating that all columns are present in all stacked files."
                   "If --no-validate-columns, the stack has the minimal overlap of columns.",
              default=True)
def stack(output_file,
          in_paths,
          in_format,
          out_format,
          validate_columns):
    """
    Vertical stack of tables.
    """

    input_tables = utils.load_tables(in_paths, in_format)

    if in_format=='HDF5':
        columns = [table.keys() for table in input_tables]
    elif in_format=='PARQUET':
        columns = [table.column_names for table in input_tables]
    else:
        columns = [list(table.columns.values) for table in input_tables]

    columns_overlap = set.intersection(*map(set, columns))
    if validate_columns and len(columns_overlap)!=len(columns[0]):
        raise ValueError("Some files do not have the full set of columns!")
    if len(columns_overlap)==0:
        raise ValueError("No columns overlap betwen files...")

    columns_selected = [col for col in columns[0] if col in columns_overlap]

    if out_format.upper() == "PARQUET":
        for i, chunk in enumerate(input_tables):
            if i == 0:
                frame = chunk.select(columns_selected)
                parquet_schema = frame.schema
                parquet_writer = pq.ParquetWriter(
                    output_file, parquet_schema, compression="snappy"
                )
            parquet_writer.write_table(chunk.select(columns_selected))

        parquet_writer.close()

    elif out_format.upper() == "CSV" or out_format.upper() == "TSV":
        header = True
        for i, chunk in enumerate(input_tables):
            chunk.loc[:, columns_selected].to_csv(output_file,
                         sep=',' if out_format.upper() == "CSV" else '\t',
                         header=header, mode='a' if i!=0 else 'w', index=False)
            header = False

    elif out_format.upper() == "HDF5":
        h = h5py.File(output_file, "w")
        s = 0
        for i, chunk in enumerate(input_tables):
            if i==0:
                for col in columns_selected:
                    if pd.api.types.is_object_dtype(chunk[col]):
                        h.create_dataset(col, data=chunk[col].astype('S100'), maxshape=(None,), chunks=True)
                    else:
                        h.create_dataset(col, data=chunk[col], maxshape=(None,), chunks=True)
                s += len(chunk[col])
            else:
                for col in columns_selected:
                    h[col].resize( (s+len(chunk[col]),) )
                    if pd.api.types.is_object_dtype(chunk[col]):
                        h[col][s:s+len(chunk[col])] = chunk[col].astype('S100')
                    else:
                        h[col][s:s + len(chunk[col])] = chunk[col]
                s += len(chunk[col])

    return 0