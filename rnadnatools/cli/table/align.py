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
@click.argument("input_file", metavar="INPUT_FILE", type=click.Path(exists=True))
@click.argument(
    "reference_file", metavar="REFERENCE_FILE", type=click.Path(exists=True)
)
@click.argument("output_file", metavar="OUTPUT_FILE", type=click.Path(exists=False))
@click.option(
    "--key-column", "--key-colname",
    help="ID of the key column in input_file. Can be either string or integer.",
    type=str,
    required=True,
)
@click.option(
    "--ref-column", "--ref-colname",
    help="ID of the reference column in reference_file. String or integer.",
    default=None,
    type=str,
    required=True,
)
@click.option(
    "--fill-values",
    help="Single value or comma-separated list to fill in the missing values "
    "(for the key column as well to keep the shape consistent with table columns). ",
    type=str,
    required=True,
)
@click.option(
    "-i",
    "--in-format",
    help="Type of input.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5", "AUTO"], case_sensitive=False),
    required=False,
    default="auto",
)
@click.option(
    "-r",
    "--ref-format",
    help="Type of reference_file.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5", "AUTO"], case_sensitive=False),
    required=False,
    default="auto",
)
@click.option(
    "-o",
    "--out-format",
    help="Type of output_file. Same as input for 'auto'",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5", "auto"], case_sensitive=False),
    required=False,
    default="auto",
)
@click.option(
    "--new-colnames",
    help="New column names for output table (comma-separated).",
    type=str,
    required=False,
)
@click.option(
    "--input-header/--no-input-header",
    help="Flag for the header in input table. Used for TSV/CSV input.",
    default=True,
)
@click.option(
    "--ref-header/--no-ref-header",
    help="Flag for the header in reference table. Used for TSV/CSV input.",
    default=True,
)
@click.option(
    "--drop-key/--no-drop-key",
    help="Flag for dropping the key column of table when writing to output.",
    default=True,
)
@click.option(
    '-c',
    "--chunksize",
    help="Chunksize for loading (not supported for HDF5 and PARQUET for now).",
    default=1_000_000,
    type=int,
    show_default=True,
)
@click.option(
    '-c',
    "--chunksize-writer",
    help="Chunksize for writing.",
    default=1_000_000,
    type=int,
    show_default=True,
)
def align(
    input_file,
    reference_file,
    output_file,
    key_column,
    ref_column,
    fill_values,
    in_format,
    ref_format,
    out_format,
    new_colnames,
    input_header,
    ref_header,
    drop_key,
    chunksize,
    chunksize_writer
):
    """
    Align the INPUT_TABLE by the key-column to the REFERENCE_TABLE by the ref-column.
    Fill in the values and retain the format of the input columns.
    The format of key-column and ref-column is assumed to be string by default.
    """

    # columns reporing mode:
    fill_values = fill_values.split(',')
    if new_colnames:
        new_colnames = new_colnames.split(',')

    # output format:
    if out_format.upper() == "AUTO":
        out_format = in_format

    # column names for input and reference:
    key_column = int(key_column) if key_column.isdigit() else key_column
    ref_column = int(ref_column) if ref_column.isdigit() else ref_column

    # Pre-load input_stream keys:
    input_stream_keys = utils.load_table(input_file,
                                    in_format,
                                    chunksize=chunksize,
                                    usecols=[key_column],
                                    header=0 if input_header else None)
    l = map(lambda x: list(x[key_column].values), input_stream_keys)
    input_ids = set([y for x in list(l) for y in x])

    # Define input stream:
    input_stream = utils.load_table(input_file,
                                    in_format,
                                    chunksize=chunksize,
                                    header=0 if input_header else None)

    # Define reference stream:
    reference_stream = utils.load_table(reference_file,
                                    ref_format,
                                    usecols=[ref_column],
                                    chunksize=chunksize,
                                    header=0 if ref_header else None)
    lst_ref = [y for x in reference_stream for y in x[ref_column].tolist()][::-1]

    # Define input for reader/writer:
    dct_input, colnames, dtypes = utils.update_df_chunk(input_stream, {}, key=key_column)
    if new_colnames:
        dct_rename = dict(zip(colnames, new_colnames))
    keys_loaded = set(dct_input.keys()) # loaded input keys for fast search
    empty_input = dict(zip(colnames, fill_values))

    k = lst_ref.pop()
    mode = 'w'
    dumped = []
    finish = False # flag for finishing the iteration over reference keys

    while True: # iterate over reference keys

        if k in keys_loaded: # check loaded keys
            # Pop written entries from dct_input and store:
            dumped.append(dct_input.pop(k))
            if len(lst_ref)>0:
                k = lst_ref.pop()
            else:
                finish = True

        elif k not in input_ids: # check all possible keys of input
            # Store empty new line:
            empty_input[key_column] = k
            dumped.append(dict(empty_input))
            if len(lst_ref)>0:
                k = lst_ref.pop()
            else:
                finish = True

        else: # not found in loaded yet available from input, read next input piece:
            # Parse next input chunk:
            try:
                utils.update_df_chunk(input_stream, dct_input, key=key_column)
                keys_loaded = set(dct_input.keys())
            except Exception as e:
                raise ValueError(f"Key {k} not found in {in_path}")

        # Write output:
        if len(dumped) >= chunksize_writer or finish:
            dumped = pd.DataFrame(dumped)
            dumped = dumped.loc[:, colnames] # preserve order
            dumped = dumped.astype(dtypes) # preserve data types

            if drop_key: # Drop original key, if needed:
                dumped = dumped.drop(key_column, axis=1)

            if new_colnames: # Add ned names of columns:
                dumped = dumped.rename(dct_rename, axis=1)

            # Writing for the first time:
            if mode == 'w':
                writer, s = utils.write_chunk(dumped, output_file, out_format, mode='w')
                mode = 'a'

            # Appending:
            else:
                writer, s = utils.write_chunk(dumped, output_file, out_format, writer, s, mode='a')

            dumped = []

        if finish: # Last reference id checked
            break

    return 0