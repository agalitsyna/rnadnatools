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
@click.argument("input_file",
                metavar="INPUT_FILE",
                type=click.Path(exists=True))
@click.argument("reference_file",
                metavar="REFERENCE_FILE",
                type=click.Path(exists=True))
@click.argument("output_file",
                metavar="OUTPUT_FILE",
                type=click.Path(exists=False))
@click.option(
    "--key-colname",
    help="ID of the key column in input_file.",
    type=str,
    default=None,
    show_default=True,
)
@click.option(
    "--key-column",
    help="Index of the column with key. Cannot be together with --key-colname.",
    default=None,
    type=int,
    show_default=True,
)
@click.option(
    "--ref-colname",
    help="ID of the reference column in reference_file.",
    type=str,
    default=None,
    show_default=True,
)
@click.option(
    "--ref-column",
    help="Index of the column with reference. Cannot be together with --ref-colname.",
    default=None,
    type=int,
    show_default=True,
)
@click.option(
    "--fill-values",
    help="Single value or comma-separated list to fill in the missing values "
         "(for the key column as well to keep the shape consistent with table columns). ",
    type=str,
    required=True
)
@click.option(
    '-i',
    "--in-format",
    help="Type of input_file.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5"], case_sensitive=False),
    required=True
)
@click.option(
    '-r',
    "--ref-format",
    help="Type of reference_file.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5"], case_sensitive=False),
    required=True
)
@click.option(
    '-o',
    "--out-format",
    help="Type of output_file. ",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5"], case_sensitive=False),
    required=True,
)
@click.option(
    "--new-colnames",
    help="New column names for output table (comma-separated).",
    type=str,
    required=False
)
@click.option('--input-header/--no-input-header',
              help="Flag for the header in input table. Used for TSV/CSV input.",
              default=True)
@click.option('--ref-header/--no-ref-header',
              help="Flag for the header in reference table. Used for TSV/CSV input.",
              default=True)
@click.option('--drop-key/--no-drop-key',
              help="Flag for dropping the key column of table when writing to output.",
              default=True)
# @click.option(
#     '-c',
#     "--chunksize",
#     help="Chunksize for input_file loading. Supported for TSV/CSV and HDF5 input for now.",
#     default=1_000_000,
#     type=int,
#     show_default=True,
# )
def align(input_file,
            reference_file,
            output_file,
            key_column,
            key_colname,
            ref_column,
            ref_colname,
            fill_values,
            in_format,
            ref_format,
            out_format,
            new_colnames,
            input_header,
            ref_header,
            drop_key
):
    """
    Align the INPUT_TABLE by the key-column to the REFERENCE_TABLE by the ref-column.
    Fill in the values and retain the format of the input columns.
    The format of key-column and ref-column is assumed to be string by default.
    """

    # Checks the input parameters:
    if (key_colname is None) and (key_column is None):
        raise ValueError("Please, provide either --key-colname or --key-column.")
    if (key_colname is not None) and (key_column is not None):
        raise ValueError("--key-colname and --key-column cannot work together.")

    if (ref_colname is None) and (ref_column is None):
        raise ValueError("Please, provide either --ref-colname or --ref-column.")
    if (ref_colname is not None) and (ref_column is not None):
        raise ValueError("--ref-colname and --ref-column cannot work together.")

    # Read input:

    # Will be used for TSV/CSV only:
    if input_header:
        input_header = 0
    else:
        input_header = None

    if in_format.upper()=='PARQUET':
        df = pd.read_parquet(input_file)
    elif in_format.upper()=='TSV':
        df = pd.read_csv(input_file, sep="\t", header=input_header)
    elif in_format.upper()=='CSV':
        df = pd.read_csv(input_file, sep=",", header=input_header)
    elif in_format.upper() == 'HDF5':
            h = h5py.File(input_file, 'r')
            dct = {k:h[k][()] for k in h.keys()}
            h.close()
            df = pd.DataFrame.from_dict(dct)

    if key_colname is None:
        key_colname = df.columns[key_column]
    ids = df.loc[:, key_colname].values.astype(str)

    # Read reference:

    # Will be used for TSV/CSV only:
    if ref_header:
        ref_header = 0
    else:
        ref_header = None

    if ref_format.upper() == 'PARQUET':
        df_ref = pd.read_parquet(reference_file)
    elif ref_format.upper() == 'TSV':
        df_ref = pd.read_csv(reference_file, sep="\t", header=ref_header)
    elif ref_format.upper() == 'CSV':
        df_ref = pd.read_csv(reference_file, sep=",", header=ref_header)
    elif ref_format.upper() == 'HDF5':
        h = h5py.File(reference_file, 'r')
        dct = {k: h[k][()] for k in h.keys()}
        h.close()
        df_ref = pd.DataFrame.from_dict(dct)

    if ref_colname is None:
        ref_colname = df_ref.columns[ref_column]
    aln_ids = df_ref[ref_colname].values.astype(str)
    del df_ref

    df_ref = pd.DataFrame({"id": aln_ids}).reset_index().set_index("id")
    l = len(df_ref)

    # Match input and reference:
    ids_order = df_ref.loc[ids, "index"]  # TODO: optimize

    # Create the list of default values:
    fill_values = fill_values.split(',')
    if len(fill_values)==1:
        fill_values = fill_values * (len(df.columns)-1)

    # Create the dictionary with the final values:
    if new_colnames is not None:
        new_colnames = new_colnames.split(',')
        assert len(new_colnames)==len(df.columns), "Please, provide the column names equal to input table."
    else:
        new_colnames = df.columns

    dct_updated = {}
    print(df.columns, new_colnames, fill_values)
    for k, k_new, v in zip(df.columns, new_colnames, fill_values):
        if k==key_column:
            if not drop_key:
                dct_updated[k_new] = aln_ids
            else:
                continue
        # Filling in the whole column including the missing values:
        dct_updated[k_new] = np.full(l, v, dtype=df.dtypes[k])
        # Update only the values present in input table:
        dct_updated[k_new][ids_order] = df.loc[:, k].values

    del df

    # Write output:
    if out_format.upper() == 'HDF5':
        output_file = h5py.File(output_file, 'a')
        for column_name, result in dct_updated.items():
            output_file.create_dataset(column_name, data=result)
        output_file.close()

    elif out_format.upper()=="CSV":
        df = pd.DataFrame( dct_updated )
        df.to_csv(output_file, sep=',', index=False)

    elif out_format.upper() == "TSV":
        df = pd.DataFrame( dct_updated )
        df.to_csv(output_file, sep='\t', index=False)

    elif out_format.upper() == "PARQUET":
        pd.DataFrame( dct_updated ).to_parquet(output_file, compression='snappy')

    return 0