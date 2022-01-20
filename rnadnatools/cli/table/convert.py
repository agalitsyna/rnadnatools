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
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(exists=False))
@click.option(
    '-i',
    "--in-format",
    help="Type of input.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5"], case_sensitive=False),
    required=True
)
@click.option(
    '-o',
    "--out-format",
    help="Type of output. ",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5"], case_sensitive=False),
    required=True,
)
@click.option(
    '-c',
    "--chunksize",
    help="Chunksize for tables loading. Supported for TSV/CSV and HDF5 input for now.",
    default=1_000_000,
    type=int,
    show_default=True,
)
@click.option(
    '-m',
    "--col-modifier",
    help='Modifier for column names (input for python formatting), for example: "{colname}__test". Optional.',
    type=str,
    default=None,
    required=False
)
def convert(input_file, output_file, in_format, out_format, chunksize, col_modifier):
    """
    Convert tables between formats, optionally modifying column names in the tables
    """

    if in_format==out_format:
        logger.info("in_format is same as out_format. Nothing to be done. Consider using cp instead.")
        return 0

    # Read PARQUET, no chunking:
    if in_format.upper()=='PARQUET':
        logger.warn("Reading PARQUET for conversion, no chunking!")
        # Read:
        df = pd.read_parquet(input_file)
        # Write:
        if  out_format.upper()=="CSV":
            df.to_csv(output_file)
        elif out_format.upper()=="TSV":
            df.to_csv(output_file, sep='\t')
        elif out_format.upper()=="HDF5":
            output_file = h5py.File(output_file, 'a')
            for column_name, result in df.to_dict(orient='list').items():
                output_file.create_dataset(column_name, data=result)
            output_file.close()
        return 0

    # Write HDF5, no chunking for now:
    if out_format.upper()=='HDF5' or in_format.upper()=="HDF5":
        logger.warn("Writing HDF5 for conversion, no chunking!")

        # Read:
        if in_format.upper() == "TSV":
            df = pd.read_csv(input_file, sep="\t", index=False)
            dct = df.to_dict(orient='list')
            del df
        elif in_format.upper() == "CSV":
            df = pd.read_csv(input_file, sep=",", index=False)
            dct = df.to_dict(orient='list')
            del df
        elif in_format.upper() == 'HDF5':
            h = h5py.File(input_file, 'r')
            dct = {k:h[k][()] for k in h.keys()}
            h.close()

        # Write:
        if out_format.upper() == 'HDF5':
            output_file = h5py.File(output_file, 'a')
            for column_name, result in dct.items():
                output_file.create_dataset(column_name, data=result)
            output_file.close()
        elif out_format.upper()=="CSV":
            df = pd.DataFrame(dct)
            df.to_csv(output_file, sep=',', index=False)
        elif out_format.upper() == "TSV":
            df = pd.DataFrame(dct)
            df.to_csv(output_file, sep='\t', index=False)
        return 0

    # Read other formats:
    if in_format.upper()=="TSV":
        instream = pd.read_csv(input_file, sep="\t", chunksize=chunksize, low_memory=True)
    elif in_format.upper()=="CSV":
        instream = pd.read_csv(input_file, sep=",", chunksize=chunksize, low_memory=True)

    if out_format.upper()=="PARQUET":
        for i, chunk in enumerate(instream):
            if i == 0:
                if col_modifier is None:
                    columns = {x: x.replace("#", "") for x in chunk.columns}
                else:
                    columns = {x: col_modifier.format(colname=x.replace("#", "")) for x in chunk.columns}
                frame = pa.Table.from_pandas(df=chunk.rename(columns=columns))
                parquet_schema = frame.schema
                parquet_writer = pq.ParquetWriter(
                    output_file, parquet_schema, compression="snappy"
                )
            table = pa.Table.from_pandas(chunk.rename(columns=columns), schema=parquet_schema)
            parquet_writer.write_table(table)

        parquet_writer.close()

    elif out_format.upper()=="CSV":
        header = True
        for chunk in instream:
            chunk.to_csv(output_file, sep=',', header=header, mode='a', index=False)
            header = False
    elif out_format.upper()=="TSV":
        header = True
        for chunk in instream:
            chunk.to_csv(output_file, sep='\t', header=header, mode='a', index=False)
            header = False

    return 0