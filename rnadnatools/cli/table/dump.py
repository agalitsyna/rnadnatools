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
@click.option(
    "-i",
    "--in-format",
    help="Type of input.",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5", "AUTO"], case_sensitive=False),
    required=False,
    default="auto",
)
@click.option(
    "-o",
    "--out-format",
    help="Type of output_file. Same as input for 'auto'",
    type=click.Choice(["TSV", "CSV", "PARQUET", "HDF5", "AUTO"], case_sensitive=False),
    required=False,
    default="auto",
)
@click.option(
    "-f",
    "--filter",
    help="Filter column (should be bool column) to filter the output. "
    "If None (default), all data will be included.",
    type=str,
    required=False,
    default=None,
)
@click.option(
    "-c",
    "--columns",
    help="Comma-separated list of columns to include into output file."
    "If None (default), all columns will be included.",
    type=str,
    required=False,
    default=None,
)
@click.option(
    "--chunksize",
    help="Chunksize for tables loading. Supported for TSV/CSV and HDF5 input for now.",
    default=1_000_000,
    type=int,
    show_default=True,
)
def dump(input_file, output_file, in_format, out_format, filter, columns, chunksize):
    """
    Dump certain columns of the dataset into output file.
    """

    if columns is not None:
        columns = columns.split(",")
        if len(columns) == 0:
            logger.warn("No columns selected. Nothing to be written. Exit.")
            return 0

    # Guess format if not specified:
    if in_format.upper() == "AUTO":
        in_format = utils.guess_format(input_file)
    if out_format.upper() == "AUTO":
        out_format = in_format

    # Read PARQUET, no chunking:
    if in_format.upper() == "PARQUET":
        # Read:
        df = pd.read_parquet(input_file)

        # Filter rows and select columns:
        if filter is not None:
            df = df.loc[df[filter], :]
        if columns is not None:
            df = df.loc[:, columns]

        # Write:
        if out_format.upper() == "CSV" or out_format.upper() == "TSV":
            df.to_csv(output_file, sep="\t" if out_format.upper() == "TSV" else ",")
        elif out_format.upper() == "HDF5":
            output_file = h5py.File(output_file, "a")
            for column_name, result in df.to_dict(orient="list").items():
                output_file.create_dataset(column_name, data=result)
            output_file.close()
        return 0

    # Write HDF5, no chunking for now:
    if out_format.upper() == "HDF5" or in_format.upper() == "HDF5":
        logger.warn("Writing HDF5 for conversion, no chunking!")

        # Read:
        if in_format.upper() == "TSV" or in_format.upper() == "CSV":
            df = pd.read_csv(
                input_file, sep="\t" if in_format.upper() == "TSV" else ",", index=False
            )

            # Filter rows and select columns:
            if filter is not None:
                df = df.loc[df[filter], :]
            if columns is not None:
                df = df.loc[:, columns]

            dct = df.to_dict(orient="list")
            del df

        elif in_format.upper() == "HDF5":
            h = h5py.File(input_file, "r")
            # dct = {k:h[k][()] for k in h.keys()}

            # Filter rows and select columns:
            if columns is not None:
                dct = {k: h[k][()] for k in columns}
            else:
                dct = {k: h[k][()] for k in h.keys()}

            if filter is not None:
                dct = {k: dct[k][dct[filter]] for k in dct.keys()}  # TODO: check

            h.close()

        # Write:
        if out_format.upper() == "HDF5":
            output_file = h5py.File(output_file, "a")
            for column_name, result in dct.items():
                output_file.create_dataset(column_name, data=result)
            output_file.close()
        elif out_format.upper() == "CSV" or out_format.upper() == "TSV":
            df = pd.DataFrame(dct)
            df.to_csv(
                output_file,
                sep="\t" if out_format.upper() == "TSV" else ",",
                index=False,
            )
        return 0

    # Read other formats:
    if in_format.upper() == "TSV" or in_format.upper() == "CSV":
        instream = pd.read_csv(
            input_file,
            sep="\t" if in_format.upper() == "TSV" else ",",
            chunksize=chunksize,
            low_memory=True,
        )

    if out_format.upper() == "PARQUET":
        for i, chunk in enumerate(instream):

            # Filter rows and select columns:
            df_chunk = chunk
            if filter is not None:
                df_chunk = chunk.loc[chunk[filter], :]
            if columns is not None:
                df_chunk = df_chunk.loc[:, columns]

            if i == 0:
                if col_modifier is None:
                    columns = {x: x.replace("#", "") for x in df_chunk.columns}
                else:
                    columns = {
                        x: col_modifier.format(colname=x.replace("#", ""))
                        for x in df_chunk.columns
                    }
                frame = pa.Table.from_pandas(df=df_chunk.rename(columns=columns))
                parquet_schema = frame.schema
                parquet_writer = pq.ParquetWriter(
                    output_file, parquet_schema, compression="snappy"
                )
            table = pa.Table.from_pandas(
                df_chunk.rename(columns=columns), schema=parquet_schema
            )
            parquet_writer.write_table(table)

        parquet_writer.close()

    elif out_format.upper() == "CSV" or out_format.upper() == "TSV":
        header = True
        for i, chunk in enumerate(instream):

            # Filter rows and select columns:
            df_chunk = chunk
            if filter is not None:
                df_chunk = chunk.loc[chunk[filter], :]
            if columns is not None:
                df_chunk = df_chunk.loc[:, columns]
            df_chunk.to_csv(
                output_file,
                sep="\t" if out_format.upper() == "TSV" else ",",
                header=header,
                mode="a" if i != 0 else "w",
                index=False,
            )
            header = False

    return 0
