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
@click.argument("output_file", type=click.Path(exists=False))
@click.argument("in_paths", nargs=-1, type=click.Path(exists=True))
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
# @click.option(
#     "--chunksize",
#     help="Chunksize for tables loading. Supported for TSV/CSV and HDF5 input for now.",
#     default=1_000_000,
#     type=int,
#     show_default=True,
# )
def dump(output_file, in_paths, in_format, out_format, filter, columns): #, chunksize):
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
        in_format = utils.guess_format(in_paths[0])
    if out_format.upper() == "AUTO":
        out_format = in_format

    input_tables = utils.load_tables(in_paths, in_format)

    # Load filter:
    if filter is not None:
        isFound = False
        for table in input_tables:
            if in_format.upper() == "PARQUET":
                if filter in table.column_names and not isFound:
                    # if table[filter].type == pa.bool_():
                    #     filter_col = np.where(table[filter].to_numpy(zero_copy_only=False))[0]
                    #     isFound = True
                    # else:
                    filter_col = np.where(table[filter].to_numpy())[0]
                    isFound = True
            # elif in_format.upper() == "HDF5":
            #     if filter in table.keys():
            #         filter_col = np.where(table[filter][()])[0]
            #         isFound = True
            # else:
            #     if filter in table.columns:
            #         filter_col = np.where(table.loc[:, filter])[0]
        if not isFound:
            raise ValueError(f"Column {filter} does not exist in the input tables!")

    # Pick the data:
    if in_format.upper()=="PARQUET" and out_format.upper() == "PARQUET":
        columns_loaded = []
        schema = []
        list_loaded = []
        list_available = []
        for i, table in enumerate(input_tables):
            columns_selected = [x for x in columns if x in table.column_names]
            list_available += table.column_names
            frame = table.select(columns_selected)
            frame_filtered = frame.take(filter_col) if filter is not None else frame
            columns_loaded += frame_filtered
            schema.append(frame.schema)
            list_loaded += columns_selected

        if len(list_loaded)!=len(columns):
            raise ValueError(f"Columns: {set(columns)-set(list_loaded)}\n were not found in input tables. Available columns:\n {list_available}")

        parquet_schema = pa.unify_schemas(schema)
        pq_merged = pa.Table.from_arrays(columns_loaded, schema=parquet_schema)
        parquet_writer = pq.ParquetWriter(
            output_file, parquet_schema, compression="snappy"
        )
        parquet_writer.write_table(pq_merged)
        parquet_writer.close()

    else:
        raise NotImplementedError(
            f"in_format {in_format} and out_format {out_format} are not supported yet."
        )

    # elif out_format.upper() == "CSV" or out_format.upper() == "TSV":
    #     header = True
    #     for i, chunk in enumerate(input_tables):
    #         chunk.loc[:, columns].to_csv(
    #             output_file,
    #             sep="," if out_format.upper() == "CSV" else "\t",
    #             header=header,
    #             mode="a" if i != 0 else "w",
    #             index=False,
    #         )
    #         header = False
    #
    # elif out_format.upper() == "HDF5":
    #     h = h5py.File(output_file, "w")
    #     s = 0
    #     for i, chunk in enumerate(input_tables):
    #         if i == 0:
    #             for col in columns:
    #                 if pd.api.types.is_object_dtype(chunk[col]):
    #                     h.create_dataset(
    #                         col,
    #                         data=chunk[col].astype("S100"),
    #                         maxshape=(None,),
    #                         chunks=True,
    #                     )
    #                 else:
    #                     h.create_dataset(
    #                         col, data=chunk[col], maxshape=(None,), chunks=True
    #                     )
    #             s += len(chunk[col])
    #         else:
    #             for col in columns:
    #                 h[col].resize((s + len(chunk[col]),))
    #                 if pd.api.types.is_object_dtype(chunk[col]):
    #                     h[col][s : s + len(chunk[col])] = chunk[col].astype("S100")
    #                 else:
    #                     h[col][s : s + len(chunk[col])] = chunk[col]
    #             s += len(chunk[col])

    return 0
