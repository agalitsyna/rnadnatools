# Manage logging
from . import get_logger
logger = get_logger(__name__)

import pyarrow as pa
import pyarrow.parquet as pq
import h5py
import pandas as pd
import numpy as np

def load_tables(in_paths, in_format):
    """
    Load multiple tables in a single list.
    TSV/CSV will be stored in memory and passed as pd.DataFrame objects,
    PARQUET and HDF5 will be passed as handlers.
    """
    input_tables = []
    for input_table in in_paths:
        if in_format.upper() == "PARQUET":
            input_tables.append(pa.parquet.read_table(input_table, memory_map=True))
        elif in_format.upper() == "HDF5":
            input_tables.append(h5py.File(input_table, "r"))
        elif in_format.upper() == "TSV":
            input_tables.append(pd.read_csv(input_table, sep="\t"))
        elif in_format.upper() == "CSV":
            input_tables.append(pd.read_csv(input_table, sep=","))
        else:
            raise ValueError(
                f"Format {in_format} is not supported, use one of: TSV, CSV, HDF5, PARQUET."
            )
    return input_tables

def dump_columns(result, in_format, column_format, column_name):
    """Dump array "result" into dictionary in required format."""

    loaded_arrays = {}

    """ Dump columns into dictionary storing in appropriate formats. """
    if in_format.upper() == "PARQUET":
        if column_format.lower() == "str":
            pyarrow_format = pa.string()
        elif column_format.lower() == "int":
            pyarrow_format = pa.int32()
        elif column_format.lower() == "int8":
            pyarrow_format = pa.int8()
        elif column_format.lower() == "int16":
            pyarrow_format = pa.int16()
        elif column_format.lower() == "int32":
            pyarrow_format = pa.int32()
        elif column_format.lower() == "bool":
            pyarrow_format = pa.bool_()
        else:
            raise ValueError("Supported formats: str, int and bool for now.")

        loaded_arrays[column_name] = pa.array(result, type=pyarrow_format)

    elif in_format.upper() == "HDF5":
        loaded_arrays[column_name] = result.copy()

    else:
        loaded_arrays[column_name] = pd.Series(
            result, dtype=column_format.lower()
        )

    return loaded_arrays

def dump_arrays(loaded_arrays, out_format, output_file):
    if out_format.upper() == "PARQUET":
        pq_table_output = pa.Table.from_pydict(loaded_arrays)
        # Write the output to the same input file:
        parquet_schema = pq_table_output.schema
        parquet_writer = pq.ParquetWriter(
            output_file, parquet_schema, compression="snappy"
        )
        parquet_writer.write_table(pq_table_output)
        parquet_writer.close()

    elif out_format.upper() == "HDF5":
        for column_name, result in loaded_arrays.iteritems():
            output_file.create_dataset(column_name, data=result)
        # Everything was stored already, just close the file handler:
        output_file.close()
    else:
        df = pd.DataFrame(loaded_arrays)
        df.to_csv(
            output_file,
            sep="\t" if out_format.upper() == "TSV" else ",",
            index=False,
        )