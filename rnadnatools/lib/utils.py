# Manage logging
from . import get_logger

logger = get_logger(__name__)

import pyarrow as pa
import pyarrow.parquet as pq
import h5py
import pandas as pd
import numpy as np
import csv

import re
def match(v, expr):
    r = re.compile(expr)
    matcher = np.vectorize(lambda x:bool(r.match(x)))

    return matcher(v)

def guess_format(in_path):
    """Guess the file format of in_path."""

    if h5py.is_hdf5(in_path):
        return "hdf5"
    else:
        try:
            file = open(in_path, "r")
            sample = file.read(1024)
            dialect = csv.Sniffer().sniff(sample, delimiters=[","])
            file.seek(0)
            return "csv"
        except Exception as e:
            try:
                file = open(in_path, "r")
                sample = file.read(1024)
                dialect = csv.Sniffer().sniff(sample, delimiters=["\t", " "])
                file.seek(0)
                return "tsv"
            except Exception as e:
                try:
                    pq.read_table(in_path)
                    return "parquet"
                except Exception as e:
                    return None

def update_df_chunk(input_stream, dct, key=3):
    """
    Update chunk read from instream into dct dictionary.

    Parameters
    ----------
    input_stream: stream of dataframes from file
    dct: output dictionary
    key: column of input stream dataframes that will be used as dict keys

    Returns
    -------

    """

    try:
        df = next(input_stream)
    except StopIteration as e:
        raise StopIteration("Update impossible")

    l = len(df)
    dct_tmp = df.to_dict(orient='list')
    dct_tmp = {dct_tmp[key][i]: {k: dct_tmp[k][i] for k in dct_tmp.keys()} for i in range(l)}

    dct.update(dct_tmp)

    return dct, df.columns, dict(df.dtypes)


def write_chunk(chunk, output_file, out_format, writer=None, s=None, mode='w'):
    """
    Write chunk table into output file in a desired format.
    hdf5 not tested yet.

    Parameters
    ----------
    chunk
    output_file
    out_format
    writer: writer (for hdf5 and parquet)
    s: length of written objects (for hdf5)
    mode: 'w' for writing or 'a' for appending

    Returns
    -------
    (writer, s) tuples, will be None if absent
    """
    if out_format.upper() == "PARQUET":

        pq_table_output = pa.Table.from_pandas(chunk)

        # Write the output to the same input file:
        parquet_schema = pq_table_output.schema

        if mode == 'w':
            parquet_writer = pq.ParquetWriter(
                output_file, parquet_schema, compression="snappy"
            )
        else:
            parquet_writer = writer

        parquet_writer.write_table(pq_table_output)

        return parquet_writer, None


    elif out_format.upper() == "HDF5":

        if mode == 'w':
            h = h5py.File(output_file, mode=mode)
        else:
            h = writer

        if mode == 'w':
            s = 0
            for col in chunk.columns:
                if pd.api.types.is_object_dtype(chunk[col]):
                    h.create_dataset(
                        col,
                        data=chunk[col].astype('S100').values,
                        dtype='S100',
                        maxshape=(None,),
                        chunks=True,
                    )
                else:
                    h.create_dataset(
                        col,
                        data=chunk[col].values,
                        dtype=chunk[col].dtype.type,
                        maxshape=(None,),
                        chunks=True
                    )
            s += len(chunk[col])
        else:
            for col in chunk.columns:
                h[col].resize((s + len(chunk[col]),))
                if pd.api.types.is_object_dtype(chunk[col]):
                    h[col][s: s + len(chunk[col])] = chunk[col].astype("S100")
                else:
                    h[col][s: s + len(chunk[col])] = chunk[col]
            s += len(chunk[col])

        return h, s

    else:

        chunk.to_csv(
            output_file,
            sep="," if out_format.upper() == "CSV" else "\t",
            header=True if mode == 'w' else None,
            mode=mode,
            index=False,
        )

        return None, None


def load_table(in_path,
               in_format="AUTO",
               chunksize=None,
               usecols=None,
               header=None):
    """

    Parameters
    ----------
    in_path: input file
    in_format: Type of input. Can be either "TSV", "CSV", "PARQUET", "HDF5", "AUTO"

    Returns
    -------
    iterator with chunked input tables
    """

    if in_format.upper() == "AUTO":
        in_format = guess_format(in_path)

    # Read input file:
    if in_format.upper() == "PARQUET":
        stream = [pd.read_parquet(in_path, columns=usecols)]
    elif in_format.upper() in ["TSV", "CSV"]:
        stream = pd.read_csv(in_path, sep="," if in_format.upper()=="CSV" else "\t",
                             header=header,
                             chunksize=chunksize,
                             usecols=usecols)
    elif in_format.upper() == "HDF5":
        h = h5py.File(in_path, "r")
        keys = usecols if usecols else h.keys()
        dct = {k: h[k][()] for k in keys}
        h.close()
        stream = [pd.DataFrame.from_dict(dct)]

    return stream

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
            logger.warning(
                "Loading multiple TSV tables into memory, "
                "might result in RAM overload!"
            )
            input_tables.append(pd.read_csv(input_table, sep="\t"))
        elif in_format.upper() == "CSV":
            logger.warning(
                "Loading multiple CSV tables into memory, "
                "might result in RAM overload!"
            )
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
        loaded_arrays[column_name] = pd.Series(result, dtype=column_format.lower())

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
