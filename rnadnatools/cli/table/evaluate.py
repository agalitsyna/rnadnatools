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
@click.argument("column_schema", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(exists=False))
@click.argument("in_paths", nargs=-1, type=click.Path(exists=True))
@click.option(
    '-i',
    "--in-format",
    help="Type of input. Optional.",
    default="PARQUET",
    type=click.Choice(["TSV", "PARQUET", "HDF5"], case_sensitive=False),
    show_default=True,
)
@click.option(
    '-o',
    "--out-format",
    help="Type of output. Optional.",
    default="PARQUET",
    type=click.Choice(["TSV", "PARQUET", "HDF5"], case_sensitive=False),
    show_default=True,
)
def evaluate(column_schema, output_file, in_paths, in_format, out_format):
    """Create new columns according to the input expression.
    The result of evaluation will be a vector of type column_format with the number of
    entries equal to the input size of array columns.

    Column schema should be tab-separated and contain three columns:
    column_name, column_format and column_expression.

    **Column_name** is the name of the output column with evaluation result.
    **Column expression** is one-liner that does not contain lambda expressions,
    list comprehensions, and can use only column names from input parquets as variables,
    built-in functions and numpy for their evaluation.
    **Column format** is one of the following: str, int, int8, int16, int32, bool.
    """

    import numpy as np  # Import is within the function to guarantee the visibility in vars()

    prohibited_symbols = [":", ".", "-", "/", "!", "?", "&", "|", "'", "%", "@"]

    input_tables = utils.load_tables(in_paths, in_format)

    if out_format.upper() == "HDF5":
        h = h5py.File(output_file, "w")

    loaded_arrays = {}
    n_evaluated = 0
    with open(column_schema, "r") as input_file:
        # Iterate over each expression:
        for line in input_file.readlines():
            column_name, column_format, column_expression = line.strip().split("\t")

            assert np.all(
                [x not in column_name for x in prohibited_symbols]
            ), "Check the column name. It cannot contain " + ",".join(
                prohibited_symbols
            )

            # Load dataset keys as variables:
            loaded_ids = []
            syntax_tree = ast.parse(column_expression)
            for node in ast.walk(syntax_tree):
                if type(node) is ast.Name:
                    # The element is not loaded yet and is not a builtin name:
                    if (node.id not in list(vars().keys())) and (
                        node.id not in dir(__builtins__)
                    ):
                        # Element is already loaded:
                        if node.id in loaded_arrays.keys():
                            if in_format.upper() == "PARQUET":
                                vars()[node.id] = loaded_arrays[node.id].to_numpy()
                            elif in_format.upper() == "HDF5":
                                vars()[node.id] = loaded_arrays[node.id][()]
                            else:
                                vars()[node.id] = loaded_arrays[node.id]

                            loaded_ids.append(str(node.id))

                        # Element is not loaded, check the input tables:
                        else:
                            is_found = False
                            for table in input_tables:
                                if in_format.upper() == "PARQUET":
                                    if node.id in table.column_names:
                                        vars()[node.id] = table[node.id].to_numpy()
                                        is_found = True
                                elif in_format.upper() == "HDF5":
                                    if node.id in table.keys():
                                        vars()[node.id] = table[node.id][()]
                                        is_found = True
                                else:
                                    if node.id in table.columns:
                                        vars()[node.id] = table.loc[:, node.id]
                                        is_found = True
                                if is_found:
                                    loaded_ids.append(str(node.id))
                                    break

                            if not is_found:
                                if in_format.upper() == "PARQUET":
                                    avail_colnames = [list(table.column_names) for table in input_tables] + [list(loaded_arrays.keys())]
                                elif in_format.upper() == "HDF5":
                                    avail_colnames = [list(table.keys()) for table in input_tables] + [list(loaded_arrays.keys())]
                                else:
                                    avail_colnames = [list(table.columns) for table in input_tables] + [list(loaded_arrays.keys())]
                                raise ValueError(
                                    f"Variable {node.id} is not available from input/created pyarrow file. "
                                    f"List of variables that can be loaded:\n{ str( avail_colnames ) }"
                                )


            # Evaluate expression:
            logger.debug(f"Evaluating column: {column_name}, expression: {column_expression} ")
            result = eval(column_expression)

            # Remove unused variables:
            for v in loaded_ids:
                del vars()[v]

            loaded_arrays.update(
                utils.dump_columns(result, in_format, column_format, column_name)
            )
            n_evaluated += 1

    if n_evaluated > 0:
        logger.info(
            f"Evaluated {n_evaluated} expressions, including columns: {', '.join(loaded_arrays.keys())}"
        )

        utils.dump_arrays(loaded_arrays, out_format, h if in_format.upper()=="HDF5" else output_file)

    else:
        logger.info("No evaluated expression. Is the input table with expressions empty?")

    return 0
