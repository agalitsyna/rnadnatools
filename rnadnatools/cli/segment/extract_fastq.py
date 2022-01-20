#!/usr/bin/env python3
import click
import click_log

# Set up logging:
from .. import get_logger

logger = get_logger(__name__)

from . import segment

from ...lib import utils

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import ast

# Read the arguments:
@segment.command()
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
    '-s',
    "--selection-expression",
    help="Expression that will be used for filtering the output",
    default=None,
    show_default=True,
)
@click.option(
    "--key-start",
    help="Column name for the sequence start",
    default="dna_start",
    show_default=True,
)
@click.option(
    "--key-end",
    help="Column name for the sequence start",
    default="dna_end",
    show_default=True,
)
@click.option(
    "--key-readid",
    help="Column name for the readID",
    default="readID",
    show_default=True,
)
@click.option(
    "--key-seq",
    help="Column name for read nucleotide sequence",
    default="R1",
    show_default=True,
)
@click.option(
    "--key-qual",
    help="Column name for read quality sequence",
    default="Q1",
    show_default=True,
)
def extract_fastq(
    in_paths, output_file, in_format, selection_expression,
    key_start, key_end, key_readid, key_seq, key_qual
):
    """Convert table to fastq file.
    The result of evaluation should be a vector of type column_format with the number of entries equal to the input size of array columns.

    Example usage:
    `rnadnatools segment extract-fastq -s "dna_end-dna_start>14" -i PARQUET tmp.fq test-sample_01.fragments.pq test-sample_01.table.tsv.pq`
    """

    import numpy as np # Import is within the function to guarantee the visibility in vars()

    input_tables = utils.load_tables(in_paths, in_format)

    if selection_expression is not None:
        syntax_tree = ast.parse(selection_expression)
        additional_vars = [node.id for node in ast.walk(syntax_tree) if type(node) is ast.Name]
    else:
        additional_vars = []

    loaded_ids = []

    for k in [key_start, key_end, key_readid, key_seq, key_qual] + additional_vars:
        is_found = False
        for table in input_tables:
            if in_format.upper() == "PARQUET":
                if k in table.column_names:
                    vars()[k] = table[k].to_numpy()
                    is_found = True
            elif in_format.upper() == "HDF5":
                if k in table.keys():
                    vars()[k] = table[k].values()
                    is_found = True
            else:
                if k in table.columns:
                    vars()[k] = table.loc[:, k]
                    is_found = True
            if is_found:
                loaded_ids.append(str(k))
                break
        if not is_found:
            if in_format.upper() == "PARQUET":
                avail_colnames = [list(table.column_names) for table in input_tables]
            elif in_format.upper() == "HDF5":
                avail_colnames = [list(table.keys()) for table in input_tables]
            else:
                avail_colnames = [list(table.columns) for table in input_tables]
            raise ValueError(
                f"Variable {k} is not available from input/created pyarrow file. "
                f"List of variables that can be loaded:\n{str(avail_colnames)}"
            )

    if selection_expression is not None:
        mask = eval(selection_expression)
    else:
        mask = np.ones(len(vars()[key_readid]))
    selected = np.where(mask)[0]

    with open(output_file, "w") as outf:
        readIDs = vars()[key_readid]
        seqs = vars()[key_seq]
        quals = vars()[key_qual]
        starts = vars()[key_start]
        ends = vars()[key_end]
        for i in selected:
            outf.write(readIDs[i] + "\n")  # Sequence name
            outf.write(seqs[i][starts[i]: ends[i]] + "\n")  # Sequence
            outf.write("+\n")
            outf.write(quals[i][starts[i]: ends[i]] + "\n")  # Qualities

    logger.info(f"Done writing {len(selected)} sequences into {output_file} !")

    return 0
