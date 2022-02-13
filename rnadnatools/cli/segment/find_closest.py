#!/usr/bin/env python3
import click
import click_log

# Set up logging:
from .. import get_logger

logger = get_logger(__name__)

from . import segment

from ...lib import *

import numpy as np
import pandas as pd


# Read the arguments:
@segment.command()
@click.argument("input_file", metavar="BED_INPUT", type=click.Path(exists=True))
@click.argument("reference_file", metavar="RSITES_REFERENCE", type=click.Path(exists=True))
@click.argument("output_file", metavar="OUTPUT_FILE", type=click.Path(exists=False))
@click.option(
    "-s",
    "--strand",
    help="Strand to search for restriction sites: '+' plus strand, '-' minus strand or 'b' - both strands. ",
    default="b",
    show_default=True,
    type=click.Choice(["+", "-", "b"], case_sensitive=False),
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
    "--key-columns", "--key-colnames",
    help="IDs of the columns in BED_INPUT file with chrom-start-end. Can be either string or integer.",
    default='chrom,start,end',
    type=str,
    show_default=True,
)
@click.option(
    "--ref-columns", "--ref-colnames",
    help="IDs of the columns in RSITES_REFERENCE reference file with chrom-start-strand. Can be either string or integer.",
    default='chrom,start,strand',
    type=str,
    show_default=True,
)
@click.option(
    "-c",
    "--output-columns",
    help="Name of columns with sites positions for reporting in the output. "
         "Should be in the default order: "
         "start of left closest site, end of left closest, "
         "start of right closest, end of right closest. "
         "Optional. ",
    default="start_left,end_left,start_right,end_right",
    type=str,
    show_default=True,
)
@click.option(
    '-c',
    "--chunksize",
    help="Chunksize for loading the input (not supported for HDF5 and PARQUET for now).",
    default=1_000_000,
    type=int,
    show_default=True,
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
def get_closest_sites(
        input_file,
        reference_file,
        output_file,
        key_columns,
        ref_columns,
        in_format,
        ref_format,
        out_format,
        strand,
        output_columns,
        chunksize,
        input_header,
        ref_header
):
    """
    Get distances to the closest sites to the start and end of mapped reads.
    Input:
     1) BED table (note that is should be are already aligned by the key)
     2) table with restriction sites (or any other genome annotation) with no header,
        with the columns: ["chrom", "start", "end", "name", "_", "strand"]
    Output file format: tsv file with fields:
    read id,
    distance to the closest site (strand-specific) to the read start from the left,
    distance to the closest site to the read start from the right,
    distance to the closest site to the read end from the left,
    distance to the closest site to the read end from the right.
    If the read is located before the first site or after the last site in the chromosome,
    then the reported distance is artificially large (1e10).
    """

    # Guess format if not specified:
    if in_format.upper() == "AUTO":
        in_format = utils.guess_format(input_file)
    if ref_format.upper() == "AUTO":
        ref_format = utils.guess_format(reference_file)
    if out_format.upper() == "AUTO":
        out_format = in_format

    # Column names for input and reference:
    key_columns = [int(key_column) if key_column.isdigit() else key_column for key_column in key_columns.split(',')]
    ref_columns = [int(ref_column) if ref_column.isdigit() else ref_column for ref_column in ref_columns.split(',')]
    if len(ref_columns) != 3:
        raise ValueError("Please, provide 3 reference columns for chrom, start, strand.")
    if len(key_columns) != 3:
        raise ValueError("Please, provide 3 input columns for chrom, start, end.")

    if output_columns:
        output_columns = output_columns.split(',')
        if len(output_columns) != 4:
            raise ValueError(f"Provide 4 output colnames, not: {output_columns}.")

    # Define input stream:
    input_stream = utils.load_table(input_file,
                                    in_format,
                                    usecols=key_columns,
                                    chunksize=chunksize,
                                    header=0 if input_header else None)

    # Define reference stream:
    reference_stream = utils.load_table(reference_file,
                                        ref_format,
                                        usecols=ref_columns,
                                        chunksize=None,
                                        header=0 if ref_header else None)
    rsites = reference_stream
    rsites.columns = ["chrom", "start", "strand"]
    if strand != "b":
        rsites = rsites.loc[rsites.strand == strand, :].sort_values(["chrom", "start"])
    else:
        rsites = rsites.sort_values(["chrom", "start"])
    rsited_dct = {} # Dictionary easy to access
    for name, gr in rsites.groupby("chrom"):
        rsited_dct[name] = {}
        for k in gr.columns:
            rsited_dct[name][k] = gr[k].values

    mode = 'w'
    for df_bed in input_stream:
        df_bed.columns = ["chrom", "start", "end"]

        # Convert dataframe to more effective numpy arrays:
        chs = df_bed.chrom.values.astype(str)
        # ids = df_bed.read_id.values.astype(str)
        bgn = df_bed.start.values.astype(int)
        end = df_bed.end.values.astype(int)
        l = len(chs)

        # Numpy checks, more effective than pandas:
        dct = {}
        for k in ["start_left", "start_right", "end_left", "end_right"]:
            dct[k] = np.full(l, -1).astype(int)

        for ch in rsited_dct.keys():
            rs = np.concatenate(
                [[-1e10], rsited_dct[ch]["start"], [1e10]]
            )
            mask = chs == ch

            idx = np.digitize(bgn[mask], rs)
            bgns = rs[idx - 1]
            ends = rs[idx]
            dct["start_left"][mask] = bgns - bgn[mask]
            dct["start_right"][mask] = ends - bgn[mask]

            idx = np.digitize(end[mask], rs)
            bgns = rs[idx - 1]
            ends = rs[idx]
            dct["end_left"][mask] = bgns - end[mask]
            dct["end_right"][mask] = ends - end[mask]

        # Write the results for a chunk:
        dump = pd.DataFrame(dct)
        dump = dump.loc[:, ["start_left", "start_right", "end_left", "end_right"]]  # preserve order
        dump = dump.astype(int)  # preserve data type

        if output_columns:  # Add ned names of columns:
            dump.columns = output_columns

        # Writing for the first time:
        if mode == 'w':
            writer, s = utils.write_chunk(dump, output_file, out_format, mode='w')
            mode = 'a'

        # Appending:
        else:
            writer, s = utils.write_chunk(dump, output_file, out_format, writer, s, mode='a')

    return 0
