#!/usr/bin/env python3

## TODO: add output formats

import click
import click_log

# Set up logging:
from .. import get_logger

logger = get_logger(__name__)

from . import read

# from ...lib import *

import numpy as np

# Read the arguments:
@read.command()
@click.argument(
    "input_fastq_table", type=click.Path(exists=True), metavar="FASTQ_TABLE"
)
@click.argument(
    "input_ref_table", type=click.Path(exists=True), metavar="REFERENCE_TABLE"
)
@click.argument("output_file", type=click.Path(exists=False), metavar="OUTPUT_TABLE")
@click.option(
    "--oligo",
    help="Sequence of oligo.",
    required=True,
)
@click.option(
    "--oligo-name",
    help="Name of oligo used for column naming. Will be set to --oligo if not provided.",
    required=False,
    default=None,
)
@click.option(
    "--readid-colname",
    help="Column name with readID in FASTQ_TABLE.",
    default=None,
    type=str,
    show_default=True,
)
@click.option(
    "--readid-column",
    help="Index of the column with readID in FASTQ_TABLE. Cannot be together with --readid-colname.",
    default=None,
    type=int,
    show_default=True,
)
@click.option(
    "--seq-colname",
    help="Column name with sequence in FASTQ_TABLE.",
    default=None,
    type=str,
    show_default=True,
)
@click.option(
    "--seq-column",
    help="Index of the column with position of oligo in FASTQ_TABLE. Cannot be together with --seq-colname.",
    default=None,
    type=int,
    show_default=True,
)
@click.option(
    "--ref-colname",
    help="Column name with position of oligo in REFERENCE_TABLE.",
    default=None,
    type=str,
    show_default=True,
)
@click.option(
    "--ref-column",
    help="Index of the column with position of oligo in REFERENCE_TABLE. Cannot be together with --ref-colname.",
    default=None,
    type=int,
    show_default=True,
)
@click.option(
    "--shift",
    help="Shift relative to position of oligo (in nucleotides). Default (35) is for bridge in RedC.",
    default=35,
    type=int,
    show_default=True,
)
# @click.option('--fastq-table-header/--no-fastq-table-header',
#               help="Flag for the header in fastq table.",
#               default=True)
# @click.option('--position-table-header/--no-position-table-header',
#               help="Flag for the header in position table.",
#               default=True)
def check_nucleotides(
    input_fastq_table,
    input_ref_table,
    oligo,
    oligo_name,
    output_file,
    readid_colname,
    readid_column,
    seq_colname,
    seq_column,
    ref_colname,
    ref_column,
    shift,
    # fastq_table_header,
    # position_table_header
):
    """
    Check that certain positions in the reads (FASTQ_TABLE) match oligo sequence
    in a certain position relative to reference position (specified as a column in REFERENCE_TABLE).

    Example use case: you have an output of oligos search that allowed mismatches, however,
    you want to make sure some positions relative to oligo starts are untouched and have no mismatches.

    This approach is used in original RedC paper, where we checked GA nucleotides at the end of bridge adaptor.

    Example usage:
    `rnadnatools read check-nucleotides --oligo GA -o tmp.txt --readid-colname readID --seq-colname R1 --reference-colname start_hit__bridge_forward_R1 --shift 35 tests/data/test-sample.table.tsv tests/data/test-sample.oligos.tsv`
    """

    # Checks the input parameters:
    if (readid_colname is None) and (readid_column is None):
        raise ValueError("Please, provide either --readid-colname or --readid-column.")
    if (readid_colname is not None) and (readid_column is not None):
        raise ValueError("--readid-colname and --readid-column cannot work together.")

    if (seq_colname is None) and (seq_column is None):
        raise ValueError("Please, provide either --seq-colname or --seq-column.")
    if (seq_colname is not None) and (seq_column is not None):
        raise ValueError("--seq-colname and --seq-column cannot work together.")

    if (ref_colname is None) and (ref_column is None):
        raise ValueError("Please, provide either --ref-colname or --ref-column.")
    if (ref_colname is not None) and (ref_column is not None):
        raise ValueError("--ref-colname and --ref-column cannot work together.")

    if oligo_name is None:
        oligo_name = oligo

    # Sniff for headers:
    if seq_colname is not None or readid_colname is not None:
        seqfile_header = open(input_fastq_table, "r").readline().strip()
        if not seqfile_header.startswith("#"):
            logger.warning(
                "Are you sure sequence table has header? Header line does not start with '#'."
            )
        else:
            seqfile_header = seqfile_header[1:]
        header = seqfile_header.split()

        if seq_colname is not None:
            seq_column = np.where(np.array(header) == seq_colname)[0]
            if len(seq_column) > 1:
                logger.warning(
                    f"Mupltiple {seq_colname} columns in input sequence table"
                )
            seq_column = seq_column[0]

        if readid_colname is not None:
            readid_column = np.where(np.array(header) == readid_colname)[0]
            if len(readid_column) > 1:
                logger.warning(
                    f"Mupltiple {readid_colname} columns in input sequence table"
                )
            readid_column = readid_column[0]

    if ref_colname is not None:
        posfile_header = open(input_ref_table, "r").readline().strip()
        if not posfile_header.startswith("#"):
            logger.warning(
                "Are you sure sequence table has header? Header line does not start with '#'."
            )
        else:
            posfile_header = posfile_header[1:]
        header = posfile_header.split()
        ref_column = np.where(np.array(header) == ref_colname)[0]
        if len(ref_column) > 1:
            logger.warning(f"Mupltiple {ref_colname} columns in input sequence table")
        ref_column = ref_column[0]

    # Read the tables, check oligonucleotides and write output:
    with open(output_file, "w") as outf:
        outf.write(f"#entry_index_{oligo_name}\toligo_{oligo_name}_at_{shift}\n")
        with open(input_fastq_table, "r") as in_f:
            with open(input_ref_table, "r") as hits_f:
                fastq_table_line = in_f.readline()
                hits_table_line = hits_f.readline()

                if fastq_table_line.startswith("#"):
                    fastq_table_line = in_f.readline()
                if hits_table_line.startswith("#"):
                    hits_table_line = hits_f.readline()

                while len(fastq_table_line) > 0:

                    # Full read sequence:
                    read = fastq_table_line.split()[seq_column]
                    # Start position of the oligo in the read:
                    oligo_position_start = int(hits_table_line.split()[ref_column])
                    # ID of the read in the table with hits:
                    idx = hits_table_line.split()[readid_column]

                    # Checking presence:
                    start_corrected = oligo_position_start + shift
                    end_corrected = oligo_position_start + shift + len(oligo)
                    # if the rightmost position of the oligo is out of the read, then no need to check at all:
                    if (end_corrected > len(read)) or (start_corrected < 0):
                        ret = 0
                    else:
                        if read[start_corrected:end_corrected] == oligo:
                            ret = 1
                        else:
                            ret = 0
                    outf.write("{}\t{}\n".format(idx, ret))

                    # Continue to the next read:
                    hits_table_line = hits_f.readline()
                    fastq_table_line = in_f.readline()

    return 0
