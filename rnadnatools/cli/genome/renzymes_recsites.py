#!/usr/bin/env python3
import click
import click_log

# Set up logging:
from .. import get_logger

logger = get_logger(__name__)

from . import genome

# from ...lib import *

import numpy as np
import pandas as pd

import Bio.Restriction as biorst
from Bio.SeqIO import parse

# Read the arguments:
@genome.command()
@click.argument("genome_path", type=click.Path(exists=True))
@click.argument("restriction_enzyme", type=str)
@click.argument("output_path", type=click.Path(exists=False))
def renzymes_recsites(genome_path, restriction_enzyme, output_path):
    """
    Detect recognition sites of restriction enzymes (start, end) and report strand
    This is not the same as restriction sites, see http://biopython.org/DIST/docs/cookbook/Restriction.html#mozTocId447698

    Note that we report formal end of restriction recognition site to comply with BED format.
    """

    fasta_records = parse(genome_path, "fasta")
    enzyme = getattr(biorst, restriction_enzyme)

    rsites = []
    for seq_record in fasta_records:
        chrom = seq_record.id
        enzyme.search(seq_record.seq)
        if enzyme.is_palindromic():
            rsites.append(
                pd.DataFrame({"chrom": chrom, "site": enzyme.results, "strand": "+"})
            )
        else:
            minus = enzyme.on_minus
            rsites.append(pd.DataFrame({"chrom": chrom, "site": minus, "strand": "-"}))
            plus = np.setdiff1d(enzyme.results, minus)
            rsites.append(pd.DataFrame({"chrom": chrom, "site": plus, "strand": "+"}))

    rsites = pd.concat(rsites).sort_values(["chrom", "site"]).reset_index(drop=True)

    def detect_recognition_start(row):
        if row.strand == "+":
            return row.site - enzyme.fst5 - 1
        else:
            return row.site + enzyme.fst3 - 1

    # Retrieving actual position of recognition site
    idx_pos = rsites.strand == "+"
    rsites.loc[idx_pos, "start"] = rsites.loc[idx_pos, "site"] - enzyme.fst5 - 1
    idx_neg = rsites.strand == "-"
    rsites.loc[idx_neg, "start"] = rsites.loc[idx_neg, "site"] + enzyme.fst3 - 1

    rsites.loc[:, "end"] = rsites.start.astype(int)
    rsites.loc[:, "name"] = [f"{restriction_enzyme}_{idx+1}" for idx in rsites.index]
    rsites.loc[:, "foo"] = "."

    rsites.to_csv(
        output_path,
        sep="\t",
        columns=["chrom", "start", "end", "name", "foo", "strand"],
        header=False,
        index=False,
    )

    return 0
