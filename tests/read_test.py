from click.testing import CliRunner
from rnadnatools.cli import cli
import os.path as op
import pandas as pd
import numpy as np


def test_read_cli(request, tmpdir):

    outfile = op.join(tmpdir, "tmp.tsv")
    input_seqtable = op.join(request.fspath.dirname, "data/test-sample.table.tsv")
    input_postable = op.join(request.fspath.dirname, "data/test-sample.oligos.tsv")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "read",
            "check-nucleotides",
            "--oligo",
            "GA",
            "--readid-colname",
            "readID",
            "--seq-colname",
            "R1",
            "--ref-colname",
            "start_hit__bridge_forward_R1",
            "--shift",
            35,
            input_seqtable,
            input_postable,
            outfile,
        ],
    )
    assert result.exit_code == 0, result.output

    df = pd.read_csv(outfile, sep="\t")
    assert np.sum(df.loc[:, "oligo_GA_at_35"] == 1) > 0
    assert np.sum(df.loc[:, "oligo_GA_at_35"] == 0) > 0
