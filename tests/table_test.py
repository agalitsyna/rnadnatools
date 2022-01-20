from click.testing import CliRunner
from rnadnatools.cli import cli
import os.path as op
import pandas as pd
import numpy as np

def test_table_cli(request, tmpdir):

    outfile = op.join(tmpdir, 'tmp.tsv')
    input_scheme = op.join(request.fspath.dirname, "data/test_evaluation_scheme.tsv")
    input_table = op.join(request.fspath.dirname, "data/test_table.tsv")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "table",
            "evaluate",
            "-i",
            "TSV",
            "-o",
            "TSV",
            input_scheme,
            outfile,
            input_table
        ],
    )
    assert result.exit_code == 0, result.output

    df = pd.read_csv(outfile, sep='\t')
    assert np.allclose( df.loc[:, "eq_start"].values, np.array([False, True, False]) ) # See tests/data/test_table.tsv
    assert np.allclose( df.loc[:, "flipped_dna_start"].values, np.array([100, 10, 0]) )
