# RNA-DNA tools
Tools for RNA-DNA data manipulation.


There are stereotypical actions required for data anslysis of RNA-DNA contacts, 
such as table manipultaions, nucleotide sequence extraction, etc.
We collect all related instruments as Python-based CLI.

These tools are used in recently developed nextflow pipeline to map and analyse RNA-DNA interactions:
[https://github.com/agalitsyna/RedClib/tree/redc-nextflow-dsl2](https://github.com/agalitsyna/RedClib/tree/redc-nextflow-dsl2)


## Installation

```
git clone https://github.com/agalitsyna/rnadnatools
cd rnadnatools 
pip install -e .
```

## Key concepts

### Read
Read is a sequence of nucleotides and qualities.

Read always has attributes:
- read ID
- sequence of nucleotides
- sequence of qualities

Optional attributes are obtained after some processing: 
- read length
- positions of trimming
- start and end position of the substring (e.g. primer or bridge adaptor)

#### Read manipulations:
- `check_nucleotides` at certain positions in a read

### Segment
Segment is a continuous genomic region after mapping to the reference genome.

Segment always has: 
- chromosome, start and end positions in the reference genome
- associated read sequence

#### Segment manipulations:
- `extract_fastq` from the segments annotation
- `find_closest` restriction sites to the segments

### Extended Pair
For each read, we obtain pair (DNA-RNA) or extended pair (DNA-RNA1-RNA2) of segments.

TBA

### Table
Table is a columnar file with multiple read annotations, might include read, segment or pair attributes.
We support tsv, csv, parquet and hdf5 formats for now. 

#### Table manipulations:
- `convert` between table formats
- `evaluate` expressions treating columns as variables
- `merge` talbes into singe file

### Genome
fasta file with genomic sequence.

#### Genome manipulations:
- `renzymes_recsites` extracts recognition sites of restriction enzyme (not the same as restriction sites!)