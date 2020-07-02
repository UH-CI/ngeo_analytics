
import sqlite3

sqlite_file = "genedb.sqlite"

with sqlite3.connect(sqlite_file) as con:
    cur = con.cursor()

    indexes = [
        "CREATE INDEX gene_id_idx ON gene2accession (gene_id)",
        "CREATE INDEX rna_nucleotide_accession_idx ON gene2accession (rna_nucleotide_accession)",
        "CREATE INDEX rna_nucleotide_gi_idx ON gene2accession (rna_nucleotide_gi)",
        "CREATE INDEX protein_accession_idx ON gene2accession (protein_accession)",
        "CREATE INDEX protein_gi_idx ON gene2accession (protein_gi)",
        "CREATE INDEX genomic_nucleotide_accession_idx ON gene2accession (genomic_nucleotide_accession)",
        "CREATE INDEX genomic_nucleotide_gi_idx ON gene2accession (genomic_nucleotide_gi)",
        "CREATE INDEX mature_peptide_accession_idx ON gene2accession (mature_peptide_accession)",
        "CREATE INDEX mature_peptide_gi_idx ON gene2accession (mature_peptide_gi)",
    ]

    for index in indexes:
        cur.execute(index)