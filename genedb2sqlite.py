
import gzip
import sqlite3
import csv


class QueryParamGen():
    def __init__(self, source, nodata):
        self.source = source
        self.nodata = nodata

    def __next__(self):
        row = next(self.source)
        for i in range(len(row)):
            #some data seems to have weird trailing newlines
            row[i] = row[i].strip()
            if row[i] == self.nodata:
                row[i] = None
        return row



def create_tables(cur):
    #gene2accession table def
    
    #gene_info table def
    gene_info_query = """CREATE TABLE IF NOT EXISTS gene_info (
        tax_id TEXT NOT NULL,
        gene_id TEXT NOT NULL PRIMARY KEY,
        gene_symbol TEXT NOT NULL,
        locus_tag TEXT,
        synonyms TEXT,
        db_x_refs TEXT,
        chromosome TEXT,
        map_location TEXT,
        description TEXT,
        type_of_gene TEXT,
        symbol_from_nomenclature_authority TEXT,
        full_name_from_nomenclature_authority TEXT,
        nomenclature_status TEXT,
        other_designations TEXT,
        modification_date TEXT,
        feature_type TEXT
    );"""
    #execute this first for foreign key constraint
    cur.execute(gene_info_query)

    #use the set of ids as primary key (gene by itself might map to multiples and some ids might be null)
    g2a_query = """CREATE TABLE IF NOT EXISTS gene2accession (
        tax_id TEXT,
        gene_id TEXT NOT NULL,
        status TEXT,
        rna_nucleotide_accession TEXT,
        rna_nucleotide_gi TEXT,
        protein_accession TEXT,
        protein_gi TEXT,
        genomic_nucleotide_accession TEXT,
        genomic_nucleotide_gi TEXT,
        start_position_on_the_genomic_accession TEXT,
        end_position_on_the_genomic_accession TEXT,
        orientation TEXT,
        assembly TEXT,
        mature_peptide_accession TEXT,
        mature_peptide_gi TEXT,
        gene_symbol TEXT,
        FOREIGN KEY (gene_id) REFERENCES gene_info(gene_id)
        PRIMARY KEY (gene_id, rna_nucleotide_gi, protein_gi, genomic_nucleotide_gi, mature_peptide_gi)
    );"""

    cur.execute(g2a_query)
    

# def check_table_exists(cur, table_name):
#     query = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?;"
#     cur.execute(query, [table_name])
#     return cur.fetchone()[0] > 0

def drop_table(cur, table_name):
    query = "DROP TABLE %s" % table_name
    cur.execute(query)

def clear_table(cur, table_name):
    query = "DELETE FROM %s" % table_name
    cur.execute(query)

def insert_batch(cur, table_name, params, num_cols):
    query = "INSERT INTO %s VALUES (%s);" % (table_name, ", ".join("?" for i in range(num_cols)))
    print(query)
    cur.executemany(query, params)


def create_data(sqlite_file, data):

    with sqlite3.connect(sqlite_file) as con:
        cur = con.cursor()
        create_tables(cur)

        for item in data:
            table_name = item[0]
            data_file = item[1]
            zipped = item[2]

            # drop_table(cur, table_name)
            # exit()

            #make sure cleared before inserting
            clear_table(cur, table_name)

            f = gzip.open(data_file, "rt") if zipped else open(data_file, "r")

            reader = csv.reader(f, delimiter = "\t")
            header = next(reader)

            param_gen = QueryParamGen(reader, "-")
            
            insert_batch(cur, table_name, param_gen, len(header))
            
            f.close()


def main():
    data = [
        ("gene_info", "c:/users/mcleanj/downloads/gene_info", False),
        ("gene2accession", "c:/users/mcleanj/downloads/gene2accession", False)
    ]
    sqlite_file = "./genedb.sqlite"
    create_data(sqlite_file, data)



if __name__ == "__main__":
    main()