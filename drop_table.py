
import sqlite3

db_file = "genedb.sqlite"

con = sqlite3.connect(db_file)

cur = con.cursor()

cur.execute("DROP TABLE gene2accession")