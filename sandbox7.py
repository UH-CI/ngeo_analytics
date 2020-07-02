
import sqlite3
s = "genedb.sqlite"


with sqlite3.connect(s) as con:
    cur = con.cursor()
    # p = "PRAGMA case_sensitive_like = on"
    q = "SELECT * FROM gene2accession WHERE protein_accession GLOB 'NP_047184[.]?*'"
    # cur.execute(p)
    cur.execute(q)
    print(cur.fetchall())

