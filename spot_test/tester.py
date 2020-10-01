import gzip
import csv

table_start = "!platform_table_begin"
table_end = "!platform_table_end"

with gzip.open("GPL1003_family.soft.gz", "rt", encoding = "utf-8", errors = "ignore") as f:
    #read past header data to table
    for line in f:
        line = line.strip()
        if line == table_start:
            break
    reader = csv.reader(f, delimiter='\t')
    header = None
    for row in reader:
        #apparently some might have extra lines? just ignore
        if len(row) == 0:
            continue
        if row[0] == table_end:
            break
        #haven't gotten header yet, this row should be the header
        if header is None:
            header = row
        else:
            print(row)