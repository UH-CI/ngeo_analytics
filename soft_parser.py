import csv

file = "./cache/GPL5.txt"

with open(file, "r") as f:
    for line in f:
        line = line.strip()
        if line == "!platform_table_begin":
            break
    reader = csv.reader(f, delimiter='\t')
    for row in reader:
        if row[0] == "!platform_table_end":
            break
        print(row)




import io
import gzip

file = ""

with gzip.open(file, "r") as gz:
    f = io.BufferedReader(gz)
    for line in f:
        line = line.strip()
        if line == "!platform_table_begin":
            break
    reader = csv.reader(f, delimiter='\t')
    for row in reader:
        if row[0] == "!platform_table_end":
            break
        print(row)

