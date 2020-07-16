import csv
import io
import gzip

# file = "./cache/GPL5.txt"

# with open(file, "r") as f:
#     for line in f:
#         line = line.strip()
#         if line == "!platform_table_begin":
#             break
#     reader = csv.reader(f, delimiter='\t')
#     for row in reader:
#         if row[0] == "!platform_table_end":
#             break
#         print(row)






def parse_soft_gz(file):

    with gzip.open(file, "rt") as f:
        #not compatible with custom buffer
        # f = io.BufferedReader(gz)
        for line in f:
            line = line.strip()
            if line == "!platform_table_begin":
                break
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            #apparently some might have extra lines? just ignore
            if len(row) == 0:
                continue
            if row[0] == "!platform_table_end":
                break
            # print(row)
        # print("end_table")

