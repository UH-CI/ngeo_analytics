
import gzip
#fp = "c:/users/jard/downloads/gene_info.gz"
fp = "c:/users/jard/downloads/gene2accession.gz"

s = set()

with gzip.open(fp, "rt") as f:
    # headers = next(f).split("\t")
    # next(f)
    # data = next(f).split("\t")
    # d = dict(zip(headers, data))
    # print(d)
    #index 1 is gene id
    headers = next(f).split("\t")
    for header in headers:
        print(header)
    # for line in f:
    #     row = line.split("\t")
    #     if row[1] in s:
    #         print(row[1])
    #         break
    #     # print(row[1])
    #     s.add(row[1])

    print("\n\n")
    for line in f:
        row = line.strip().split("\t")
        if (row[7] == "-") != (row[5] == "-"):
            print(row)
            break

# params = ["sd", "cwsd", "csda", "er", "erw"]

# query = "INSERT INTO ? (%s)" % ",".join("?" for i in range(len(params) - 1))
# print(query)

#foreign key GeneID

