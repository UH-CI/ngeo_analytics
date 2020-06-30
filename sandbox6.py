
fp = "c:/users/jard/downloads/gene_info"

with open(fp, "r") as f:
    i = 0
    for line in f:
        print(line)
        if i > 2:
            break
        i += 1

