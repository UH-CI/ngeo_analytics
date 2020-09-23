from ftp_manager import FTPManager
from time import sleep

manager = FTPManager("ftp.ncbi.nlm.nih.gov")

cons = []
for i in range(400):
    con = manager.get_con()
    cons.append(con)
    print("Complete connection %d" % i)

for con in cons:
    manager.release_con(con)




