






import ftplib


class FTPStreamException(Exception):
    pass
class FTPStreamException2(Exception):
    pass

print(ftplib.all_errors)

try:
    raise FTPStreamException("test")
except ftplib.all_errors + (FTPStreamException,) as e:
    print(e)