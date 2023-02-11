from ftplib import FTP
ftp = FTP('ftp.mtps.gov.br')
ftp.login()

ftp.retrlines('LIST') # ls
ftp.cwd('pdet') # cd

ftp.nlst("microdados") # return a list python of files and dirs

