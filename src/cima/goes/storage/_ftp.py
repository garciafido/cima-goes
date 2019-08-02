import os
import io
import ftplib
import netCDF4
from cima.goes.storage._file_systems import Storage


class FTP(Storage):
    '''
    File Transfer Protocol
    '''
    def __init__(self, host=None, port=21, user='', password=''):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def list(self, path):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            return ftp.nlst(path)
        finally:
            ftp.close()

    def mkdir(self, path):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            ftp.mkd(path)
        finally:
            ftp.close()

    def upload_stream(self, data, filepath):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            path = os.path.dirname(os.path.abspath(filepath))
            try:
                ftp.mkd(path)
            except Exception as e:
                pass
            ftp.storbinary('STOR ' + filepath, data)
        finally:
            ftp.close()

    def download_stream(self, filepath):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            in_memory_file = io.BytesIO()
            ftp.retrbinary('RETR ' + filepath, lambda block: in_memory_file.write(block))
            in_memory_file.seek(0)
            return in_memory_file.read()
        finally:
            ftp.close()

    def download_dataset(self, filepath):
        data = self.download_stream(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
