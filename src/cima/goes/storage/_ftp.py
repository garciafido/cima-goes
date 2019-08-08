import os
import io
import ftplib
import netCDF4
from cima.goes.storage._file_systems import Storage, storage_info, storage_type


class FTP(Storage):
    '''
    File Transfer Protocol
    '''
    def __init__(self, host=None, port=21, user='', password=''):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def get_storage_info(self) -> storage_info:
        return storage_info(storage_type.FTP, host=self.host, port=self.port, user=self.user, password=self.password)

    def list(self, path: str):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            return ftp.nlst(path)
        finally:
            ftp.close()

    def mkdir(self, path: str):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            ftp.mkd(path)
        finally:
            ftp.close()

    def upload_data(self, data: bytes, filepath: str):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            path = os.path.dirname(os.path.abspath(filepath))
            try:
                ftp.mkd(path)
            except Exception as e:
                pass
            stream = io.BytesIO(data)
            stream.seek(0)
            ftp.storbinary('STOR ' + filepath, stream)
        finally:
            ftp.close()

    def download_data(self, filepath: str) -> bytes:
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

    def download_dataset(self, filepath: str) -> netCDF4.Dataset:
        data = self.download_data(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
