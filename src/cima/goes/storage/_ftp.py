import os
import io
import ftplib
import netCDF4
from cima.goes.storage._file_systems import Storage, StorageInfo, storage_type


class FTP(Storage):
    '''
    File Transfer Protocol
    '''
    def __init__(self, host=None, port=21, user='', password=''):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def get_storage_info(self) -> StorageInfo:
        return StorageInfo(storage_type.FTP, host=self.host, port=self.port, user=self.user, password=self.password)

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

    def try_create_path(self, ftp, path):
        parts = path.split('/')
        if path[0] == '/':
            ftp.cwd('/')
        for part in parts:
            try:
                ftp.cwd(part)
            except Exception as e:
                ftp.mkd(part)
                ftp.cwd(part)

    def upload_data(self, data: bytes, filepath: str, override: bool = True):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            path = os.path.dirname(os.path.abspath(filepath))
            try:
                self.try_create_path(ftp, path)
            except Exception as e:
                pass
            if override:
                try:
                    ftp.delete(filepath)
                except:
                    pass
            ftp.storbinary('STOR ' + filepath, data)
        finally:
            ftp.close()

    def upload_stream(self, stream: io.BytesIO, filepath: str, override: bool = True):
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            path = os.path.dirname(os.path.abspath(filepath))
            try:
                ftp.mkd(path)
            except Exception as e:
                pass
            stream.seek(0)
            if override:
                try:
                    ftp.delete(filepath)
                except:
                    pass
            ftp.storbinary('STOR ' + filepath, stream)
        finally:
            ftp.close()

    def download_data(self, filepath: str) -> bytes:
        stream = self.download_stream(filepath)
        return stream.read()

    def download_stream(self, filepath: str) -> io.BytesIO:
        ftp = ftplib.FTP()
        try:
            ftp.connect(host=self.host, port=self.port)
            ftp.login(user=self.user, passwd=self.password)
            in_memory_file = io.BytesIO()
            ftp.retrbinary('RETR ' + filepath, lambda block: in_memory_file.write(block))
            in_memory_file.seek(0)
            return in_memory_file
        finally:
            ftp.close()

    def download_dataset(self, filepath: str) -> netCDF4.Dataset:
        data = self.download_data(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
