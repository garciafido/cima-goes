import os
import io
import netCDF4
from cima.goes.storage._file_systems import Storage, storage_type, StorageInfo


class NFS(Storage):
    '''
    Network File System
    '''
    def __init__(self):
        self.stype = storage_type.NFS

    def list(self, path):
        raise Exception('Not implemented')

    def get_storage_info(self) -> StorageInfo:
        return StorageInfo(storage_type.NFS)

    def mkdir(self, path):
        raise Exception('Not implemented')

    def download_stream(self, path):
        raise Exception('Not implemented')

    def upload_stream(self, stream, filepath):
        self.upload_data(stream.read(), filepath)

    def upload_data(self, data, filepath):
        directory = os.path.dirname(filepath)
        if not directory:
            directory = './'
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(filepath, mode='w+b') as f:
            f.write(data)

    def download_data(self, filepath):
        with open(filepath, mode='rb') as f:
            return f.read()

    def get_dataset(self, filepath):
        data = self.download_data(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)

    def append_data(self, data: bytes, filepath: str):
        raise Exception('Not implemented: upload_stream')

    def append_stream(self, stream: io.BytesIO, filepath: str):
        raise Exception('Not implemented: upload_stream')
