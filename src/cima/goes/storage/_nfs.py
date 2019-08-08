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

    def upload_data(self, data, filepath):
        with open(filepath, mode='w+b') as f:
            f.write(data)

    def download_data(self, filepath):
        with open(filepath, mode='r') as f:
            return f.read()

    def download_dataset(self, filepath):
        data = self.download_data(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
