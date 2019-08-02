import aiofiles
import netCDF4
from cima.goes.storage._file_systems import Storage, storage_type


class NFS(Storage):
    '''
    Network File System
    '''
    def __init__(self):
        self.stype = storage_type.NFS
    async def list(self, path):
        pass

    async def mkdir(self, path):
        pass

    async def upload_stream(self, data, filepath):
        async with aiofiles.open(filepath, mode='w+b') as f:
            return await f.write(data)

    async def download_stream(self, filepath):
        async with aiofiles.open(filepath, mode='r') as f:
            return await f.read()

    async def download_dataset(self, filepath):
        data = await self.download_stream(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
