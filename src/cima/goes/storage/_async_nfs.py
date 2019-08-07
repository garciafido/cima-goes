import aiofiles
import netCDF4
from cima.goes.storage._file_systems import Storage, storage_type, storage_info


class ANFS(Storage):
    '''
    Network File System
    '''
    def __init__(self):
        self.stype = storage_type.ANFS

    def get_storage_info(self) -> storage_info:
        return storage_info(storage_type.ANFS)

    async def list(self, path):
        raise Exception('Not implemented')

    async def mkdir(self, path):
        raise Exception('Not implemented')

    async def upload_stream(self, data, filepath):
        async with aiofiles.open(filepath, mode='w+b') as f:
            return await f.write(data)

    async def download_stream(self, filepath):
        async with aiofiles.open(filepath, mode='r') as f:
            return await f.read()

    async def download_dataset(self, filepath):
        data = await self.download_stream(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
