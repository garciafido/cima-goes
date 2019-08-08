import aiofiles
import netCDF4
from cima.goes.storage._file_systems import Storage, storage_type, StorageInfo


class ANFS(Storage):
    '''
    Network File System
    '''
    def __init__(self):
        self.stype = storage_type.ANFS

    def get_storage_info(self) -> StorageInfo:
        return StorageInfo(storage_type.ANFS)

    async def list(self, path: str):
        raise Exception('Not implemented')

    async def mkdir(self, path: str):
        raise Exception('Not implemented')

    async def upload_data(self, data: bytes, filepath: str):
        async with aiofiles.open(filepath, mode='w+b') as f:
            return await f.write(data)

    async def download_data(self, filepath: str) -> bytes:
        async with aiofiles.open(filepath, mode='r') as f:
            return await f.read()

    async def download_dataset(self, filepath: str) -> netCDF4.Dataset:
        data = await self.download_data(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
