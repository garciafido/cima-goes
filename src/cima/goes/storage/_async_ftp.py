import os
import io
import aioftp
import netCDF4
from cima.goes.storage._file_systems import Storage, StorageInfo, storage_type


class AFTP(Storage):
    '''
    File Transfer Protocol
    '''
    def __init__(self, host=None, port=aioftp.DEFAULT_PORT, user=aioftp.DEFAULT_USER, password=aioftp.DEFAULT_PASSWORD):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def get_storage_info(self) -> StorageInfo:
        return StorageInfo(storage_type.AFTP, host=self.host, port=self.port, user=self.user, password=self.password)

    async def list(self, path: str):
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            return await client.list(path)

    async def mkdir(self, path: str):
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            await client.make_directory(path)

    async def upload_stream(self, data: io.BytesIO, filepath: str):
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            path = os.path.dirname(os.path.abspath(filepath))
            await client.make_directory(path)
            async with client.upload_stream(filepath, offset=0) as stream:
                await stream.write(data)

    async def upload_data(self, data: bytes, filepath: str):
        in_memory = io.BytesIO(data)
        await self.upload_stream(in_memory, filepath)

    async def download_data(self, filepath: str) -> bytes:
        stream = await self.download_stream(filepath)
        return stream.read()

    async def download_stream(self, filepath: str) -> io.BytesIO:
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            async with client.download_data(filepath, offset=0) as stream:
                in_memory_file = io.BytesIO()
                async for block in stream.iter_by_block():
                    in_memory_file.write(block)
                in_memory_file.seek(0)
                return in_memory_file

    async def download_dataset(self, filepath: str) -> netCDF4.Dataset:
        data = await self.download_data(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)

    async def append_data(self, data: bytes, filepath: str):
        raise Exception('Not implemented: upload_stream')

    async def append_stream(self, stream: io.BytesIO, filepath: str):
        raise Exception('Not implemented: upload_stream')
