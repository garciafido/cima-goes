import os
import io
import aioftp
import netCDF4
from cima.goes.storage._file_systems import Storage


class AFTP(Storage):
    '''
    File Transfer Protocol
    '''
    def __init__(self, host=None, port=aioftp.DEFAULT_PORT, user=aioftp.DEFAULT_USER, password=aioftp.DEFAULT_PASSWORD):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    async def list(self, path):
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            return await client.list(path)

    async def mkdir(self, path):
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            await client.make_directory(path)

    async def upload_stream(self, data, filepath):
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            path = os.path.dirname(os.path.abspath(filepath))
            await client.make_directory(path)
            async with client.upload_stream(filepath, offset=0) as stream:
                await stream.write(data)

    async def download_stream(self, filepath):
        async with aioftp.ClientSession(self.host, self.port, self.user, self.password) as client:
            async with client.download_stream(filepath, offset=0) as stream:
                in_memory_file = io.BytesIO()
                async for block in stream.iter_by_block():
                    in_memory_file.write(block)
                in_memory_file.seek(0)
                return in_memory_file.read()

    async def download_dataset(self, filepath):
        data = await self.download_stream(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)
