import abc
import io
from enum import Enum
from typing import Dict, Tuple, Union
from dataclasses import dataclass
import netCDF4


class storage_type(Enum):
    NFS = 'nfs' # Local File System
    ANFS = 'async_nfs' # Local File System
    FTP = 'ftp' # File Transfer Protocol
    AFTP = 'async_ftp' # File Transfer Protocol
    GCS = 'gcs' # Google Cloud Storage
    HTTP = 'http' # HTTP protocol


@dataclass
class StorageInfo:
    def __init__(self, stype: storage_type, *args, **kwargs):
        self.stype = stype
        self.args = args if args else ()
        self.kwargs = kwargs if kwargs else {}
    stype: storage_type
    args: Tuple = None
    kwargs: Dict = None


class Storage(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_storage_info(self) -> StorageInfo:
        pass

    @abc.abstractmethod
    def list(self, path: str):
        pass

    @abc.abstractmethod
    def mkdir(self, path: str):
        pass

    @abc.abstractmethod
    def append_data(self, data: bytes, filepath: str):
        pass

    @abc.abstractmethod
    def append_stream(self, stream: io.BytesIO, filepath: str):
        pass

    @abc.abstractmethod
    def upload_data(self, data: bytes, filepath: str):
        pass

    @abc.abstractmethod
    def upload_stream(self, stream: io.BytesIO, filepath: str):
        pass

    @abc.abstractmethod
    def download_data(self, filepath: str) -> bytes:
        pass

    @abc.abstractmethod
    def download_stream(self, filepath: str) -> io.BytesIO:
        pass

    @abc.abstractmethod
    def get_dataset(self, blob) -> netCDF4.Dataset:
        pass

