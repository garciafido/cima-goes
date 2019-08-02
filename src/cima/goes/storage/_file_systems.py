import abc
from enum import Enum
from typing import Dict, Tuple
from dataclasses import dataclass


class storage_type(Enum):
    NFS = 'nfs' # Local File System
    FTP = 'ftp' # File Transfer Protocol
    AFTP = 'async_ftp' # File Transfer Protocol
    GCS = 'gcs' # Google Cloud Storage


@dataclass
class storage_info:
    def __init__(self, stype: storage_type, *args, **kwargs):
        self.stype = stype
        self.args = args if args else ()
        self.kwargs = kwargs if kwargs else {}
    stype: storage_type
    args: Tuple = None
    kwargs: Dict = None


@dataclass
class goesdata_info(storage_info):
    def __init__(self, stype: storage_type, *args, **kwargs):
        self.stype = stype
        self.args = args if args else ()
        self.kwargs = kwargs if kwargs else {}
    stype: storage_type
    args: Tuple = None
    kwargs: Dict = None


class Storage(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def list(self, path):
        pass

    @abc.abstractmethod
    def mkdir(self, path):
        pass

    @abc.abstractmethod
    def upload_stream(self, data, filepath):
        pass

    @abc.abstractmethod
    def download_stream(self, filepath):
        pass
