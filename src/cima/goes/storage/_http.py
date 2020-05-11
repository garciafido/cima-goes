import io
from typing import List

import netCDF4
import urllib

from cima.goes.storage._blobs import GroupedBandBlobs, BandBlobs, GoesBlob
from cima.goes.storage._file_systems import StorageInfo, storage_type
from cima.goes.storage._file_systems import Storage
from cima.goes.utils._file_names import ProductBand, GOES_PUBLIC_BUCKET, ANY_MODE, Product, get_gcs_url, get_browse_url


class HTTP(Storage):
    '''
    Google Cloud Storage
    '''
    def __init__(self,
                 bucket: str=GOES_PUBLIC_BUCKET,
                 product: Product=Product.CMIPF,
                 mode: str = ANY_MODE):
        self.product = product
        self.mode = mode
        self.bucket = bucket

    #
    # Storage methods
    #
    def get_storage_info(self) -> StorageInfo:
        return StorageInfo(storage_type.HTTP,
                           credentials_as_dict=self.credentials_as_dict,
                           bucket=self.bucket,
                           product=self.product,
                           credentials_filepath=self.credentials_filepath)

    def get_storage_info(self) -> StorageInfo:
        raise Exception('Not implemented: mkdir')

    def list(self, path: str):
        raise Exception('Not implemented: mkdir')

    def mkdir(self, path: str):
        raise Exception('Not implemented: mkdir')

    def upload_data(self, data: bytes, filepath: str):
        raise Exception('Not implemented: mkdir')

    def upload_stream(self, stream: io.BytesIO, filepath: str):
        raise Exception('Not implemented: mkdir')

    def download_data(self, filepath: str) -> bytes:
        resp = urllib.request.urlopen(get_browse_url(filepath))
        return resp.read()

    def download_stream(self, filepath: str) -> io.BytesIO:
        raise Exception('Not implemented: mkdir')

    def get_dataset(self, filepath: str) -> netCDF4.Dataset:
        resp = urllib.request.urlopen(get_gcs_url(filepath))
        return netCDF4.Dataset("in_memory_file", mode='r', memory=resp.read())

    def append_data(self, data: bytes, filepath: str):
        raise Exception('Not implemented: upload_stream')

    def append_stream(self, stream: io.BytesIO, filepath: str):
        raise Exception('Not implemented: upload_stream')
