import os
import io
import netCDF4
from collections import namedtuple
from typing import List, Dict, Tuple
import google.cloud.storage as gcs
from google.auth.credentials import AnonymousCredentials
from cima.goes.utils._file_names import ProductBand, GOES_PUBLIC_BUCKET
from google.oauth2 import service_account
from cima.goes.utils._file_names import file_regex_pattern, path_prefix, slice_obs_start, Product
from cima.goes.utils._file_names import day_path_prefix, hour_file_regex_pattern
from cima.goes import Band, ANY_MODE
from cima.goes.storage._file_systems import Storage, storage_type, StorageInfo
from cima.goes.storage._blobs import GoesBlob, GroupedBandBlobs, BandBlobs
from cima.goes.storage._goes_data import GoesStorage


class GCS(GoesStorage):
    '''
    Google Cloud Storage
    '''
    def __init__(self, credentials_as_dict: dict=None,
                 bucket: str=GOES_PUBLIC_BUCKET,
                 product: Product=Product.CMIPF, mode: str = ANY_MODE,
                 subproduct: int = None,
                 credentials_filepath: str=None):
        self.credentials_as_dict = credentials_as_dict
        self.credentials_filepath = credentials_filepath
        if credentials_as_dict is not None:
            self.set_credentials_dict(credentials_as_dict)
        else:
            self.credentials = None
            if credentials_filepath is not None:
                self.set_credentials(credentials_filepath)
        self.product = product
        self.subproduct = subproduct
        self.mode = mode
        self.bucket = bucket

    #
    # Storage methods
    #
    def get_storage_info(self) -> StorageInfo:
        return StorageInfo(storage_type.GCS,
                           credentials_as_dict=self.credentials_as_dict,
                           bucket=self.bucket,
                           product=self.product,
                           credentials_filepath=self.credentials_filepath)

    def list(self, path):
        return self.list_blobs(path)

    def download_data(self, filepath):
        return self.download_as_stream(filepath)

    def download_stream(self, filepath: str):
        return self.download_as_stream(filepath)

    def mkdir(self, path):
        raise Exception('Not implemented: mkdir')

    def upload_data(self, data, filepath):
        raise Exception('Not implemented: upload_data')

    def upload_stream(self, stream: io.BytesIO, filepath: str):
        raise Exception('Not implemented: upload_stream')

    def append_data(self, data: bytes, filepath: str):
        raise Exception('Not implemented: upload_stream')

    def append_stream(self, stream: io.BytesIO, filepath: str):
        raise Exception('Not implemented: upload_stream')

    #
    # GoesStorage methods
    #
    def get_client(self):
        if self.credentials_as_dict:
            client = gcs.Client(project="<none>", credentials=self.credentials)
        else:
            client = gcs.Client(project="<none>", credentials=AnonymousCredentials())
        client.project = None
        return client

    def get_dataset(self, blob: GoesBlob) -> netCDF4.Dataset:
        data = self.download_from_blob(blob)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)

    def grouped_one_hour_blobs(self, year: int, month: int, day: int, hour: int, product_bands: List[ProductBand]) -> List[GroupedBandBlobs]:
        band_blobs_list: List[BandBlobs] = []
        for product_band in product_bands:
            blobs = self.band_blobs(year, month, day, hour, product_band)
            band_blobs_list.append(BandBlobs(product_band.product, product_band.band, blobs, subproduct=product_band.subproduct))
        return self.group_blobs(band_blobs_list)

    def one_hour_blobs(self, year: int, month: int, day: int, hour: int, product_band: ProductBand) -> BandBlobs:
        blobs = self.band_blobs(year, month, day, hour, product_band)
        return BandBlobs(product_band.product, product_band.band, blobs, subproduct=product_band.subproduct)

    def grouped_one_day_blobs(self, year: int, month: int, day: int, hours: List[int], product_bands: List[ProductBand]) -> List[GroupedBandBlobs]:
        band_blobs_list: List[BandBlobs] = []
        for product_band in product_bands:
            blobs = self.day_band_blobs(year, month, day, hours, product_band)
            band_blobs_list.append(BandBlobs(product_band.product, product_band.band, blobs, subproduct=product_band.subproduct))
        return self.group_blobs(band_blobs_list)

    def one_day_blobs(self, year: int, month: int, day: int, hours: List[int], product_band: ProductBand) -> BandBlobs:
        blobs = self.day_band_blobs(year, month, day, hours, product_band)
        return BandBlobs(product_band.product, product_band.band, blobs, subproduct=product_band.subproduct)

    #
    # GCS methods
    #
    def download_from_blob(self, blob):
        in_memory_file = io.BytesIO()
        blob.download_to_file(in_memory_file)
        in_memory_file.seek(0)
        return in_memory_file.read()

    def download_as_stream(self, filepath):
        client = self.get_client()
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(filepath)
        return self.download_from_blob(blob)

    def download_dataset(self, filepath):
        data = self.download_as_stream(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)

    def list_blobs(self, path: str, delimiter='/'):
        client = self.get_client()
        bucket = client.get_bucket(self.bucket)
        return bucket.list_blobs(prefix=path, delimiter=delimiter)

    def get_blob(self, name: str):
        client = self.get_client()
        bucket = client.get_bucket(self.bucket)
        return bucket.blob(name)

    def band_blobs(self, year: int, month: int, day: int, hour: int, product_band: ProductBand) -> List[GoesBlob]:
        return self._list_blobs(
            path_prefix(year=year, month=month, day=day, hour=hour, product=product_band.product),
            [file_regex_pattern(band=product_band.band, product=product_band.product, mode=self.mode, subproduct=product_band.subproduct)]
        )

    def day_band_blobs(self, year: int, month: int, day: int, hours: List[int], product_band: ProductBand) -> List[GoesBlob]:
        return self._list_blobs(
            day_path_prefix(year=year, month=month, day=day, product=product_band.product),
            [hour_file_regex_pattern(hour=hour, band=product_band.band, product=product_band.product, mode=self.mode, subproduct=product_band.subproduct) for hour in hours],
            delimiter=None
        )

    def group_blobs(self, band_blobs_list: List[BandBlobs]) -> List[GroupedBandBlobs]:
        blobs_by_start: Dict[str, Dict[Tuple[Product, Band], List[GoesBlob]]] = {}
        for band_blobs in band_blobs_list:
            for blob in band_blobs.blobs:
                key = blob.name[slice_obs_start(product=band_blobs.product, subproduct=band_blobs.subproduct)]
                if key not in blobs_by_start:
                    blobs_by_start[key] = {(band_blobs.product, band_blobs.band): [blob]}
                else:
                    band_key = (band_blobs.product, band_blobs.band)
                    if band_key not in blobs_by_start[key]:
                        blobs_by_start[key][band_key] = [blob]
                    else:
                        blobs_by_start[key][band_key].append(blob)
        result: List[GroupedBandBlobs] = []
        for start, band_blobs_dict in blobs_by_start.items():
            blobs_list: List[BandBlobs] = []
            for pband, blobs in band_blobs_dict.items():
                band_blob: BandBlobs = BandBlobs(pband[0], pband[1], blobs)
                blobs_list.append(band_blob)
            result.append(GroupedBandBlobs(start, blobs_list))
        return result

    def get_datasets(self, year: int, month: int, day: int, hour: int, bands: List[Band]):
        blobs = self.one_hour_blobs(year, month, day, hour, bands)
        Datasets = namedtuple('Datasets', ['start'] + [band.name for band in bands])
        for blob in blobs:
            data = {band.name: self.get_dataset(getattr(blob, band.name)) for band in bands}
            yield Datasets(start=blob.start, **data)

    def close_dataset(self, dataset):
        dataset.close()

    def close_datasets(self, datasets):
        for k in datasets._fields:
            if k != 'start':
                self.close_dataset(datasets._asdict()[k])

    def set_credentials(self, filepath: str):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = filepath

    def set_credentials_dict(self, credentials_as_dict: dict):
        self.credentials = service_account.Credentials.from_service_account_info(credentials_as_dict)

    def _list_blobs(self, path: str, gcs_patterns, delimiter='/') -> List[GoesBlob]:
        blobs = self.list_blobs(path, delimiter=delimiter)
        result = []
        if gcs_patterns is None or len(gcs_patterns) == 0:
            for blob in blobs:
                result.append(blob)
        else:
            for blob in blobs:
                for pattern in gcs_patterns:
                    if pattern.search(blob.name):
                        result.append(blob)
        return result

