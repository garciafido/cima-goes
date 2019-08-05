import os
import io
import netCDF4
from collections import namedtuple
from typing import List, Dict, Tuple
import google.cloud.storage as gcs
from cima.goes.utils._file_names import ProductBand
from google.oauth2 import service_account
from cima.goes import file_pattern, path_prefix, slice_obs_start, Product, mode
from cima.goes import Band
from cima.goes.storage._file_systems import Storage, storage_type, storage_info
from cima.goes.storage._blobs import GoesBlob, GroupedBandBlobs, BandBlobs
from cima.goes.storage._goes_data import GoesStorage


# Browse: https://console.cloud.google.com/storage/browser/gcp-public-data-goes-16
GOES_PUBLIC_BUCKET = 'gcp-public-data-goes-16'


class GCS(GoesStorage):
    '''
    Google Cloud Storage
    '''
    def __init__(self, credentials_as_dict: dict=None,
                 bucket: str=GOES_PUBLIC_BUCKET,
                 product: Product=Product.CMIPF, mode: mode=mode.M3,
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
        self.mode = mode
        self.bucket = bucket

    #
    # Storage methods
    #
    def get_storage_info(self) -> storage_info:
        return storage_info(storage_type.GCS,
                 credentials_as_dict=self.credentials_as_dict,
                 bucket=self.bucket,
                 product=self.product,
                 credentials_filepath=self.credentials_filepath)

    async def list(self, path):
        return self.list_blobs(path)

    async def download_stream(self, filepath):
        return self.download_as_stream(filepath)

    async def mkdir(self, path):
        raise Exception('Not implemented: mkdir')

    async def upload_stream(self, data, filepath):
        raise Exception('Not implemented: upload_stream')

    #
    # GoesStorage methods
    #
    def get_dataset(self, blob: GoesBlob) -> netCDF4.Dataset:
        data = self.download_from_blob(blob)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)

    def grouped_one_hour_blobs(self, year: int, day_of_year: int, hour: int, product_bands: List[ProductBand]) -> List[GroupedBandBlobs]:
        band_blobs_list: List[BandBlobs] = []
        for product_band in product_bands:
            blobs = self.band_blobs(year, day_of_year, hour, product_band)
            band_blobs_list.append(BandBlobs(product_band.product, product_band.band, blobs))
        return self.group_blobs(band_blobs_list)

    def one_hour_blobs(self, year: int, day_of_year: int, hour: int, product_band: ProductBand) -> BandBlobs:
        blobs = self.band_blobs(year, day_of_year, hour, product_band)
        return BandBlobs(product_band.product, product_band.band, blobs)

    #
    # GCS methods
    #
    def download_from_blob(self, blob):
        in_memory_file = io.BytesIO()
        blob.download_to_file(in_memory_file)
        in_memory_file.seek(0)
        return in_memory_file.read()

    def download_as_stream(self, filepath):
        client = gcs.Client(credentials=self.credentials, project='')
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(filepath)
        return self.download_from_blob(blob)

    def download_dataset(self, filepath):
        data = self.download_as_stream(filepath)
        return netCDF4.Dataset("in_memory_file", mode='r', memory=data)

    def list_blobs(self, path: str):
        client = gcs.Client(credentials=self.credentials, project='')
        bucket = client.get_bucket(self.bucket)
        return bucket.list_blobs(prefix=path, delimiter='/')

    def band_blobs(self, year: int, day_of_year: int, hour: int, product_band: ProductBand) -> List[GoesBlob]:
      return self._list_blobs(
          path_prefix(year=year, day_of_year=day_of_year, hour=hour, product=product_band.product),
          [file_pattern(band=product_band.band.value, product=product_band.product, mode=self.mode)]
      )

    def group_blobs(self, band_blobs_list: List[BandBlobs]) -> List[GroupedBandBlobs]:
        blobs_by_start: Dict[str, Dict[Tuple[Product, Band], List[GoesBlob]]] = {}
        for band_blobs in band_blobs_list:
            for blob in band_blobs.blobs:
                key = blob.name[slice_obs_start(product=self.product)]
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
            for product_band, blobs in band_blobs_dict.items():
                band_blob: BandBlobs = BandBlobs(product_band[0], product_band[1], blobs)
                blobs_list.append(band_blob)
            result.append(GroupedBandBlobs(start, blobs_list))
        return result

    def get_datasets(self, year: int, day_of_year: int, hour: int, bands: List[Band]):
        blobs = self.one_hour_blobs(year, day_of_year, hour, bands)
        Datasets = namedtuple('Datasets', ['start'] + [band.name for band in bands])
        for blob in blobs:
            data = {band.name: self.get_dataset_from_blob(getattr(blob, band.name)) for band in bands}
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

    def _list_blobs(self, path: str, gcs_patterns) -> List[GoesBlob]:
        blobs = self.list_blobs(path)
        result = []
        if gcs_patterns == None or len(gcs_patterns) == 0:
            for b in blobs:
                result.append(b)
        else:
            for b in blobs:
                match = True
                for pattern in gcs_patterns:
                    if not pattern in b.path:
                        match = False
                if match:
                    result.append(b)
        return result

