import abc
from typing import List
from cima.goes.storage._blobs import GroupedBandBlobs, GoesBlob, BandBlobs
from cima.goes.storage._file_systems import Storage
from cima.goes import ProductBand
from netCDF4 import Dataset


class GoesStorage(Storage):
    @abc.abstractmethod
    def grouped_one_hour_blobs(self, year: int, day_of_year: int, hour: int, bands: List[ProductBand]) -> List[GroupedBandBlobs]:
        pass

    @abc.abstractmethod
    def one_hour_blobs(self, year: int, day_of_year: int, hour: int, product_band: ProductBand) -> BandBlobs:
        pass

    @abc.abstractmethod
    def get_dataset(self, blob: GoesBlob) -> Dataset:
        pass
