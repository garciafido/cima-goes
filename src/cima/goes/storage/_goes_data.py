import abc
from typing import List, Union
from cima.goes.storage._blobs import GroupedBandBlobs, GoesBlob, BandBlobs
from cima.goes.storage._file_systems import Storage
from cima.goes import ProductBand
from netCDF4 import Dataset


class GoesStorage(Storage):
    @abc.abstractmethod
    def grouped_one_hour_blobs(self, year: int, month: int, day: int, hour: int, bands: List[ProductBand]) -> List[GroupedBandBlobs]:
        pass

    @abc.abstractmethod
    def one_hour_blobs(self, year: int, month: int, day: int, hour: int, product_band: ProductBand) -> BandBlobs:
        pass

    @abc.abstractmethod
    def grouped_one_day_blobs(self, year: int, month: int, day: int, hours: List[int], bands: List[ProductBand]) -> List[GroupedBandBlobs]:
        pass

    @abc.abstractmethod
    def one_day_blobs(self, year: int, month: int, day: int, hours: List[int], product_band: ProductBand) -> BandBlobs:
        pass

    @abc.abstractmethod
    def get_blob(self, name: str) -> GoesBlob:
        pass
