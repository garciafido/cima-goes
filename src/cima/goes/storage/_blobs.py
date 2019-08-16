from dataclasses import dataclass
from typing import Union, List
from cima.goes import Product, Band

from google.cloud.storage.blob import Blob

GoesBlob = Union[Blob]


@dataclass
class BandBlobs:
    product: Product
    band: Band
    blobs: List[GoesBlob]
    subproduct: int = None


@dataclass
class GroupedBandBlobs:
    start: str
    blobs: List[BandBlobs]


