from typing import List

from cima.goes import ProductBand, Product, Band
from cima.goes.tiles import find_dataset_area, LatLonArea, dataset_area_as_dict, expand_area, band_key_as_string
from cima.goes.tiles import get_dataset_key
from cima.goes.tiles import get_tiles
from cima.goes.utils import timeit
from cima.goes.tiles import save_tiles, save_areas
from cima.goes.storage import GoesStorage

area = LatLonArea(
        lat_south=-45,
        lat_north=-20,
        lon_west=-75,
        lon_east=-45,
    )

lon_excess = 0.5
lat_excess = 0.5
lon_overlap = 1.0
lat_overlap = 1.0
expanded_area = expand_area(area, lon=lon_overlap+lon_excess, lat=lat_overlap+lat_excess)


@timeit
def get_netcdf_dataset(goes_storage: GoesStorage, product_band: ProductBand, year: int, month: int, day: int, hour: int):
    band_blobs = goes_storage.one_hour_blobs(year, month, day, hour, product_band)
    blob = band_blobs.blobs[0]
    dataset = goes_storage.get_dataset(blob)
    return dataset


@timeit
def get_area(dataset):
    return find_dataset_area(dataset, expanded_area)


@timeit
def fill_bands_info(goes_storage: GoesStorage, area_dict: dict, bands: List[ProductBand],
                    year: int, month: int, day: int, hour: int):
    for band in bands:
        dataset = get_netcdf_dataset(goes_storage, band, year, month, day, hour)
        sat_band_key = get_dataset_key(dataset)
        key = band_key_as_string(sat_band_key)
        if key not in area_dict:
            area = get_area(dataset)
            area_dict[band_key_as_string(area.sat_band_key)] = area


@timeit
def generate_areas(goes_storage, storage, filepath):
    area_dict = {}
    fill_bands_info(goes_storage,
        area_dict,
        [
            ProductBand(Product.CMIPF, 2),
            ProductBand(Product.CMIPF, 13),
            ProductBand(Product.CMIPF, Band.BLUE),
        ], 2017, 8, 1, 12)
    fill_bands_info(goes_storage,
        area_dict,
        [
            ProductBand(Product.CMIPF, 2),
            ProductBand(Product.CMIPF, 13),
            ProductBand(Product.CMIPF, Band.BLUE),
        ], 2019, 6, 1, 12)
    print(area_dict)
    save_areas(area_dict, storage, filepath)


@timeit
def generate_tiles(storage, filepath):
    tiles = get_tiles(area, lat_step=5, lon_step=5, lat_overlap=lat_overlap, lon_overlap=lon_overlap)
    save_tiles(tiles, storage, filepath)


if __name__ == '__main__':
    import json
    from cima.goes.storage import NFS, GCS, GoesStorage

    data = NFS().download_stream('../../../../../_test/credentials.json')
    credentials_as_dict = json.loads(data)
    goes_storage = GCS(credentials_as_dict=credentials_as_dict)

    generate_areas(goes_storage, NFS(), './areas.json')
    generate_tiles(NFS(), './tiles.json')
