import json
from cima.goes import ProductBand, Product, Band
from cima.goes.tiles import find_dataset_area, LatLonArea, dataset_area_as_dict, expand_area, band_key_as_string
from cima.goes.tiles import get_dataset_key
from cima.goes.tiles import get_tiles, tiles_to_dict
from cima.goes.utils import timeit


area = LatLonArea(
        lat_south=-45,
        lat_north=-20,
        lon_west=-75,
        lon_east=-45,
    )

lon_overlap = 1.5
lat_overlap = 1.5
expanded_area = expand_area(area, lon=lon_overlap, lat=lat_overlap)


@timeit
def get_netcdf_dataset(goes_storage, product_band, year, day_of_year, hour):
    band_blobs = goes_storage.one_hour_blobs(year, day_of_year, hour, product_band)
    blob = band_blobs.blobs[0]
    dataset = goes_storage.get_dataset(blob)
    return dataset


@timeit
def get_area(dataset):
    return find_dataset_area(dataset, expanded_area)


def fill_bands_info(area_dict, bands, year, day_of_year, hour):
    for band in bands:
        dataset = get_netcdf_dataset(band, year, day_of_year, hour)
        sat_band_key = get_dataset_key(dataset)
        key = band_key_as_string(sat_band_key)
        if key not in area_dict:
            area = get_area(dataset)
            area_dict[band_key_as_string(area.sat_band_key)] = dataset_area_as_dict(area)


def generate_areas():
    area_dict = {}
    fill_bands_info(
        area_dict,
        [
            ProductBand(Product.CMIPF, 2),
            ProductBand(Product.CMIPF, 13),
            ProductBand(Product.CMIPF, Band.BLUE),
        ], 2017, 192, 12)
    fill_bands_info(
        area_dict,
        [
            ProductBand(Product.CMIPF, 2),
            ProductBand(Product.CMIPF, 13),
            ProductBand(Product.CMIPF, Band.BLUE),
        ], 2019, 60, 12)

    print(json.dumps(area_dict, indent=2))


def generate_tiles():
    tiles = get_tiles(area, lat_step=5, lon_step=5, lat_overlap=lat_overlap, lon_overlap=lon_overlap)
    print(json.dumps(tiles_to_dict(tiles), indent=2))

