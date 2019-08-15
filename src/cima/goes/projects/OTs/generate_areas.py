from cima.goes import ProductBand, Product, Band
from cima.goes.tiles import LatLonRegion, expand_region
from cima.goes.tiles import get_tiles
from cima.goes.tiles import RegionData, generate_region_data
from cima.goes.utils import timeit
from cima.goes.tiles import save_tiles, save_region_data

area = LatLonRegion(
        lat_south=-45,
        lat_north=-20,
        lon_west=-75,
        lon_east=-45,
    )

lon_excess = 0.5
lat_excess = 0.5
lon_overlap = 1.0
lat_overlap = 1.0
expanded_area = expand_region(area, lon=lon_overlap + lon_excess, lat=lat_overlap + lat_excess)


@timeit
def generate_areas_data(goes_storage, storage, filepath):
    area_dict: RegionData = generate_region_data(
        goes_storage, [
            ProductBand(Product.CMIPF, 2),
            ProductBand(Product.CMIPF, 13),
            ProductBand(Product.CMIPF, Band.BLUE),
        ])
    print(area_dict)
    save_region_data(area_dict, storage, filepath)


@timeit
def generate_tiles(storage, filepath):
    tiles = get_tiles(area, lat_step=5, lon_step=5, lat_overlap=lat_overlap, lon_overlap=lon_overlap)
    save_tiles(tiles, storage, filepath)
