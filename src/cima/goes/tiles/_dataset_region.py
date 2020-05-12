import json
from dataclasses import dataclass, asdict
from typing import Dict, Tuple, List
import numpy as np
import pyproj
from netCDF4 import Dataset
from cima.goes.storage._file_systems import Storage
from cima.goes.storage._goes_data import GoesStorage
from cima.goes.utils._file_names import ProductBand

default_major_order = FORTRAN_ORDER = 'F'


@dataclass
class LatLonRegion:
    lat_north: float
    lat_south: float
    lon_west: float
    lon_east: float


@dataclass
class RegionIndexes:
    x_min: int = None
    x_max: int = None
    y_min: int = None
    y_max: int = None


@dataclass
class SatBandKey:
    sat_height: float
    sat_lon: float
    sat_sweep: float
    x_size: int
    y_size: int


@dataclass
class DatasetRegion:
    sat_band_key: SatBandKey
    region: LatLonRegion
    indexes: RegionIndexes
    product_bands: List[Dict[str, str]]
    # shape: Tuple[int, int]


TilesDict = Dict[Tuple[int, int], LatLonRegion]
RegionData = Dict[str, DatasetRegion]


def get_dataset_region(dataset: Dataset, region_data: RegionData) -> DatasetRegion:
    sat_band_key = get_dataset_key(dataset)
    return region_data[band_key_as_string(sat_band_key)]


def get_tile_extent(region: LatLonRegion, trim_excess=0) -> tuple:
    # (left, right, bottom, top)
    return (
        region.lon_west + trim_excess,
        region.lon_east - trim_excess,
        region.lat_south + trim_excess,
        region.lat_north - trim_excess
    )


def generate_region_data(goes_storage, lat_lon_region: LatLonRegion, bands: List[ProductBand]) -> RegionData:
    region_indexes_dict: RegionData = {}
    fill_bands_info(goes_storage, lat_lon_region, region_indexes_dict, bands, 2017, 8, 1, 12)
    fill_bands_info(goes_storage, lat_lon_region, region_indexes_dict, bands, 2019, 6, 1, 12)
    return region_indexes_dict


def get_one_dataset(goes_storage: GoesStorage, product_band: ProductBand, year: int, month: int, day: int, hour: int):
    band_blobs = goes_storage.one_hour_blobs(year, month, day, hour, product_band)
    blob = band_blobs.blobs[0]
    dataset = goes_storage.get_dataset(blob)
    return dataset


def fill_bands_info(goes_storage: GoesStorage, lat_lon_region: LatLonRegion, region_indexes_dict: dict, bands: List[ProductBand],
                    year: int, month: int, day: int, hour: int):
    for band in bands:
        dataset = get_one_dataset(goes_storage, band, year, month, day, hour)
        sat_band_key = get_dataset_key(dataset)
        key = band_key_as_string(sat_band_key)
        product_band = {'product': band.product.__doc__, 'band': band.band.__doc__}
        if key not in region_indexes_dict:
            dataset_region = find_dataset_region(dataset, lat_lon_region)
            dataset_region.product_bands = [product_band]
            region_indexes_dict[band_key_as_string(dataset_region.sat_band_key)] = dataset_region
        else:
            region_indexes_dict[key].product_bands.append(product_band)


def load_tiles(storage: Storage, filepath) -> TilesDict:
    data = storage.download_data(filepath)
    tiles_dict = json.loads(data)
    return dict_to_tiles(tiles_dict)


def save_tiles(tiles: TilesDict, storage: Storage, filepath):
    tiles_dict = tiles_to_dict(tiles)
    storage.upload_data(bytes(json.dumps(tiles_dict, indent=2), 'utf8'), filepath)


def save_region_data(region_data: RegionData, storage: Storage, filepath):
    region_dict = region_data_as_dict(region_data)
    storage.upload_data(bytes(json.dumps(region_dict, indent=2), 'utf8'), filepath)


def load_region_data(storage: Storage, filepath) -> RegionData:
    data = storage.download_data(filepath)
    region_dict = json.loads(data)
    return region_data_from_dict(region_dict)


def dataset_region_as_dict(dataset_region: DatasetRegion) -> dict:
    return {
        'sat_band_key': asdict(dataset_region.sat_band_key),
        'region': asdict(dataset_region.region),
        'indexes': asdict(dataset_region.indexes)
    }


def region_data_as_dict(region_data: RegionData) -> dict:
    region_dict = {}
    for k, v in region_data.items():
        region_dict[k] = dataset_region_as_dict(v)
    return region_dict


def region_data_from_dict(bands_region_dict: dict) -> RegionData:
    bands_region = {}
    for k, v in bands_region_dict.items():
        bands_region[k] = DatasetRegion(
            sat_band_key=SatBandKey(**v['sat_band_key']),
            region=LatLonRegion(**v['region']),
            indexes=RegionIndexes(**v['indexes']),
            product_bands=None
        )
    return bands_region


def tiles_to_dict(tiles: TilesDict) -> dict:
    tiles_dict = {}
    for k, v in tiles.items():
        tiles_dict[str(k)] = asdict(v)
    return tiles_dict


def dict_to_tiles(tiles: dict) -> TilesDict:
    from ast import literal_eval
    tiles_dict = {}
    for k, v in tiles.items():
        tiles_dict[literal_eval(k)] = LatLonRegion(**v)
    return tiles_dict


def band_key_as_string(band_key: SatBandKey) -> str:
    return f'{band_key.sat_height}#{band_key.sat_lon}#{band_key.sat_sweep}#{band_key.x_size}#{band_key.y_size}'


def dataset_key_as_string(dataset) -> str:
    band_key = get_dataset_key(dataset)
    return band_key_as_string(band_key)


def expand_region(region: LatLonRegion, lat, lon):
    return LatLonRegion(
        lat_south=region.lat_south - lat,
        lat_north=region.lat_north + lat,
        lon_west=region.lon_west - lon,
        lon_east=region.lon_east + lon,
    )


def contract_region(region: LatLonRegion, lat, lon):
    return LatLonRegion(
        lat_south=region.lat_south + lat,
        lat_north=region.lat_north - lat,
        lon_west=region.lon_west + lon,
        lon_east=region.lon_east - lon,
    )


def get_lats_lons(dataset, indexes: RegionIndexes = None):
    dataset_key = get_dataset_key(dataset)
    if indexes is None:
        x = dataset['x'][:] * dataset_key.sat_height
        y = dataset['y'][:] * dataset_key.sat_height
    else:
        x = dataset['x'][indexes.x_min: indexes.x_max] * dataset_key.sat_height
        y = dataset['y'][indexes.y_min: indexes.y_max] * dataset_key.sat_height
    XX, YY = np.meshgrid(np.array(x), np.array(y))
    projection = pyproj.Proj(proj='geos', h=dataset_key.sat_height, lon_0=dataset_key.sat_lon,
                             sweep=dataset_key.sat_sweep)
    lons, lats = projection(XX, YY, inverse=True)
    return np.array(lats), np.array(lons)


def get_data(dataset, indexes: RegionIndexes = None, variable: str = None):
    if variable is None:
        if 'CMI' in dataset.variables:
            variable = 'CMI'
        elif 'Rad' in dataset.variables:
            variable = 'Rad'
    if indexes is None:
        data = dataset.variables[variable][:,:]
    else:
        data = dataset.variables[variable][indexes.y_min : indexes.y_max, indexes.x_min : indexes.x_max]
    data.units = dataset.variables[variable].units
    data.long_name = dataset.variables[variable].long_name
    data.name = variable
    return data


def save_netcdf(filename: str, dataset, indexes: RegionIndexes = None, variable: str = None):
    if variable is None:
        if 'CMI' in dataset.variables:
            variable = 'CMI'
        elif 'Rad' in dataset.variables:
            variable = 'Rad'

    data = get_data(dataset, indexes, variable)

    lats, lons = get_lats_lons(dataset, indexes)

    clipped_dataset = Dataset(filename, 'w', format='NETCDF4')
    clipped_dataset.createDimension('x', data.shape[0])
    clipped_dataset.createDimension('y', data.shape[1])

    # create latitude axis
    new_lats = clipped_dataset.createVariable('lats', lats.dtype, ('x', 'y'))
    new_lats.standard_name = 'latitude'
    new_lats.long_name = 'latitude'
    new_lats.units = 'degrees_north'
    new_lats.axis = 'Y'
    new_lats[:,:] = lats[:,:]

    # create longitude axis
    new_lons = clipped_dataset.createVariable('lons', lons.dtype, ('x', 'y'))
    new_lons.standard_name = 'longitude'
    new_lons.long_name = 'longitude'
    new_lons.units = 'degrees_east'
    new_lons.axis = 'X'
    new_lons[:,:] = lons[:,:]

    # create variable array
    new_data = clipped_dataset.createVariable(data.name, data.dtype, ('x', 'y'))
    new_data.long_name = data.long_name
    new_data.units = data.units
    new_data[:,:] = data[:,:]

    clipped_dataset.close()


def get_dataset_key(dataset: Dataset) -> SatBandKey:
    imager_projection = dataset['goes_imager_projection']
    sat_height = imager_projection.perspective_point_height
    sat_lon = imager_projection.longitude_of_projection_origin
    sat_sweep = imager_projection.sweep_angle_axis
    return SatBandKey(
        sat_height=sat_height,
        sat_lon=sat_lon,
        sat_sweep=sat_sweep,
        x_size=dataset.dimensions['x'].size,
        y_size=dataset.dimensions['y'].size
    )


def find_dataset_region(dataset, region: LatLonRegion, major_order=default_major_order) -> DatasetRegion:
    sat_band_key = get_dataset_key(dataset)
    lats, lons = get_lats_lons(dataset)
    indexes = find_indexes(region, lats, lons, major_order)
    return DatasetRegion(
        sat_band_key=sat_band_key,
        region=region,
        indexes=indexes,
        product_bands=None
    )


def nearest_indexes(lat, lon, lats, lons, major_order):
    distance = (lat - lats) * (lat - lats) + (lon - lons) * (lon - lons)
    return np.unravel_index(np.argmin(distance), lats.shape, major_order)


def find_indexes(region: LatLonRegion, lats, lons, major_order) -> RegionIndexes:
    x1, y1 = nearest_indexes(region.lat_north, region.lon_west, lats, lons, major_order)
    x2, y2 = nearest_indexes(region.lat_north, region.lon_east, lats, lons, major_order)
    x3, y3 = nearest_indexes(region.lat_south, region.lon_west, lats, lons, major_order)
    x4, y4 = nearest_indexes(region.lat_south, region.lon_east, lats, lons, major_order)

    indexes = RegionIndexes()
    indexes.x_min = int(min(x1, x2, x3, x4))
    indexes.x_max = int(max(x1, x2, x3, x4))
    indexes.y_min = int(min(y1, y2, y3, y4))
    indexes.y_max = int(max(y1, y2, y3, y4))
    return indexes


def get_tiles(region: LatLonRegion,
              lat_step: float,
              lon_step: float,
              lat_overlap: float,
              lon_overlap: float) -> TilesDict:
    tiles = {}
    lat_step = -lat_step
    lats = [x for x in np.arange(region.lat_north, region.lat_south, lat_step)]
    lons = [x for x in np.arange(region.lon_west, region.lon_east, lon_step)]
    for lat_index, lat in enumerate(lats):
        for lon_index, lon in enumerate(lons):
            tiles[(lat_index, lon_index)] = expand_region(
            LatLonRegion(
                lat_north=float(lat),
                lat_south=float(lat + lat_step),
                lon_west=float(lon),
                lon_east=float(lon + lon_step)),
            lat_overlap,
            lon_overlap)
    return tiles
