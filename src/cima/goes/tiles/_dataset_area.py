import json
from dataclasses import dataclass, asdict
from typing import Dict, Tuple

import numpy as np
import pyproj
from cima.goes.storage._file_systems import Storage

default_major_order = FORTRAN_ORDER = 'F'


@dataclass
class LatLonArea:
    lat_south: float
    lat_north: float
    lon_west: float
    lon_east: float


@dataclass
class AreaIndexes:
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
class DatasetArea:
    sat_band_key: SatBandKey
    lan_lot_area: LatLonArea
    indexes: AreaIndexes


TilesDict = Dict[Tuple[int, int], LatLonArea]
AreasDict = Dict[str, DatasetArea]


def load_tiles(storage: Storage, filepath) -> TilesDict:
    data = storage.download_data(filepath)
    tiles_dict = json.loads(data)
    return dict_to_tiles(tiles_dict)


def save_tiles(tiles: TilesDict, storage: Storage, filepath):
    tiles_dict = tiles_to_dict(tiles)
    storage.upload_data(bytes(json.dumps(tiles_dict, indent=2), 'utf8'), filepath)


def save_areas(areas: AreasDict, storage: Storage, filepath):
    areas_dict = areas_as_dict(areas)
    storage.upload_data(bytes(json.dumps(areas_dict, indent=2), 'utf8'), filepath)


def load_areas(storage: Storage, filepath) -> AreasDict:
    data = storage.download_data(filepath)
    areas_dict = json.loads(data)
    return areas_from_dict(areas_dict)


def dataset_area_as_dict(dataset_area: DatasetArea) -> dict:
    return {
        'sat_band_key': asdict(dataset_area.sat_band_key),
        'lan_lot_area': asdict(dataset_area.lan_lot_area),
        'indexes': asdict(dataset_area.indexes)
    }


def areas_as_dict(areas: AreasDict) -> dict:
    areas_dict = {}
    for k, v in areas.items():
        areas_dict[k] = dataset_area_as_dict(v)
    return areas_dict


def areas_from_dict(bands_areas_dict: dict) -> AreasDict:
    bands_areas = {}
    for k, v in bands_areas_dict.items():
        bands_areas[k] = DatasetArea(
            sat_band_key=SatBandKey(**v['sat_band_key']),
            lan_lot_area=LatLonArea(**v['lan_lot_area']),
            indexes=AreaIndexes(**v['indexes']),
        )
    return bands_areas


def tiles_to_dict(tiles: TilesDict) -> dict:
    tiles_dict = {}
    for k, v in tiles.items():
        tiles_dict[str(k)] = asdict(v)
    return tiles_dict


def dict_to_tiles(tiles: dict) -> TilesDict:
    from ast import literal_eval
    tiles_dict = {}
    for k, v in tiles.items():
        tiles_dict[literal_eval(k)] = LatLonArea(**v)
    return tiles_dict


def band_key_as_string(band_key: SatBandKey) -> str:
    return f'{band_key.sat_height}#{band_key.sat_lon}#{band_key.sat_sweep}#{band_key.x_size}#{band_key.y_size}'


def dataset_key_as_string(dataset) -> str:
    band_key = get_dataset_key(dataset)
    return band_key_as_string(band_key)


def expand_area(area: LatLonArea, lat, lon):
    return LatLonArea(
        lat_south=area.lat_south - lat,
        lat_north=area.lat_north + lat,
        lon_west=area.lon_west - lon,
        lon_east=area.lon_east + lon,
    )


def get_lats_lons(dataset, indexes: AreaIndexes = None):
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


def get_dataset_key(dataset) -> SatBandKey:
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


def find_dataset_area(dataset, area: LatLonArea, major_order=default_major_order) -> DatasetArea:
    sat_band_key = get_dataset_key(dataset)
    lats, lons = get_lats_lons(dataset)
    indexes = find_indexes(area, lats, lons, major_order)
    return DatasetArea(
        sat_band_key=sat_band_key,
        lan_lot_area=area,
        indexes=indexes
    )


def nearest_indexes(lat, lon, lats, lons, major_order):
    distance = (lat - lats) * (lat - lats) + (lon - lons) * (lon - lons)
    return np.unravel_index(np.argmin(distance), lats.shape, major_order)


def find_indexes(area: LatLonArea, lats, lons, major_order) -> AreaIndexes:
    x1, y1 = nearest_indexes(area.lat_north, area.lon_west, lats, lons, major_order)
    x2, y2 = nearest_indexes(area.lat_north, area.lon_east, lats, lons, major_order)
    x3, y3 = nearest_indexes(area.lat_south, area.lon_west, lats, lons, major_order)
    x4, y4 = nearest_indexes(area.lat_south, area.lon_east, lats, lons, major_order)

    indexes = AreaIndexes()
    indexes.x_min = int(min(x1, x2, x3, x4))
    indexes.x_max = int(max(x1, x2, x3, x4))
    indexes.y_min = int(min(y1, y2, y3, y4))
    indexes.y_max = int(max(y1, y2, y3, y4))
    return indexes


def get_tiles(area: LatLonArea,
              lat_step: float,
              lon_step: float,
              lat_overlap: float,
              lon_overlap: float) -> TilesDict:
    tiles = {}
    lats = [x for x in np.arange(area.lat_south, area.lat_north, lat_step)]
    lons = [x for x in np.arange(area.lon_west, area.lon_east, lon_step)]
    for lon_index, lon in enumerate(lons):
        for lat_index, lat in enumerate(lats):
            tiles[(lat_index, lon_index)] = expand_area(
                LatLonArea(
                    lat_north=float(lat),
                    lat_south=float(lat + lat_step),
                    lon_west=float(lon),
                    lon_east=float(lon + lon_step)),
                lat_overlap,
                lon_overlap)
    return tiles


a = {'35786023.0#-89.5#x#21696#21696': DatasetArea(
    sat_band_key=SatBandKey(sat_height=35786023.0, sat_lon=-89.5, sat_sweep='x', x_size=21696, y_size=21696),
    lan_lot_area=LatLonArea(lat_south=-46.5, lat_north=-18.5, lon_west=-76.5, lon_east=-43.5),
    indexes=AreaIndexes(x_min=12699, x_max=18990, y_min=14628, y_max=19486)),
     '35786023.0#-89.5#x#5424#5424': DatasetArea(
         sat_band_key=SatBandKey(sat_height=35786023.0, sat_lon=-89.5, sat_sweep='x', x_size=5424, y_size=5424),
         lan_lot_area=LatLonArea(lat_south=-46.5, lat_north=-18.5, lon_west=-76.5, lon_east=-43.5),
         indexes=AreaIndexes(x_min=3174, x_max=4747, y_min=3657, y_max=4871)),
     '35786023.0#-89.5#x#10848#10848': DatasetArea(
         sat_band_key=SatBandKey(sat_height=35786023.0, sat_lon=-89.5, sat_sweep='x', x_size=10848, y_size=10848),
         lan_lot_area=LatLonArea(lat_south=-46.5, lat_north=-18.5, lon_west=-76.5, lon_east=-43.5),
         indexes=AreaIndexes(x_min=6349, x_max=9495, y_min=7314, y_max=9743)),
     '35786023.0#-75.0#x#21696#21696': DatasetArea(
         sat_band_key=SatBandKey(sat_height=35786023.0, sat_lon=-75.0, sat_sweep='x', x_size=21696, y_size=21696),
         lan_lot_area=LatLonArea(lat_south=-46.5, lat_north=-18.5, lon_west=-76.5, lon_east=-43.5),
         indexes=AreaIndexes(x_min=10535, x_max=16926, y_min=14725, y_max=19512)),
     '35786023.0#-75.0#x#5424#5424': DatasetArea(
         sat_band_key=SatBandKey(sat_height=35786023.0, sat_lon=-75.0, sat_sweep='x', x_size=5424, y_size=5424),
         lan_lot_area=LatLonArea(lat_south=-46.5, lat_north=-18.5, lon_west=-76.5, lon_east=-43.5),
         indexes=AreaIndexes(x_min=2633, x_max=4231, y_min=3681, y_max=4878)),
     '35786023.0#-75.0#x#10848#10848': DatasetArea(
         sat_band_key=SatBandKey(sat_height=35786023.0, sat_lon=-75.0, sat_sweep='x', x_size=10848, y_size=10848),
         lan_lot_area=LatLonArea(lat_south=-46.5, lat_north=-18.5, lon_west=-76.5, lon_east=-43.5),
         indexes=AreaIndexes(x_min=5267, x_max=8463, y_min=7362, y_max=9756))}


from cima.goes.storage import NFS, FTP
ad = areas_as_dict(a)
adb = bytes(json.dumps(ad, indent=2), 'utf8')
# NFS().upload_stream(adb, './areas.json')

FTP_HOST = 'mate.cima.fcen.uba.ar'
FTP_USER = 'ftp_alertar'
FTP_PASSWORD = 'Dra6h&b3wUDr'
ftp = FTP(
    host=FTP_HOST,
    user=FTP_USER,
    password=FTP_PASSWORD)
ftp.upload_data(adb, '/OTs/areas/areas.json')
