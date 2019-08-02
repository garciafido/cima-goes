import io
import os
import json
import pyproj
from typing import Dict, List, Tuple
from dataclasses import dataclass, asdict
from cima.goes import Band, Product, ProductBand
from cima.goes import GoesData, GroupedBandBlobs
from cima.goes.storage._blobs import BandBlobs
from cima.goes.tasks import Task, run_concurrent
from cima.goes.storage._file_systems import goesdata_info, storage_info
from cima.goes.storage._factories import mount_goesdata, mount_store

try:
    import numpy as np
    # import cupy as cp
    cp = np
    asnumpy = cp.asnumpy
except:
    import numpy as np
    cp = np
    asnumpy = lambda x: x

FORTRAN_ORDER = 'F'


@dataclass
class Tile:
    lat_south: float
    lat_north: float
    lon_west: float
    lon_east: float

    x_min: int = None
    x_max: int = None
    y_min: int = None
    y_max: int = None


def get_tile_extent(tile, trim_excess=0) -> tuple:
    # (left, right, bottom, top)
    return (
        tile.lon_west + trim_excess,
        tile.lon_east - trim_excess,
        tile.lat_south + trim_excess,
        tile.lat_north - trim_excess
    )


@dataclass
class BandTiles:
    product: Product
    band: Band
    tiles: Dict[str, Tile]


BandTilesDict = Dict[Tuple[Product, Band], Dict[str, Tile]]


def generate_tiles(goes_info: goesdata_info,
                   bands: List[ProductBand],
                   lat_south: float,
                   lat_north: float,
                   lon_west: float,
                   lon_east: float,
                   lat_step: float,
                   lon_step: float,
                   lon_overlap: float,
                   lat_overlap: float,
                   workers=2,
                   ) -> BandTilesDict:
    tasks = []
    for band in bands:
        tasks.append(
            Task(_get_indexed_tiles,
                 goes_info,
                 band,
                 lat_south=lat_south, lat_north=lat_north,
                 lon_west=lon_west, lon_east=lon_east,
                 lat_step=lat_step, lon_step=lon_step,
                 lat_overlap=lat_overlap, lon_overlap=lon_overlap
            ))
    workers = min(workers, len(tasks))
    responses: List[BandTiles] = run_concurrent(tasks, workers=workers)
    errors = []
    for resp in responses:
        if not isinstance(resp, BandTiles):
            errors.append(resp)
    if errors:
        raise Exception(str(errors))

    band_tiles_dict = {(band_tiles.product, band_tiles.band): band_tiles.tiles for band_tiles in responses}
    return band_tiles_dict


def _get_indexed_tiles(goes_info: goesdata_info, band: ProductBand, **kwargs) -> BandTiles:
    goesdata: GoesData = mount_goesdata(goes_info)
    band_blobs: BandBlobs = goesdata.get_blobs(2018, 360, 12, band)
    dataset = goesdata.get_dataset(band_blobs.blobs[0])
    try:
        tiles = _get_tiles_one_band(**kwargs)
        tiles = _add_indexes_for_dataset(dataset, tiles)
        return BandTiles(band.product, band.band, tiles)
    finally:
        dataset.close()


def save_tiles(store_info: storage_info, filepath: str, band_tiles_dict: BandTilesDict):
    tiles_dict = {}
    for product_band, tiles in band_tiles_dict.items():
        tiles: Dict[str, Tile]
        band_dict = {}
        for k, tile in tiles.items():
            band_dict[k] = asdict(tile)
        tiles_dict[f'{product_band[0].name}/{product_band[1].name}'] = band_dict
    data = bytes(json.dumps(tiles_dict, indent=2), 'utf-8')
    in_memory_file = io.BytesIO()
    in_memory_file.write(data)
    in_memory_file.seek(0)
    store = mount_store(store_info)
    store.upload_stream(in_memory_file, filepath)


def load_tiles(store_info: storage_info, filepath) -> BandTilesDict:
    store = mount_store(store_info)
    data = store.download_stream(filepath)
    tiles_dict = json.loads(data)
    return get_tiles_from_dict(tiles_dict)


def get_tiles_from_dict(tiles_dict) -> BandTilesDict:
    bands_tiles: BandTilesDict = {}
    for product_band, tiles in tiles_dict.items():
        product, band = product_band.split('/')
        if tiles is not None:
            tiles_dict = {}
            bands_tiles[(Product[product], Band[band])] = tiles_dict
            for tile_number, tile_data in tiles.items():
                tiles_dict[tile_number] = Tile(**tile_data)
    return bands_tiles


def get_lats_lons(dataset, tile: Tile = None):
    sat_height = dataset['goes_imager_projection'].perspective_point_height
    sat_lon = dataset['goes_imager_projection'].longitude_of_projection_origin
    sat_sweep = dataset['goes_imager_projection'].sweep_angle_axis
    projection = pyproj.Proj(proj='geos', h=sat_height, lon_0=sat_lon, sweep=sat_sweep)
    if tile is None:
        x = dataset['x'][:] * sat_height
        y = dataset['y'][:] * sat_height
    else:
        x = dataset['x'][tile.x_min: tile.x_max] * sat_height
        y = dataset['y'][tile.y_min: tile.y_max] * sat_height
    XX, YY = cp.meshgrid(cp.array(x), cp.array(y))
    lons, lats = projection(asnumpy(XX), asnumpy(YY), inverse=True)
    return cp.array(lats), cp.array(lons)


def _get_tiles_one_band(lat_south: float, lat_north: float,
                        lon_west: float, lon_east: float,
                        lat_step: float, lon_step: float,
                        lat_overlap: float, lon_overlap: float):
    """
    >>> _get_tiles_one_band(lat_south=-45, lat_north=-40, lon_west=-75, lon_east=-70, lat_step=5, lon_step=5, lon_overlap=1, lat_overlap=1)
    {'0': Tile(lat_south=-46.0, lat_north=-39.0, lon_west=-76.0, lon_east=-69.0, x_min=None, x_max=None, y_min=None, y_max=None)}
    >>> _get_tiles_one_band(lat_south=-43, lat_north=-33, lon_west=-75, lon_east=-70, lat_step=10, lon_step=2.5, lon_overlap=1, lat_overlap=1)
    {'0': Tile(lat_south=-44.0, lat_north=-32.0, lon_west=-76.0, lon_east=-71.5, x_min=None, x_max=None, y_min=None, y_max=None), '1': Tile(lat_south=-44.0, lat_north=-32.0, lon_west=-73.5, lon_east=-69.0, x_min=None, x_max=None, y_min=None, y_max=None)}
    """
    tiles = {}
    lats = [x for x in cp.arange(lat_south, lat_north, lat_step)]
    lons = [x for x in cp.arange(lon_west, lon_east, lon_step)]
    tiles_coordinates = [(lat, lat + lat_step, lon, lon + lon_step, ) for lat in lats for lon in lons]
    for index, lats_lons in enumerate(tiles_coordinates):
        tiles[str(index)] = Tile(
            lat_south = float(lats_lons[0] - lat_overlap),
            lat_north = float(lats_lons[1] + lat_overlap),
            lon_west = float(lats_lons[2] - lon_overlap),
            lon_east = float(lats_lons[3] + lon_overlap))
    return tiles


def _add_indexes_for_dataset(dataset, tiles):
    major_order = FORTRAN_ORDER
    lats, lons = get_lats_lons(dataset)
    _add_indexes(tiles, lats, lons, major_order)
    return tiles


def _nearest_indexes(lat, lon, lats, lons, major_order):
    distance = (lat - lats) * (lat - lats) + (lon - lons) * (lon - lons)
    return cp.unravel_index(cp.argmin(distance), lats.shape, major_order)


def _find_indexes(tile: Tile, lats, lons, major_order):
    x1, y1 = _nearest_indexes(tile.lat_north, tile.lon_west, lats, lons, major_order)
    x2, y2 = _nearest_indexes(tile.lat_north, tile.lon_east, lats, lons, major_order)
    x3, y3 = _nearest_indexes(tile.lat_south, tile.lon_west, lats, lons, major_order)
    x4, y4 = _nearest_indexes(tile.lat_south, tile.lon_east, lats, lons, major_order)

    tile.x_min = int(min(x1, x2, x3, x4))
    tile.x_max = int(max(x1, x2, x3, x4))
    tile.y_min = int(min(y1, y2, y3, y4))
    tile.y_max = int(max(y1, y2, y3, y4))


def _add_indexes(tiles, lats, lons, order):
    for index, tile in tiles.items():
        _find_indexes(tile, lats, lons, order)
