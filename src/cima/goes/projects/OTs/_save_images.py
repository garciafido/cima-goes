import os
from cima.goes.gcs import get_datasets
from cima.goes import Band
from cima.goes.gcs import close_datasets
from cima.goes.tiles import get_lats_lons, BandsTiles, get_tile_extent
from cima.goes.data import get_data, gamma_correction
from cima.goes.img import save_image, get_cloud_tops_palette
from cima.goes.utils import get_usage


def save_vis_ir_pairs(images_path: str, year: int, day_of_year: int, hour: int, tiles: BandsTiles,
                      credentials_as_dict=None):
    datasets = get_datasets(year, day_of_year, hour, [Band.RED, Band.CLEAN_LONGWAVE_WINDOW], credentials_as_dict=credentials_as_dict)
    tiles_count = len(tiles.RED.keys())
    print('tiles count:', tiles_count)
    for ds in datasets:
        for tile_number in map(str, range(tiles_count)):
            filepath_prefix = os.path.join(images_path, f'{ds.start[:4]}/{ds.start[4:7]}/{ds.start}')
            print(filepath_prefix, 'tile:', tile_number)
            print(get_usage())

            tile_red = tiles.RED[tile_number]
            tile_ir = tiles.CLEAN_LONGWAVE_WINDOW[tile_number]

            lats, lons = get_lats_lons(ds.RED, tiles.RED[tile_number])
            data = get_data(ds.RED, tiles.RED[tile_number])
            gray = gamma_correction(data)
            extent = get_tile_extent(tile_red, trim_excess=0.5)
            save_image(gray,
                       f'{filepath_prefix}_{tile_number}_vis',
                       tile_red,
                       lats, lons,
                       extent=extent)

            lats, lons = get_lats_lons(ds.CLEAN_LONGWAVE_WINDOW, tiles.CLEAN_LONGWAVE_WINDOW[tile_number])
            data = get_data(ds.CLEAN_LONGWAVE_WINDOW, tiles.CLEAN_LONGWAVE_WINDOW[tile_number])
            ir = data - 273
            extent = get_tile_extent(tile_ir, trim_excess=0.5)
            save_image(ir,
                       f'{filepath_prefix}_{tile_number}_ir',
                       tile_ir,
                       lats, lons,
                       extent=extent,
                       cmap=get_cloud_tops_palette(),
                       vmin=-90, vmax=50)
        close_datasets(ds)

    return f'end of {year} {day_of_year} {hour}'
