from cima.goes import Band
from cima.goes.tiles import generate_tiles, save_tiles, load_tiles
from cima.goes.utils import timeit

@timeit
def generate_ir_vis_tiles(
        bands=[
           Band.CLEAN_LONGWAVE_WINDOW,
           Band.RED,
           Band.VEGGIE,
           Band.BLUE,
        ],
        lat_south=-45,
        lat_north=-20,
        lon_west=-75,
        lon_east=-45,
        lat_step=5,
        lon_step=5,
        lon_overlap=1.5,
        lat_overlap=1.5,
        save_filepath=None,
        credentials_as_dict=None):

    tiles = generate_tiles(bands,
        lat_south=lat_south,
        lat_north=lat_north,
        lon_west=lon_west,
        lon_east=lon_east,
        lat_step=lat_step,
        lon_step=lon_step,
        lon_overlap=lon_overlap,
        lat_overlap=lat_overlap,
        credentials_as_dict=credentials_as_dict,
    )
    if save_filepath is not None:
        save_tiles(save_filepath, tiles)
    return tiles

