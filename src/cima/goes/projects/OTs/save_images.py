from cima.goes import ProductBand
from cima.goes.tiles import load_tiles, load_region_data
from cima.goes.storage import NFS


def show_info():
    areas = load_region_data(NFS(), './areas.json')
    tiles = load_tiles(NFS(), './tiles.json')
    print(areas)
    print(tiles)


def save_tiles(product_band: ProductBand, year: int, month: int, day: int, hour: int):
    pass
