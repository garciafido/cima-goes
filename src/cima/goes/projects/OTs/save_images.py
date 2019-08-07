from cima.goes.tiles import load_tiles, load_areas
from cima.goes.storage import NFS

areas = load_areas(NFS(), './areas.json')
tiles = load_tiles(NFS(), './tiles.json')
print(areas)
print(tiles)