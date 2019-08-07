from cima.goes.projects.OTs.areas import dict_areas
from cima.goes.projects.OTs.tiles import dict_tiles
from cima.goes.tiles import bands_areas_from_dict, dict_to_tiles

areas = bands_areas_from_dict(dict_areas)
tiles = dict_to_tiles(dict_tiles)