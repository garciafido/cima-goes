from cima.goes.tiles._tiles import Tile
try:
    import numpy as np
    # import cupy as cp
    cp = np
    asnumpy = cp.asnumpy
except:
    import numpy as np
    cp = np
    asnumpy = lambda x: x


def get_data(dataset, tile: Tile, variable: str = 'CMI'):
    return dataset.variables[variable][tile.y_min : tile.y_max, tile.x_min : tile.x_max]


