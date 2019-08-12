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


def get_data(dataset, tile: Tile, variable: str = None):
    if variable is None:
        if 'CMI' in dataset.variables:
            variable = 'CMI'
        elif 'Ref' in dataset.variables:
            variable = 'Ref'
    return dataset.variables[variable][tile.y_min : tile.y_max, tile.x_min : tile.x_max]


