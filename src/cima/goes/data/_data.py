import cv2
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


def gamma_correction(image):
    # Apply range limits for each channel. RGB values must be between 0 and 1
    image = cp.clip(image, 0, 1)
    # Apply a gamma correction to the image to correct ABI detector brightness
    gamma = 1.2 # 2.2
    return cp.power(image, 1 / gamma)


def compose_RGB(dataset_R, dataset_G, dataset_B, tile_R: Tile, tile_G: Tile, tile_B: Tile):
    R_size = (tile_R.x_max - tile_R.x_min, tile_R.y_max - tile_R.y_min)
    R = dataset_R.variables['CMI'][tile_R.y_min : tile_R.y_max, tile_R.x_min : tile_R.x_max]
    G = dataset_G.variables['CMI'][tile_G.y_min : tile_G.y_max, tile_G.x_min : tile_G.x_max]
    B = dataset_B.variables['CMI'][tile_B.y_min : tile_B.y_max, tile_B.x_min : tile_B.x_max]

    R = gamma_correction(R)
    G = gamma_correction(G)
    B = gamma_correction(B)

    # Calculate the "True" Green
    G_resized = _resize(G, R_size)
    B_resized = _resize(B, R_size)
    G_true = 0.48358168 * R + 0.45706946 * B_resized + 0.06038137 * G_resized
    G_true = cp.clip(G_true, 0, 1)

    RGB = cp.dstack([R, G_true, B_resized])

    return RGB, R, G_true, B


def get_data(dataset, tile: Tile):
    return dataset.variables['CMI'][tile.y_min : tile.y_max, tile.x_min : tile.x_max]


def _resize(image, new_size):
  return cv2.resize(image, dsize=new_size, interpolation=cv2.INTER_CUBIC)


