import io
import os
from dataclasses import dataclass

import numpy as np
import cartopy
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from cima.goes.storage._file_systems import Storage
from cima.goes.tiles import Tile, get_tile_extent
from cima.goes.utils.load_cpt import load_cpt
from matplotlib.axes import Axes


LOCAL_BASE_PATH = os.path.dirname(os.path.abspath(__file__))


def add_cultural(ax):
    states_provinces = cartopy.feature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='10m',
        facecolor='none')

    countries = cartopy.feature.NaturalEarthFeature(
        category='cultural',
        name='admin_0_countries',
        scale='10m',
        facecolor='none')

    linewidth = 0.50
    ax.coastlines(resolution='10m', color='white', linewidth=linewidth)
    ax.add_feature(countries, edgecolor='white', linewidth=linewidth)
    ax.add_feature(states_provinces, edgecolor='white', linewidth=linewidth)


def add_grid(ax):
    linewidth = 1.25
    gl = ax.gridlines(linewidth=linewidth,
                      linestyle='dotted',
                      color='r',
                      crs=ccrs.PlateCarree(),
                      draw_labels=True)


def get_cloud_tops_palette():
    from matplotlib.colors import LinearSegmentedColormap
    filepath = os.path.join(LOCAL_BASE_PATH, 'smn_topes.cpt')
    cpt = load_cpt(filepath)
    return LinearSegmentedColormap('cpt', cpt)


def pcolormesh(ax: Axes, image, lons, lats, cmap=None, vmin=None, vmax=None):
    if len(image.shape) == 3:
        mesh_rgb = image[:, :-1, :]
        colorTuple = mesh_rgb.reshape((mesh_rgb.shape[0] * mesh_rgb.shape[1]), 3)
        # ADDED THIS LINE
        colorTuple = np.insert(colorTuple, 3, 1.0, axis=1)
        # What you put in for the image doesn't matter because of the color mapping
        ax.pcolormesh(lons, lats, image[:, :, 0], color=colorTuple)
    else:
        ax.pcolormesh(lons, lats, image, cmap=cmap, vmin=vmin, vmax=vmax)


def set_extent(ax: Axes, tile: Tile, trim_excess=0):
    extent = get_tile_extent(tile, trim_excess=trim_excess)
    ax.set_extent(extent, crs=ccrs.PlateCarree())


@dataclass
class ImageResolution:
    dpi: int
    x: int
    y: int


def get_image_inches(image):
    dummy_dpi = 100
    x, y = image.shape[:2]
    return ImageResolution(dummy_dpi, x / dummy_dpi, y / dummy_dpi)


def save_image(image,
               storage: Storage,
               filepath: str,
               tile: Tile,
               lats, lons,
               cmap=None, vmin=None, vmax=None,
               draw_cultural=False, draw_grid=False,
               trim_excess=0):
    image_inches = get_image_inches(image)

    fig = plt.figure(frameon=False)
    try:
        fig.set_size_inches(image_inches.x, image_inches.y)

        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        ax.set_axis_off()
        set_extent(ax, tile, trim_excess)
        if draw_cultural:
            add_cultural(ax)
        if draw_grid:
            add_grid(ax)
        pcolormesh(ax, lons, lats, image, cmap=cmap, vmin=vmin, vmax=vmax)
        fig.add_axes(ax, projection=ccrs.PlateCarree())
        ax.axis('off')
        
        buffer = io.BytesIO()
        _, file_extension = os.path.splitext(filepath)
        format = 'png'
        if file_extension[0] == '.':
            format = file_extension[1:]
        plt.savefig(buffer, format=format, dpi=image_inches.dpi, bbox_inches='tight', pad_inches=0)
        buffer.seek(0)
        storage.upload_stream(buffer, filepath)
        buffer.seek(0)
        return buffer
    finally:
        fig.clear()
        plt.close()
