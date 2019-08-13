import io
import os
import cv2
from dataclasses import dataclass
import numpy as np
import cartopy
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from cima.goes.storage._file_systems import Storage
from cima.goes.tiles import DatasetRegion, LatLonRegion, get_tile_extent
from cima.goes.utils.load_cpt import load_cpt
from matplotlib.axes import Axes
# from PIL import Image


LOCAL_BASE_PATH = os.path.dirname(os.path.abspath(__file__))


# def fig2data(fig):
#     """
#     @brief Convert a Matplotlib figure to a 4D numpy array with RGBA channels and return it
#     @param fig a matplotlib figure
#     @return a numpy 3D array of RGBA values
#     """
#     # draw the renderer
#     fig.canvas.draw()
#
#     # Get the RGBA buffer from the figure
#     w, h = fig.canvas.get_width_height()
#     buf = np.frombuffer(fig.canvas.tostring_argb(), dtype=np.uint8)
#     buf.shape = (w, h, 4)
#
#     # canvas.tostring_argb give pixmap in ARGB mode. Roll the ALPHA channel to have it in RGBA mode
#     buf = np.roll(buf, 3, axis=2)
#     return buf
#
#
# def fig2data(fig):
#     """
#     @brief Convert a Matplotlib figure to a 4D numpy array with RGBA channels and return it
#     @param fig a matplotlib figure
#     @return a numpy 3D array of RGBA values
#     """
#     # draw the renderer
#     fig.canvas.draw()
#
#     # Get the RGBA buffer from the figure
#     w, h = fig.canvas.get_width_height()
#     buf = np.frombuffer(fig.canvas.tostring_argb(), dtype=np.uint8)
#     buf.shape = (w, h, 4)
#
#     # canvas.tostring_argb give pixmap in ARGB mode. Roll the ALPHA channel to have it in RGBA mode
#     buf = np.roll(buf, 3, axis=2)
#     return buf
#
#
# def fig2img(fig):
#     """
#     @brief Convert a Matplotlib figure to a PIL Image in RGBA format and return it
#     @param fig a matplotlib figure
#     @return a Python Imaging Library ( PIL ) image
#     """
#     # put the figure pixmap into a numpy array
#     buf = fig2data(fig)
#     w, h, d = buf.shape
#     # im = Image.open(cStringIO.StringIO(buf))
#     # return Image.frombuffer("RGBA", (w ,h), buf.tostring())
#     return Image.open("RGBA", buf.tostring())
#
#
def _resize(image, new_size):
  return cv2.resize(image, dsize=new_size, interpolation=cv2.INTER_CUBIC)


def compose_rgb(dataset_red, dataset_veggie, dataset_blue,
                tile_red: DatasetRegion, tile_veggie: DatasetRegion, tile_blue: DatasetRegion):
    def gamma_correction(image):
        # Apply range limits for each channel. RGB values must be between 0 and 1
        image = np.clip(image, 0, 1)
        # Apply a gamma correction to the image to correct ABI detector brightness
        gamma = 2.2
        return np.power(image, 1 / gamma)

    red_size = (tile_red.x_max - tile_red.x_min, tile_red.y_max - tile_red.y_min)
    red = dataset_red.variables['CMI'][tile_red.y_min: tile_red.y_max, tile_red.x_min: tile_red.x_max]
    veggie = dataset_veggie.variables['CMI'][tile_veggie.y_min: tile_veggie.y_max, tile_veggie.x_min: tile_veggie.x_max]
    blue = dataset_blue.variables['CMI'][tile_blue.y_min: tile_blue.y_max, tile_blue.x_min: tile_blue.x_max]

    red = gamma_correction(red)
    veggie = gamma_correction(veggie)
    blue = gamma_correction(blue)

    # Calculate the "True" Green
    veggie_resized = _resize(veggie, red_size)
    blue_resized = _resize(blue, red_size)
    green = 0.48358168 * red + 0.45706946 * blue_resized + 0.06038137 * veggie_resized
    green = np.clip(green, 0, 1)
    rgb = np.clip(np.dstack([red, green, blue_resized]), 0, 1)

    return rgb, red, green, blue_resized


def get_cropped_cv2_image(image, x: int, y: int, width, height):
    image_shape = image.shape
    return image[x:min(x+width, image_shape[0]), y:min(y+height, image_shape[1])]


def get_clipped(image, image_region: LatLonRegion, clip: LatLonRegion):
    image_shape = image.shape
    pixels_per_lon = image_shape[0] / abs(image_region.lon_east-image_region.lon_west)
    pixels_per_lat = image_shape[1] / abs(image_region.lat_south-image_region.lat_north)
    x = int(pixels_per_lon * abs(image_region.lon_east-clip.lon_west))
    y = int(pixels_per_lat * abs(image_region.lat_north-clip.lat_north))
    width = int(pixels_per_lon * abs(clip.lon_east-clip.lon_west))
    height = int(pixels_per_lat * abs(clip.lat_south-clip.lat_north))
    print('CLIP:', x, y, width, height)
    return image[x:min(x+width, image_shape[0]), y:min(y+height, image_shape[1])]


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
                      draw_labels=False)


def get_cloud_tops_palette():
    from matplotlib.colors import LinearSegmentedColormap
    filepath = os.path.join(LOCAL_BASE_PATH, 'smn_topes.cpt')
    cpt = load_cpt(filepath)
    return LinearSegmentedColormap('cpt', cpt)


def make_color_tuple(rgb):
    """
    Convert an 3D RGB array into an color tuple list suitable for plotting with
    pcolormesh.
    Input:
        rgb - a three dimensional array of RGB values from np.dstack([R, G, B])
    """
    # Don't use the last column of the RGB array or else the image will be scrambled!
    # This is the strange nature of pcolormesh.
    rgb = rgb[:, :-1, :]

    # Flatten the array, because that's what pcolormesh wants.
    color_tuple = rgb.reshape((rgb.shape[0] * rgb.shape[1]), 3)

    # Adding an alpha channel will plot faster, according to Stack Overflow. Not sure why.
    color_tuple = np.insert(color_tuple, 3, 1.0, axis=1)

    return color_tuple


def pcolormesh(ax: Axes, image, lons, lats, cmap=None, vmin=None, vmax=None):
    if len(image.shape) == 3:
        color_tuple = make_color_tuple(image)
        # ax.pcolormesh(lons, lats, np.zeros_like(lons),
        #               color=color_tuple, linewidth=0)
        ax.pcolormesh(lons, lats, image[:, :, 0], color=color_tuple)
    else:
        ax.pcolormesh(lons, lats, image, cmap=cmap, vmin=vmin, vmax=vmax)


def set_extent(ax: Axes, lonlat_region: LatLonRegion, trim_excess=0):
    extent = get_tile_extent(lonlat_region, trim_excess=trim_excess)
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
               lonlat_region: LatLonRegion,
               lats, lons,
               format=None,
               cmap=None, vmin=None, vmax=None,
               draw_cultural=False, draw_grid=False,
               trim_excess=0):
    if format is None:
        format = 'png'
        _, file_extension = os.path.splitext(filepath)
        if file_extension[0] == '.':
            format = file_extension[1:]
    figure = get_fig_stream(image, lonlat_region, lats, lons, format=format, cmap=cmap, vmin=vmin, vmax=vmax,
                    draw_cultural=draw_cultural, draw_grid=draw_grid, trim_excess=0)
    storage.upload_data(figure, filepath)
    figure.seek(0)
    return figure


def getfig(image,
           region: LatLonRegion,
           lats, lons,
           format='png',
           cmap=None, vmin=None, vmax=None,
           draw_cultural=False, draw_grid=False,
           trim_excess=0):
    image_inches = get_image_inches(image)
    fig = plt.figure(frameon=False)
    try:
        fig.set_size_inches(image_inches.x, image_inches.y)
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        ax.set_axis_off()
        set_extent(ax, region, trim_excess)

        if draw_cultural:
            add_cultural(ax)
        if draw_grid:
            add_grid(ax)
        else:
            ax.axis('off')

        pcolormesh(ax, image, lons, lats, cmap=cmap, vmin=vmin, vmax=vmax)
        fig.add_axes(ax, projection=ccrs.PlateCarree())
        return fig
    finally:
        # fig.clear()
        plt.close()


def get_fig_stream(image,
           region: LatLonRegion,
           lats, lons,
           format='png',
           cmap=None, vmin=None, vmax=None,
           draw_cultural=False, draw_grid=False,
           trim_excess=0):
    image_inches = get_image_inches(image)
    fig = plt.figure(frameon=False)
    try:
        fig.set_size_inches(image_inches.x, image_inches.y)
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        ax.set_axis_off()
        set_extent(ax, region, trim_excess)

        if draw_cultural:
            add_cultural(ax)
        if draw_grid:
            add_grid(ax)
        # else:
        ax.axis('off')

        pcolormesh(ax, image, lons, lats, cmap=cmap, vmin=vmin, vmax=vmax)
        fig.add_axes(ax, projection=ccrs.PlateCarree())
        buffer = io.BytesIO()
        plt.savefig(buffer, format=format, dpi=image_inches.dpi, bbox_inches='tight', pad_inches=0)
        buffer.seek(0)
        return buffer
    finally:
        fig.clear()
        plt.close()


def get_pil_img(image,
           region: LatLonRegion,
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
        set_extent(ax, region, trim_excess)

        if draw_cultural:
            add_cultural(ax)
        if draw_grid:
            add_grid(ax)
        else:
            ax.axis('off')

        pcolormesh(ax, image, lons, lats, cmap=cmap, vmin=vmin, vmax=vmax)
        fig.add_axes(ax, projection=ccrs.PlateCarree())
        return fig2img(fig)
    finally:
        fig.clear()
        plt.close()


