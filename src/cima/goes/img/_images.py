import io
import os
import cv2
from dataclasses import dataclass
from typing import Tuple
import numpy as np
import cartopy
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from cima.goes.storage._file_systems import Storage
from cima.goes.tiles import DatasetRegion, LatLonRegion, get_tile_extent
from cima.goes.utils.load_cpt import load_cpt
from matplotlib.axes import Axes
from PIL import Image


LOCAL_BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DUMMY_DPI = 1000


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
    return image[x:min(x+width, image_shape[1]), y:min(y+height, image_shape[0])]


def get_clipped(image, image_region: LatLonRegion, clip: LatLonRegion):
    image_height, image_width, _ = image.shape
    pixels_per_lon = image_width / abs(image_region.lon_east-image_region.lon_west)
    pixels_per_lat = image_height / abs(image_region.lat_south-image_region.lat_north)
    x = int(pixels_per_lon * abs(image_region.lon_east-clip.lon_east))
    y = int(pixels_per_lat * abs(image_region.lat_north-clip.lat_north))
    width = int(pixels_per_lon * abs(clip.lon_east-clip.lon_west))
    height = int(pixels_per_lat * abs(clip.lat_south-clip.lat_north))
    return image[y:min(y+height, image_height), x:min(x+width, image_width)]


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


def add_grid(ax, draw_labels=False, step=2):
    linewidth = 1.25
    gl = ax.gridlines(linewidth=linewidth,
                      linestyle='dotted',
                      color='gray',
                      crs=ccrs.PlateCarree(),
                      draw_labels=draw_labels)
    gl.xlocator = mticker.FixedLocator([x for x in range(-180, 180, step)])
    gl.ylocator = mticker.FixedLocator([x for x in range(-180, 180, step)])
    gl.xlabels_top = False
    gl.ylabels_right = False


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


def set_extent(ax: Axes, lonlat_region: LatLonRegion, trim_excess=0, projection=ccrs.PlateCarree()):
    extent = get_tile_extent(lonlat_region, trim_excess=trim_excess)
    ax.set_extent(extent, crs=projection)


@dataclass
class ImageResolution:
    dpi: int
    x: int
    y: int


def get_image_inches(image):
    y, x = image.shape[:2]
    return ImageResolution(DUMMY_DPI, x / float(DUMMY_DPI), y / float(DUMMY_DPI))


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
    figure = get_image_stream(image, lats, lons, lonlat_region, format=format, cmap=cmap, vmin=vmin, vmax=vmax,
                    draw_cultural=draw_cultural, draw_grid=draw_grid, trim_excess=trim_excess)
    storage.upload_data(figure, filepath)
    figure.seek(0)
    return figure


def getfig(image,
           region: LatLonRegion,
           lats, lons,
           format='png',
           projection=ccrs.PlateCarree(),
           cmap=None, vmin=None, vmax=None,
           draw_cultural=False, draw_grid=False,
           trim_excess=0):
    image_inches = get_image_inches(image)
    fig = plt.figure(frameon=False)
    try:
        fig.set_size_inches(image_inches.x, image_inches.y)
        if projection is not None:
            ax = fig.add_subplot(1, 1, 1, projection=projection)
        else:
            ax = fig.add_subplot(1, 1, 1)
        ax.set_axis_off()
        set_extent(ax, region, trim_excess, projection=projection)

        if draw_cultural:
            add_cultural(ax)
        if draw_grid:
            add_grid(ax)
        else:
            ax.axis('off')
        pcolormesh(ax, image, lons, lats, cmap=cmap, vmin=vmin, vmax=vmax)
        if projection is not None:
            fig.add_axes(ax, projection=projection)
        else:
            fig.add_axes(ax)
        return fig
    finally:
        # fig.clear()
        plt.close()


def interpolate_invalid(data):
    data = np.ma.masked_invalid(data)
    data = data.filled(np.nan)
    nans = np.isnan(data)
    nz = lambda x: x.nonzero()[0]
    data[nans] = np.interp(nz(nans), nz(~nans), data[~nans])
    return data


def get_image_stream(
        data,
        lats,
        lons,
        region: LatLonRegion = None,
        projection=ccrs.PlateCarree(),
        format='png',
        cmap=None,
        vmin=None,
        vmax=None,
        draw_cultural=False,
        draw_grid=False,
        title: str = None,
        grid_step=1,
        trim_excess=0):
    image_inches = get_image_inches(data)
    fig = plt.figure(frameon=False)
    try:
        # Interpolate invalid values to fix pcolormesh errors
        lons = interpolate_invalid(lons)
        lats = interpolate_invalid(lats)

        if projection is not None:
            ax = fig.add_subplot(1, 1, 1, projection=projection)
            if region is not None:
                set_extent(ax, region, trim_excess, projection=projection)
        else:
            ax = fig.add_subplot(1, 1, 1)
        ax.set_axis_off()
        fig.set_size_inches(image_inches.x, image_inches.y)
        if draw_cultural:
            add_cultural(ax)
        if draw_grid:
            add_grid(ax, step=grid_step)
        if title is not None:
            ax.title.set_text(title)
        ax.axis('off')

        pcolormesh(ax, data, lons, lats, cmap=cmap, vmin=vmin, vmax=vmax)

        if projection is not None:
            fig.add_axes(ax, projection=projection)
        else:
            fig.add_axes(ax)

        buffer = io.BytesIO()
        res = plt.savefig(buffer, format=format, dpi=image_inches.dpi, pad_inches=0)
        buffer.seek(0)
        return buffer
    finally:
        fig.clear()
        plt.close()


def get_pil_image(
        image,
        region: LatLonRegion,
        lats, lons,
        cmap=None, vmin=None, vmax=None,
        draw_cultural=False, draw_grid=False,
        trim_excess=0):
    image_stream = get_image_stream(image,
           lats=lats, lons=lons,
           region=region,
           cmap=cmap, vmin=vmin, vmax=vmax,
           draw_cultural=draw_cultural, draw_grid=draw_grid,
           trim_excess=trim_excess)
    return Image.open(image_stream)


def pil2cv(pil_image: Image):
    return np.array(pil_image.convert('RGB'))[:, :, ::-1].copy()
    # return np.array(pil_image)[:, :, ::-1].copy()


def cv2pil(cv_image: Image):
    img =  cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)
    return Image.fromarray(img)


def pil2stream(pil_image):
    stream = io.BytesIO()
    pil_image.save(stream, 'PNG')
    stream.seek(0)
    return stream


def stream2cv(image_stream):
    image_stream.seek(0)
    np_image = np.asarray(bytearray(image_stream.read()), dtype="uint8")
    return cv2.imdecode(np_image, cv2.IMREAD_COLOR)


def cv2stream(cv_image):
    pil_image = cv2pil(cv_image)
    return pil2stream(pil_image)


def stream2pil(image_stream) -> Image:
    image_stream.seek(0)
    return Image.open(image_stream).convert('RGB')


def contrast_correction(color, contrast):
    """
    Modify the contrast of an R, G, or B color channel
    See: #www.dfstudios.co.uk/articles/programming/image-programming-algorithms/image-processing-algorithms-part-5-contrast-adjustment/
    Input:
        C - contrast level
    """
    F = (259*(contrast + 255))/(255.*259-contrast)
    COLOR = F*(color-.5)+.5
    COLOR = np.minimum(COLOR, 1)
    COLOR = np.maximum(COLOR, 0)
    return COLOR


def get_true_colors(red, veggie, blue):
    # Turn empty values into nans
    red[red == -1] = np.nan
    veggie[veggie == -1] = np.nan
    blue[blue == -1] = np.nan

    R = np.maximum(red, 0)
    R = np.minimum(red, 1)
    G = np.maximum(veggie, 0)
    G = np.minimum(veggie, 1)
    B = np.maximum(blue, 0)
    B = np.minimum(blue, 1)

    gamma = 0.4
    R = np.power(R, gamma)
    G = np.power(G, gamma)
    B = np.power(B, gamma)

    G_true = 0.48358168 * R + 0.45706946 * B + 0.06038137 * G
    G_true = np.maximum(G_true, 0)
    G_true = np.minimum(G_true, 1)

    contrast = 125
    return contrast_correction(np.dstack([R, G_true, B]), contrast)


def apply_albedo(data):
    albedo = (data * np.pi * 0.3) / 663.274497
    albedo = np.clip(albedo, 0, 1)
    return np.power(albedo, 1.5)
