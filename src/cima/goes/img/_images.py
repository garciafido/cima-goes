import os
import cartopy
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from cima.goes.utils.load_cpt import load_cpt

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


def add_grid(ax, lonlat_region):
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


def trim_excess(lonlat_region):
    latsouth = lonlat_region[2]
    latnorth = lonlat_region[3]
    lonwest = lonlat_region[0]
    loneast = lonlat_region[1]
    return [lonwest+0.5, loneast-0.5, latsouth+0.5, latnorth-0.5]


def save_image(image, filepath, tile, lats, lons, cmap='gray', vmin=0, vmax=0.7, cultural=False, extent=None):
    dirpath = os.path.dirname(os.path.abspath(filepath))
    if not os.path.exists(dirpath):
        os.makedirs(dirpath, exist_ok=True)
    dummy_dpi = 100
    x, y = image.shape
    fig = plt.figure(frameon=False)
    try:
        fig.set_size_inches(x / dummy_dpi, y / dummy_dpi)

        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        ax.set_axis_off()
        if extent is not None:
            ax.set_extent(extent, crs=ccrs.PlateCarree())
        if cultural:
            add_cultural(ax)
            add_grid(ax, extent)
        ax.pcolormesh(lons, lats, image, cmap=cmap, vmin=vmin, vmax=vmax)

        fig.add_axes(ax, projection=ccrs.PlateCarree())
        ax.axis('off')
        plt.savefig(filepath + '.png', dpi=dummy_dpi, bbox_inches='tight', pad_inches=0)
    finally:
        fig.clear()
        plt.close()
