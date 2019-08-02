import os

from cima.goes.utils.load_cpt import load_cpt
from matplotlib.colors import LinearSegmentedColormap


LOCAL_BASE_PATH = os.path.dirname(os.path.abspath(__file__))


def _get_cloud_tops_palette():
    filepath = os.path.join(LOCAL_BASE_PATH, 'smn_topes.cpt')
    cpt = load_cpt(filepath)
    return LinearSegmentedColormap('cpt', cpt)


CLOUD_TOPS_PALETTE = _get_cloud_tops_palette()

