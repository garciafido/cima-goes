import datetime
from dataclasses import dataclass
from enum import IntEnum, Enum
import re

# File neme pattern:
# OR_ABI-L2–CMIPF–M3C09_G16_sYYYYJJJHHMMSSs_eYYYYJJJHHMMSSs_cYYYYJJJHHMMSSs.nc
# Where:
# OR: Operational System Real-Time Data
# ABI-L2: Advanced Baseline Imager Level 2+
# CMIPF: Cloud and Moisture Image Product – Full Disk
# M3 / M4: ABI Mode 3 or ABI Mode 4
# C09: Channel Number (Band 9 in this example)
# G16: GOES-16
# sYYYYJJJHHMMSSs: Observation Start
# eYYYYJJJHHMMSSs: Observation End
# cYYYYJJJHHMMSSs: File Creation
#
# http://edc.occ-data.org/goes16/getdata/


class Product(Enum):
    ACMF = 'ABI-L2-ACMF' # Clear Sky Masks
    ACHAF = 'ABI-L2-ACHAF' # Cloud Top Height
    ACHTF = 'ABI-L2-ACHTF' # Cloud Top Temperature
    ACTPF = 'ABI-L2-ACTPF' # Cloud Top Phase
    ADPF = 'ABI-L2-ADPF' # Aerosol Detection (including Smoke and Dust)
    AODF = 'ABI-L2-AODF' # Aerosol Optical Depth
    CMIPF = 'ABI-L2-CMIPF' # Cloud and Moisture Image Product – Full Disk
    MCMIPF = 'ABI-L2-MCMIPF'
    MCMIPC = 'ABI-L2-MCMIPC'
    CODF = 'ABI-L2-CODF' # Cloud Optical Depth
    CPSF = 'ABI-L2-CPSF' # Cloud Particle Size Distribution
    DSIF = 'ABI-L2-DSIF' # Derived Stability Indices
    DMWF = 'ABI-L2-DMWF' # Derived Motion Winds
    FDCF = 'ABI-L2-FDCF' # Fire / Hot Spot Characterization
    FSCF = 'ABI-L2-FSCF' # Snow Cover
    LSTF = 'ABI-L2-LSTF' # Land Surface Temperature (Skin)
    RRQPEF = 'ABI-L2-RRQPEF' # Rainfall Rate /QPE
    SSTF = 'ABI-L2-SSTF' # Sea Surface Temperature (Skin)
    TPWF = 'ABI-L2-TPWF' # Total Precipitable Water
    VAAF = 'ABI-L2-VAAF' # Volcanic Ash
    LCFA = 'GLM-L2-LCFA'
    # Conus
    CMIPC = 'ABI-L2-CMIPC' # Cloud & Moisture Imagery. Conus. 5 Minutes
    RadC = 'ABI-L1b-RadC' # Radiances. Conus. 5 Minutes
    # Full disk
    RadF = 'ABI-L1b-RadF' # Radiances. Full disk. 15 Minutes
    # Mesoscale
    CMIPM = 'ABI-L2-CMIPM' # Cloud & Moisture Imagery. Mesoscale. 30-60 seconds
    MCMIPM = 'ABI-L2-MCMIPM' # Multi-Band Cloud & Moisture Imagery. Mesoscale.  30-60 seconds
    RadM = 'ABI-L1b-RadM' # Radiances. Mesoscale. 30-60 seconds


class Band(IntEnum):
    BLUE = 1
    RED = 2
    VEGGIE = 3
    CIRRUS = 4
    SNOW_ICE = 5
    CLOUD_PARTICLE_SIZE = 6
    SHORTWAVE_WINDOW = 7
    UPPER_LEVEL_TROPOSPHERIC_WATER_VAPOR = 8
    MID_LEVEL_TROPOSPHERIC_WATER_VAPOR = 9
    LOWER_LEVEL_WATER_VAPOR = 10
    CLOUD_TOP_PHASE = 11
    OZONE = 12
    CLEAN_LONGWAVE_WINDOW = 13
    IR_LONGWAVE_WINDOW = 14
    DIRTY_LONGWAVE_WINDOW = 15
    CO2_LONGWAVE_INFRARED = 16


@dataclass
class ProductBand:
    product: Product
    band: Band
    subproduct: int = None


OR = 'OR' # Operational System Real-Time Data
G16 = 'G16' # GOES-16
ANY_MODE = 'M.'


def get_day_of_year(year, month, day):
    return datetime.datetime(year=year, month=month, day=day).timetuple().tm_yday


def path_prefix(year: int, month: int, day: int, hour: int, product=Product.CMIPF):
    day_of_year = get_day_of_year(year, month, day)
    return f'{product.value}/{year:04d}/{day_of_year:03d}/{hour:02d}/'


def day_path_prefix(year: int, month: int, day: int, product=Product.CMIPF):
    day_of_year = get_day_of_year(year, month, day)
    return f'{product.value}/{year:04d}/{day_of_year:03d}/'


def file_name(band: Band, product=Product.CMIPF, mode=ANY_MODE, subproduct: int = None):
    subp = subproduct if subproduct is not None else ''
    return f'{OR}_{product.value}{subp}-{mode}C{band:02d}_{G16}'


def hour_file_name(hour: int, band: Band, product=Product.CMIPF, mode=ANY_MODE, subproduct: int = None):
    subp = subproduct if subproduct is not None else ''
    return f'{hour:02d}/{OR}_{product.value}{subp}-{mode}C{band:02d}_{G16}'


def file_regex_pattern(band: Band, product: Product = Product.CMIPF, mode: str = ANY_MODE, subproduct: int = None):
    return re.compile(file_name(band, product, mode, subproduct=subproduct))


def hour_file_regex_pattern(hour: int, band: Band, product: Product = Product.CMIPF, mode: str = ANY_MODE, subproduct: int = None):
    return re.compile(hour_file_name(hour, band, product, mode, subproduct=subproduct))


def slice_obs_start(product=Product.CMIPF, subproduct: int = None):
    prefix_pos = len(path_prefix(year=1111, month=1, day=1, hour=11, product=product)) + len(
        file_name(band=Band.RED, product=product, subproduct=subproduct)) + 2
    return slice(prefix_pos, prefix_pos + len('20183650045364'))


# Browse: https://console.cloud.google.com/storage/browser/gcp-public-data-goes-16
GOES_PUBLIC_BUCKET = 'gcp-public-data-goes-16'


def get_gcs_url(filepath: str):
    return f'https://storage.cloud.google.com/{GOES_PUBLIC_BUCKET}/{filepath}'

