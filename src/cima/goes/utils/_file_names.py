from dataclasses import dataclass
from enum import Enum

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


class Product(Enum):
    ACMF = 'ABI-L2-ACMF' # Clear Sky Masks
    ACHAF = 'ABI-L2-ACHAF' # Cloud Top Height
    ACHTF = 'ABI-L2-ACHTF' # Cloud Top Temperature
    ACTPF = 'ABI-L2-ACTPF' # Cloud Top Phase
    ADPF = 'ABI-L2-ADPF' # Aerosol Detection (including Smoke and Dust)
    AODF = 'ABI-L2-AODF' # Aerosol Optical Depth
    CMIPF = 'ABI-L2-CMIPF' # Cloud and Moisture Image Product – Full Disk
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
    RadC = 'ABI-L1b-RadC'
    RadF = 'ABI-L1b-RadF'
    RadM = 'ABI-L1b-RadM'


class Band(Enum):
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


class mode(Enum):
    M3 = 'M3'
    M4 = 'M4'


OR = 'OR' # Operational System Real-Time Data
G16 = 'G16' # GOES-16


def path_prefix(year, day_of_year, hour, product=Product.CMIPF):
    return f'{product.value}/{year:04d}/{day_of_year:03d}/{hour:02d}/'


def file_pattern(band, product=Product.CMIPF, mode=mode.M3):
    return f'{OR}_{product.value}-{mode.value}C{band:02d}_{G16}'


def slice_obs_start(product=Product.CMIPF):
    prefix_pos = len(path_prefix(year=1111, day_of_year=11, hour=11, product=product)) + len(
        file_pattern(band=2, product=product)) + 2
    return slice(prefix_pos, prefix_pos + len('20183650045364'))

