import datetime
from dataclasses import dataclass
from typing import List, Callable, Any, Dict

from cima.goes import ProductBand
from cima.goes.storage import BandBlobs, GoesBlob, GoesStorage, GroupedBandBlobs
from cima.goes.tiles import RegionData, TilesDict


@dataclass
class HoursRange:
    from_hour: int
    to_hour: int


@dataclass
class DatesRange:
    # From
    from_date: datetime.date
    # To
    to_date: datetime.date
    # Hours ranges
    hours_ranges: List[HoursRange]


class BatchProcess(object):
    def __init__(self,
                 goes_storage: GoesStorage,
                 bands: List[ProductBand],
                 date_ranges: List[DatesRange],
                 process_minute_blobs: Callable[[int, int, int, int, int, List[BandBlobs], List[Any], Dict[str, Any]], Any]
                 ):
        self.bands = bands
        self.date_ranges = date_ranges
        self.goes_storage = goes_storage
        self.process_minute_blobs = process_minute_blobs

    def run(self, *args, **kwargs):
        def dates_range(date_range: DatesRange):
            current_date = date_range.from_date
            last_date = date_range.to_date
            while current_date <= last_date:
                yield current_date
                current_date = current_date + datetime.timedelta(days=1)

        for date_range in self.date_ranges:
            for date in dates_range(date_range):
                for hour_range in date_range.hours_ranges:
                    for hour in range(hour_range.from_hour, hour_range.to_hour+1):
                        grouped_blobs_list = self.goes_storage.grouped_one_hour_blobs(
                            date.year, date.month, date.day, hour,
                            self.bands)
                        for grouped_blobs in grouped_blobs_list:
                            minute = int(grouped_blobs.start[9:11])
                            self.process_minute_blobs(date.year, date.month, date.day, hour, minute, grouped_blobs.blobs, *args, **kwargs)
