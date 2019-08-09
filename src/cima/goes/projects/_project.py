import datetime
from dataclasses import dataclass
from typing import List, Callable, Any, Dict, Tuple, Union

from cima.goes import ProductBand, Product, Band
from cima.goes.storage import BandBlobs, GoesBlob, GoesStorage, GroupedBandBlobs, mount_goes_storage
from cima.goes.storage import StorageInfo
from cima.goes.storage import mount_storage
from cima.goes.tasks import run_concurrent, Task


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


ProcessCall = Callable[[GoesStorage, int, int, int, int, int, Dict[Tuple[Product, Band], GoesBlob], List[Any], Dict[str, Any]], Any]


def process_day(process: ProcessCall,
                goes_storage: Union[StorageInfo, GoesStorage],
                bands: List[ProductBand],
                date: datetime.date,
                date_range: DatesRange,
                args, kwargs):
    if isinstance(goes_storage, StorageInfo):
        goes_storage = mount_goes_storage(goes_storage)
    results = []
    for hour_range in date_range.hours_ranges:
        hours = [hour for hour in range(hour_range.from_hour, hour_range.to_hour + 1)]
        grouped_blobs_list = goes_storage.grouped_one_day_blobs(
            date.year, date.month, date.day, hours,
            bands)
        for grouped_blobs in grouped_blobs_list:
            minute = int(grouped_blobs.start[9:11])
            hour = int(grouped_blobs.start[7:9])
            result = process(
                goes_storage,
                date.year, date.month, date.day, hour, minute,
                {(bb.product, bb.band): bb.blobs[0] for bb in grouped_blobs.blobs},
                *args, **kwargs
            )
            if result is not None:
                results.append(result)
    return results


class BatchProcess(object):
    def __init__(self,
                 goes_storage: GoesStorage,
                 bands: List[ProductBand],
                 date_ranges: List[DatesRange],
                 ):
        self.bands = bands
        self.date_ranges = date_ranges
        self.goes_storage = goes_storage

    def run(self, process: ProcessCall, workers=2, *args, **kwargs):
        def dates_range(date_range: DatesRange):
            current_date = date_range.from_date
            last_date = date_range.to_date
            while current_date <= last_date:
                yield current_date
                current_date = current_date + datetime.timedelta(days=1)

        for date_range in self.date_ranges:
            if workers > 1:
                tasks = []
                for date in dates_range(date_range):
                    tasks.append(
                        Task(
                            process_day,
                            process,
                            self.goes_storage.get_storage_info(),
                            self.bands,
                            date,
                            date_range,
                            args, kwargs)
                    )
                return run_concurrent(tasks, workers)
            else:
                results = []
                for date in dates_range(date_range):
                    result = process_day(
                        process,
                        self.goes_storage,
                        self.bands,
                        date,
                        date_range,
                        args, kwargs
                    )
                    if result is not None:
                        results.append(result)
                return results


