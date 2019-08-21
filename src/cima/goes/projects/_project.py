import datetime
import os
from dataclasses import dataclass
from typing import List, Callable, Any, Dict, Tuple, Union

from cima.goes import ProductBand, Product, Band
from cima.goes.storage import GoesBlob, GoesStorage, mount_goes_storage
from cima.goes.storage import StorageInfo
from cima.goes.storage import mount_storage
from cima.goes.storage._file_systems import Storage
from cima.goes.tasks import run_concurrent, Task
from cima.goes.utils import start_time, diff_time

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
    # ID for identify processes
    name: str = 'X'


ProcessCall = Callable[[GoesStorage, int, int, int, int, int, Dict[Tuple[Product, Band], GoesBlob], List[Any], Dict[str, Any]], Any]


def _process_day(process: ProcessCall,
                 goes_storage: Union[StorageInfo, GoesStorage],
                 bands: List[ProductBand],
                 date: datetime.date,
                 dates_range: DatesRange,
                 *args,
                 storage: Storage = None,
                 log_storage: Storage = None,
                 log_path: str = None,
                 **kwargs):
    if isinstance(goes_storage, StorageInfo):
        goes_storage = mount_goes_storage(goes_storage)
    if isinstance(storage, StorageInfo):
        storage = mount_storage(storage)
    if storage is not None:
        kwargs['storage'] = storage
    results = []

    # Process loop
    current_time = start_time()
    if log_storage is not None:
        if isinstance(log_storage, StorageInfo):
            log_storage = mount_storage(log_storage)
        _log_processed(f'# BEGIN {date.isoformat()} at {datetime.datetime.now().isoformat()}',
                       dates_range, log_storage, log_path)
    for hour_range in dates_range.hours_ranges:
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
                *args,
                **kwargs
            )
            if result is not None:
                results.append(result)
    if log_storage is not None:
        _log_processed(f'{date.isoformat()}', dates_range, log_storage, log_path)
        _log_processed(f'# END at {datetime.datetime.now().isoformat()} ({diff_time(current_time)})',
                       dates_range, log_storage, log_path)
    return results


def _get_resumed_range(
        dates_range: DatesRange,
        log_storage: Storage,
        log_path: str,
        ) -> DatesRange:
    filepath = f'{log_path}/{dates_range.name}.log'
    try:
        data = log_storage.download_data(filepath)
        data = data.decode("utf-8").splitlines()
        data = [el for el in data if el[0] != '#']
        if len(data) > 0:
            data = data[-1]
            last_processed = datetime.datetime.fromisoformat(data)
            dates_range.from_date = (last_processed + datetime.timedelta(days=1)).date()
            print(f'Last processed {dates_range.name}:', last_processed)
        return dates_range
    except Exception as e:
        init_str = f'# INIT {datetime.datetime.now().isoformat()}\n'
        log_storage.upload_data(bytes(init_str, 'utf-8'), filepath)
        return dates_range


def _log_processed(
        text: str,
        dates_range,
        log_storage: Storage,
        log_path: str,
        ) -> DatesRange:
    filepath = f'{log_path}/{dates_range.name}.log'
    log_storage.append_data(bytes(text+'\n', 'utf-8'), filepath)


class BatchProcess(object):
    def __init__(self,
                 goes_storage: GoesStorage,
                 bands: List[ProductBand],
                 dates_ranges: List[DatesRange],
                 log_storage: Storage = None,
                 log_base_path: str = '',
                 machine_id: str = '',
                 ):
        self.bands = bands
        self.dates_ranges = dates_ranges
        self.goes_storage = goes_storage
        self.log_storage = log_storage
        self.machine_id = machine_id
        self.log_base_path = log_base_path
        self.log_path = os.path.join(f'{log_base_path}', f'{machine_id}')

    def run(self, process: ProcessCall, *args, workers=1, storage: Storage=None, **kwargs):
        def dates_range(date_range: DatesRange):
            current_date = date_range.from_date
            last_date = date_range.to_date
            while current_date <= last_date:
                yield current_date
                current_date = current_date + datetime.timedelta(days=1)

        tasks = []
        results = []
        for range in self.dates_ranges:

            # Check if resume range
            if self.log_storage is not None:
                last_from = range.from_date
                range = _get_resumed_range(range, self.log_storage, self.log_path)
                if last_from < range.from_date:
                    _log_processed(
                        f'# RESUMED from {range.from_date} to {range.to_date} at {datetime.datetime.now().isoformat()}',
                        range, self.log_storage, self.log_path)
            if range.from_date > range.to_date:
                _log_processed(
                    f'# NOTHING TO DO from {range.from_date} to {range.to_date} at {datetime.datetime.now().isoformat()}',
                    range, self.log_storage, self.log_path)
            else:
                if workers > 1:
                    for date in dates_range(range):
                        tasks.append(
                            Task(
                                _process_day,
                                process,
                                self.goes_storage.get_storage_info(),
                                self.bands,
                                date,
                                range,
                                *args,
                                storage=None if storage is None else storage.get_storage_info(),
                                log_storage=None if self.log_storage is None else self.log_storage.get_storage_info(),
                                log_path=self.log_path,
                                **kwargs)
                        )
                else:
                    for date in dates_range(range):
                        result = _process_day(
                            process,
                            self.goes_storage,
                            self.bands,
                            date,
                            range,
                            *args,
                            storage=storage,
                            log_storage=self.log_storage,
                            log_path=self.log_path,
                            **kwargs
                        )
                        if result is not None:
                            results.append(result)

        if tasks:
            return run_concurrent(tasks, workers)
        return results
