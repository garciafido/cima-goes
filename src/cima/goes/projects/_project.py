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
    # Log
    if log_storage is not None:
        if isinstance(log_storage, StorageInfo):
            log_storage = mount_storage(log_storage)
        dates_range = _get_resumed_range(dates_range, log_storage, log_path)
    for hour_range in dates_range.hours_ranges:
        hours = [hour for hour in range(hour_range.from_hour, hour_range.to_hour + 1)]
        grouped_blobs_list = goes_storage.grouped_one_day_blobs(
            date.year, date.month, date.day, hours,
            bands)
        logging_hour = None
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
            if log_storage is not None and logging_hour != hour:
                if logging_hour is not None:
                    _log_processed(f'{date.isoformat()}', dates_range, log_storage, log_path)
                _log_processed(f'# BEGIN {date.isoformat()} at {datetime.datetime.now().isoformat()}', dates_range, log_storage, log_path)
                logging_hour = hour
        if logging_hour is not None:
            _log_processed(f'{date.isoformat()}', dates_range, log_storage, log_path)
    return results


def _get_resumed_range(
        dates_range: DatesRange,
        log_storage: Storage,
        log_path: str,
        ) -> DatesRange:
    filepath = f'{log_path}/{dates_range.name}.log'
    print(filepath)
    try:
        data = log_storage.download_data(filepath)
        data = data.decode("utf-8").splitlines()
        data = [el for el in data if el[0] != '#']
        if len(data) > 0:
            data = data[-1]
            last_processed = datetime.datetime.fromisoformat(data)
            print(last_processed)
        return dates_range
    except Exception as e:
        print(filepath, e)
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
    print(filepath)
    log_storage.append_data(bytes(text+'\n', 'utf-8'), filepath)


class BatchProcess(object):
    def __init__(self,
                 goes_storage: GoesStorage,
                 bands: List[ProductBand],
                 date_ranges: List[DatesRange],
                 log_storage: Storage = None,
                 log_base_path: str = '',
                 machine_id: str = '',
                 ):
        self.bands = bands
        self.date_ranges = date_ranges
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
        for date_range in self.date_ranges:
            if workers > 1:
                for date in dates_range(date_range):
                    tasks.append(
                        Task(
                            _process_day,
                            process,
                            self.goes_storage.get_storage_info(),
                            self.bands,
                            date,
                            date_range,
                            *args,
                            storage=None if storage is None else storage.get_storage_info(),
                            log_storage=None if self.log_storage is None else self.log_storage.get_storage_info(),
                            log_path=self.log_path,
                            **kwargs)
                    )
            else:
                for date in dates_range(date_range):
                    result = _process_day(
                        process,
                        self.goes_storage,
                        self.bands,
                        date,
                        date_range,
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
