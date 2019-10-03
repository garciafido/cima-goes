import datetime
import os
from dataclasses import dataclass
from typing import List, Callable, Any, Dict, Tuple, Union
from multiprocessing import Lock, Manager

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
                 hours: List[int],
                 dates_range: DatesRange,
                 *args,
                 storage: Storage = None,
                 _log_storage: Storage = None,
                 _log_path: str = None,
                 _lock: Lock = None,
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
    for hour in hours:
        grouped_blobs_list = goes_storage.grouped_one_hour_blobs(
            date.year, date.month, date.day, hour,
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
        if _log_storage is not None:
            if isinstance(_log_storage, StorageInfo):
                _log_storage = mount_storage(_log_storage)
            _log_processed(f'{date.isoformat()} {hour}# at {datetime.datetime.now().isoformat()} ({diff_time(current_time)})',
                           dates_range, _log_storage, _log_path, _lock)
            print(f'Completed {date.isoformat()} {hour} at {datetime.datetime.now().isoformat()} ({diff_time(current_time)})')
    return results


def _get_dates_and_hours(date_range: DatesRange):
    dates = {}
    current_date = date_range.from_date
    last_date = date_range.to_date
    while current_date <= last_date:
        dates[current_date] = []
        for hour_range in date_range.hours_ranges:
            hours = [hour for hour in range(hour_range.from_hour, hour_range.to_hour + 1)]
            dates[current_date].extend(hours)
        current_date = current_date + datetime.timedelta(days=1)
    return dates


def _get_resumed_range(
        dates_range: DatesRange,
        log_storage: Storage,
        log_path: str,
        ) -> Dict[datetime.date, List[int]]:
    filepath = f'{log_path}/{dates_range.name}.log'
    dates_and_hours = _get_dates_and_hours(dates_range)
    try:
        data = log_storage.download_data(filepath)
        data = data.decode("utf-8").splitlines()
        data_datetime = set([el.split('#')[0] for el in data if el[0] != '#'])
        data_per_date = {}
        if len(data) > 0:
            for dt in data_datetime:
                d, t = dt.split(' ')
                if d not in data_per_date:
                    data_per_date[d] = [t]
                else:
                    data_per_date[d].append(t)
            for d, hours in data_per_date.items():
                year, month, day = map(int, d.split('-'))
                date = datetime.date(year=year, month=month, day=day)
                if date in dates_and_hours:
                    for hour in data_per_date[d]:
                        dates_and_hours[date].remove(int(hour))
        return dates_and_hours
    except Exception as e:
        msg = str(e).replace('\n', '# ')
        init_str = f'# INIT {datetime.datetime.now().isoformat()} {msg}\n'
        log_storage.upload_data(bytes(init_str, 'utf-8'), filepath)
        return dates_and_hours


def _log_processed(
        text: str,
        dates_range,
        log_storage: Storage,
        log_path: str,
        lock: Lock,
        ) -> DatesRange:
    filepath = f'{log_path}/{dates_range.name}.log'
    with lock:
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
        tasks = []
        results = []
        manager = Manager()
        lock = manager.Lock()
        for range in self.dates_ranges:
            # Check if resume range
            dates_and_hours = []
            if self.log_storage is not None:
                last_from = range.from_date
                dates_and_hours = _get_resumed_range(range, self.log_storage, self.log_path)
                if dates_and_hours and last_from <= max(dates_and_hours.keys()):
                    _log_processed(
                        f'# RESUMED from {last_from.isoformat()} to {range.to_date} at {datetime.datetime.now().isoformat()}',
                        range, self.log_storage, self.log_path, lock)
            else:
                dates_and_hours = _get_dates_and_hours(range)

            if not dates_and_hours:
                _log_processed(
                    f'# NOTHING TO DO from {range.from_date} to {range.to_date} at {datetime.datetime.now().isoformat()}',
                    range, self.log_storage, self.log_path, lock)
            else:
                if workers > 1:
                    for date, hours in dates_and_hours.items():
                        tasks.append(
                            Task(
                                _process_day,
                                process,
                                self.goes_storage.get_storage_info(),
                                self.bands,
                                date,
                                hours,
                                range,
                                *args,
                                storage=None if storage is None else storage.get_storage_info(),
                                _log_storage=None if self.log_storage is None else self.log_storage.get_storage_info(),
                                _log_path=self.log_path,
                                _lock=lock,
                        **kwargs)
                        )
                else:
                    for date, hours in dates_and_hours.items():
                        result = _process_day(
                            process,
                            self.goes_storage,
                            self.bands,
                            date,
                            hours,
                            range,
                            *args,
                            storage=storage,
                            _log_storage=self.log_storage,
                            _log_path=self.log_path,
                            _lock=lock,
                            **kwargs
                        )
                        if result is not None:
                            results.append(result)

        if tasks:
            return run_concurrent(tasks, workers)
        return results
