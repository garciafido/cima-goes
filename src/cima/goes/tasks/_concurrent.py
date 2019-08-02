import asyncio
import concurrent.futures
from dataclasses import dataclass
from typing import Callable, List, Dict, Tuple


@dataclass
class Task:
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args if args else []
        self.kwargs = kwargs if kwargs else {}
    func: Callable
    args: Tuple
    kwargs: Dict


Tasks = List[Task]


async def _run(tasks: Tasks, workers):
    def submit(task):
        return executor.submit(task.func, *task.args, **task.kwargs)

    out = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [submit(task) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                out.append(future.result())
            except Exception as e:
                out.append(e)
    return out


def run_concurrent(tasks: Tasks, workers):
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()

    try:
        future = asyncio.ensure_future(_run(tasks, workers))
        results = loop.run_until_complete(future)
        return results
    finally:
        loop.close()
