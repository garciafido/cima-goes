import psutil
import gc


def get_usage():
    gc.collect()
    cpu = psutil.cpu_percent()
    memory = psutil.virtual_memory()
    return {'CPU%': cpu, "free GB": memory.free >> 30}
