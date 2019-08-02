import time


def timeit(method, log_name: str=None, acum_dict: dict=None):
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        name = kwargs.get('log_name', method.__name__)
        if acum_dict is not None:
            acum_dict[name] = int((te - ts) * 1000)
        else:
            print(f'{name}  {(te - ts) * 1000:.2f} ms')
        return result
    return timed
