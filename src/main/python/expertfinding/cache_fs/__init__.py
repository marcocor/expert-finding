import shelve
import threading


class Cache(object):
    def __init__(self, cache_file, readonly=False):
        self._cache_file = cache_file
        self._cache = shelve.open(cache_file, flag="r")
        self._lock = threading.Lock()
        self._readonly = readonly

    def cache(self, fun):
        return lambda *args: self._exec_if_not_in_cache(fun, *args)

    def _get_key(self, fun, *params):
        return fun.__name__ + str(params)

    def _exec_if_not_in_cache(self, fun, *params):
        fun_key = self._get_key(fun, *params)
        if self._cache.has_key(fun_key):
            return self._cache[fun_key]
        else:
            value = fun(*params)
            # self._lock.acquire()
            if not self._readonly:
                self._cache[fun_key] = value
            
            # self._cache.sync()
            # self._lock.release()
            return value

    def close(self):
        self._cache.close()

def test():
    cache = Cache('./prova')
    dbl = cache.cache(_dbl)
    sum = cache.cache(_sum)
    dbl(5)
    dbl(5)

    sum(3, 6)
    sum(2, 5)
    sum(3, 6)
    sum(2, 5)

def _dbl(x):
    return x*x

def _sum(a,b):
    return a + b
