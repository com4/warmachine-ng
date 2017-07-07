from collections import Hashable
import functools
from hashlib import sha1 as hash_
import logging


class memoize(object):
    """
    Decorator that caches a function's return value each time it is called with
    the same arguments.
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}
        self.log = logging.getLogger('memoize')

    @classmethod
    def _hash(cls, string):
        return hash_(string.encode()).hexdigest()

    def __call__(self, *args, **kwargs):
        # if not isinstance(args, Hashable) or not isinstance(kwargs, Hashable):
        #     self.log.debug('Uncacheable')
        #     return self.func(*args, **kwargs)

        h = self._hash(str(args) + str(kwargs))
        if h in self.cache:
            # This is noisy, but helpful for debugging so it's commented out
            # self.log.debug('Using cached value for {}({}, {})'.format(
            #     self.func.__name__, ', '.join(str(a) for a in args),
            #     ','.join('{}={} '.format(k, v) for k, v in kwargs.items())))
            return self.cache[h]
        else:
            value = self.func(*args, **kwargs)
            self.cache[h] = value

            return value

    def __get__(self, obj, objtype):
        return functools.partial(self.__call__, obj)
