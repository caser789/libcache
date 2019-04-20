import time
import six

from collections import namedtuple

from . import _to_native
from . import _numeric_types
from .base import Base


_Value = namedtuple('_Value', ['timeout', 'value'])


class Memory(Base):

    _TRIM_INTERVAL = 60

    def __init__(self, timeout=None, trim_internal=None):
        Base.__init__(self, timeout)
        self._trim_interval = trim_internal or self._TRIM_INTERVAL
        self._store = {}
        self._last_trim_time = time.time()

    @property
    def raw_client(self):
        """Get raw cache server client.

        :returns: underlying cache client object.
        """
        return self._store

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        key = _to_native(key)
        value = self._store.get(key)
        if value is None:
            return None
        if value.timeout is not None and value.timeout < time.time():
            del self._store[key]
            return None
        return value.value

    def set(self, key, value, timeout=None, noreply=False):
        """Add a new key/value to the cache (overwrite value, if key exists)

        :param key: the key to set
        :param value: the value of the key
        :param timeout: the cache timeout for the key.
                            If not specificed, use the default timeout.
                            If specified 0, key will never expire
        :param noreply: instructs the server to not reply
        :returns: Whether the key existed and has been set
        :rtype: boolean
        TODO: __set__
        """
        self._trim()
        key = _to_native(key)
        self._store[key] = _Value(self._normalize_timeout(timeout), value)
        return True

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        self._trim()
        key = _to_native(key)
        self._store.pop(key, None)
        return True

    def get_key_to_value(self, *keys):
        """Like :meth:`get_values` but return a dict::
        if the given key is missing, it will be missing from the response dict.

            d = cache.get_key_to_value('foo', 'bar')
            foo = d['foo']
            bar = d['bar']

        :param keys: The function accepts multiple keys as positional arguments
        """
        d = {}
        now = time.time()
        for key in keys:
            key = _to_native(key)
            value = self._store.get(key)
            if value is None: continue
            if value.timeout is not None and value.timeout < now: continue
            d[key] = value.value
        return d

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        self._store = {}
        return True

    def set_not_overwrite(self, key, value, timeout=None, noreply=False):
        """Works like :meth:`set` but does not overwrite the existing value

        :param key: the key to set
        :param value: the value of the key
        :param timeout: the cache timeout for the key.
                            If not specificed, use the default timeout.
                            If specified 0, key will never expire
        :param noreply: instructs the server to not reply
        :returns: Whether the key existed and has been set
        :rtype: boolean
        """
        self._trim()
        key = _to_native(key)
        old_value = self._store.get(key)
        if old_value is not None and (old_value.timeout is None or old_value.timeout >= time.time()):
            return False
        self._store[key] = _Value(self._normalize_timeout(timeout), value)
        return True

    def incr(self, key, delta=1, noreply=False):
        """Increments the value of a key by `delta`. If the key does not yet exists it is initialized with `delta`

        For supporting caches this is an atomic operation

        :param key: the key to increment
        :param delta: the delta to add
        :param noreply: instructs the server not reply
        :returns: The new value or ``None`` for backend errors.
        """
        key = _to_native(key)
        old_value = self._store.get(key)

        if old_value is None or (old_value.timeout is not None and old_value.timeout < time.time()):
            # Should incr an outdated value?
            self._store[key] = _Value(None, delta)
            return delta

        timeout = old_value.timeout
        value = old_value.value
        if isinstance(value, _numeric_types):
            value += delta
        elif isinstance(value, (six.binary_type, six.text_type)):
            try:
                value = six.text_type(int(value) + delta)
            except:
                return
        else:
            # Should raise Error?
            return
        self._store[key] = _Value(timeout, value)
        return int(value)

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        return self.incr(key, -delta, noreply)

    def _trim(self):
        now = time.time()
        can_trim = self._last_trim_time + self._trim_interval < now
        if can_trim:
            keys_2_trim = []
            for key, value in self._store.items():
                if value.timeout is not None and value.timeout < now:
                    keys_2_trim.append(key)
            for key in keys_2_trim:
                del self._store[key]
            self._last_trim_time = now

    def _normalize_timeout(self, timeout):
        if timeout is None:
            timeout = time.time() + self.timeout
        elif timeout == 0:
            timeout = None
        else:
            timeout = time.time() + timeout
        return timeout
