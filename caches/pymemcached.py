import six

from . import _DEFAULT_SOCKET_TIMEOUT
from . import _DEFAULT_TIMEOUT
from . import _to_native
from .base import Base


try:
    from pymemcache.client import base
    base.VALID_STRING_TYPES = (six.text_type, )
except:
    pass

from pymemcache.client import Client
from pymemcache.serde import python_memcache_serializer
from pymemcache.serde import python_memcache_deserializer


class PyMemcached(Base):
    """A cache client based on pymemcache. Implemented by pure python and support noreply
    """

    def __init__(self, host, port, timeout=None, prefix=''):
        Base.__init__(self, timeout)
        self._client = Client(
            (host, port),
            serializer=python_memcache_serializer,
            deserializer=python_memcache_deserializer,
            connect_timeout=_DEFAULT_SOCKET_TIMEOUT,
            timeout=timeout or _DEFAULT_TIMEOUT,
            key_prefix=prefix,
        )

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        return self._client.get(_to_native(key))

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        self._client.delete(_to_native(key), noreply)
        return True

    def get_values(self, *keys):
        """Get valeus by keys

        foo, bar = cache.get_values('foo', 'bar')

        Share same error handling with :meth:`get`
        :param keys: the function acception multiple keys as positional arguments
        """
        return self._client.get_many([_to_native(key) for key in keys])

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
        if timeout is None:
            timeout = self.timeout
        return self._client.set(_to_native(key), value, timeout, noreply)

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
        if timeout is None:
            timeout = self.timeout
        return self._client.add(_to_native(key), value, timeout, noreply)

    def set_many(self, timeout=None, noreply=False, **kw):
        """Sets multiple key-value pair

        :param timeout: the cache timeout for the key.
                            If not specificed, use the default timeout.
                            If specified 0, key will never expire
        :param noreply: instructs the server to not reply
        :returns: Whether all key-value pairs have been set
        :rtype: boolean
        """
        if timeout is None:
            timeout = self.timeout
        res = self._client.set_many(dict((_to_native(k), v) for k, v in kw.items()), timeout, noreply)
        if isinstance(res, list):
            # pymemcache 2.x+ returns a list of keys that failed to update, instead of hard-coded boolean
            return not res
        return res

    def delete_many(self, noreply=False, *keys):
        """Delete multiple keys at once.

        :param keys: The function accept multiple keys as positional arguments
        :param noreply: instructs the server not reply
        :returns: Whether all given keys have been deleted
        :rtype: boolen
        """
        return self._client.delete_many([_to_native(key) for key in keys], noreply)

    def incr(self, key, delta=1, noreply=False):
        """Increments the value of a key by `delta`. If the key does not yet exists it is initialized with `delta`

        For supporting caches this is an atomic operation

        :param key: the key to increment
        :param delta: the delta to add
        :param noreply: instructs the server not reply
        :returns: The new value or ``None`` for backend errors.
        """
        return self._client.incr(_to_native(key), delta, noreply)

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        return self._client.decr(_to_native(key), delta, noreply)

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        return self._client.flush_all(noreply=False)

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will automatically be deleted.

        :param key: key to set timeout
        :param timeout: timeout value in seconds
        :returns: True if timeout was set, False if key does not exist or timeout could not be set
        :rtype: boolean
        """
        # This requires the version of memcached server >= 1.4.8
        return self._client.touch(_to_native(key), timeout)
