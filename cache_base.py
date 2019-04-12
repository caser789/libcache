""" Base class for cache system
"""
default_timeout = 1


class CacheBase(object):
    """Base class for the cache system. All the caches implement this API or a superset of it.

    :param config: the config object
    """

    def __init__(self, timeout=None):
        self._client = None
        self.timeout = timeout or default_timeout

    @property
    def raw_client(self):
        """Get raw cache server client.

        :returns: underlying cache client object.
        """
        return self._client

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        return None

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        return True

    def get_values(self, *keys):
        """Get valeus by keys
        
        foo, bar = cache.get_values('foo', 'bar')
        
        Share same error handling with :meth:`get`
        :param keys: the function acception multiple keys as positional arguments
        """
        key_to_value = self.get_key_to_value(*keys)
        return [key_to_value.get(k) for k in keys]

    def get_key_to_value(self, *keys):
        """Like :meth:`get_values` but return a dict::
        if the given key is missing, it will be missing from the response dict.

            d = cache.get_key_to_value('foo', 'bar')
            foo = d['foo']
            bar = d['bar']

        :param keys: The function accepts multiple keys as positional arguments
        """
        return dict([(k, self.get(k)) for k in keys])

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
        return True

    def set_or_overwrite(self, key, value, timeout=None, noreply=False):
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
        return True

    def set_many(self, timeout=None, noreply=False, **kw):
        """Sets multiple key-value pair

        :param timeout: the cache timeout for the key.
                            If not specificed, use the default timeout.
                            If specified 0, key will never expire
        :param noreply: instructs the server to not reply
        :returns: Whether all key-value pairs have been set
        :rtype: boolean
        """
        res = True
        for k, v in kw.items():
            if not self.set(k, v, timeout):
                res = False
        return res

    def delete_many(self, noreply=False, *keys):
        """Delete multiple keys at once.

        :param keys: The function accept multiple keys as positional arguments
        :param noreply: instructs the server not reply
        :returns: Whether all given keys have been deleted
        :rtype: boolen
        """
        return all(self.delete(key) for key in keys)

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        return True

    def incr(self, key, delta=1, noreply=False):
        """Increments the value of a key by `delta`. If the key does not yet exists it is initialized with `delta`

        For supporting caches this is an atomic operation

        :param key: the key to increment
        :param delta: the delta to add
        :param noreply: instructs the server not reply
        :returns: The new value or ``None`` for backend errors.
        """
        value = (self.get(key) or 0) + delta
        return value if self.set(key, value) else None

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        value = (self.get(key) or 0) - delta
        return value if self.set(key, value) else None
