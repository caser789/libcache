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
