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
        """
        return True
