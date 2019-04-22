import re
import binascii

from . import _to_native
from . import _to_binary
from . import _DEFAULT_SOCKET_TIMEOUT
from .base import Base

_PYLIBMC_BEHAVIORS = {
    'connect_timeout': _DEFAULT_SOCKET_TIMEOUT * 1000,
    'send_timeout': _DEFAULT_SOCKET_TIMEOUT * 1000 * 1000,
    'receive_timeout': _DEFAULT_SOCKET_TIMEOUT * 1000 * 1000,
}


def _cmemcache_hash(key):
    return (((binascii.crc32(_to_binary(key)) & 0xffffffff) >> 16) & 0x7fff) or 1


_test_memcached_key = re.compile(r'[^\x00-\x21\xff]{1,250}$').match


class Memcached(Base):
    """A cache that uses memcached as backend.

    This cache looks into the following packages/modules to find bindings for
    memcached:

        - ``pylibmc``
        - ``google.appengine.api.memcached``
        - ``memcached``

    Implementation notes: This cache backend works around some limitations in
    memcached to simplify the interface. For example unicode keys are encoded
    to utf-8 on the fly. Methods such as :meth:`~Base.get_many` return
    the keys in the same format as passed. Furthermore all get methods
    silently ignore key erros to not cause problems when untrusted user data
    is passed to the get methods which is often the case in web applications.

    :param host: memcached server host
    :param port: memcached server port
    :param timeout: timeout
    :param prefix: a prefix that is added before all keys. Bear in mind that
                                    :meth:`~Base.clear` will clear keys with different prefix
    """

    def __init__(self, host='127.0.0.1', port=11211, timeout=None, prefix=None):
        Base.__init__(self, timeout)
        servers = ['{}:{}'.format(host, port)]
        self.prefix = _to_native(prefix)
        self._client = self.import_preferred_memcache_lib(servers)
        if self._client is None:
            raise RuntimeError('no memcache module found')

    def _normalize_key(self, key):
        key = _to_native(key, 'utf-8')
        if self.prefix:
            key = self.prefix + key
        return key

    def _normalize_timeout(self, timeout):
        if timeout is None:
            timeout = self.timeout
        return timeout

    def import_preferred_memcache_lib(self, servers):
        """Returns an initialized memcached client. Used by the constructor.
        """
        try:
            import pylibmc
        except ImportError:
            pass
        else:
            return pylibmc.Client(servers, behaviors=_PYLIBMC_BEHAVIORS)

        try:
            import memcache
            memcache.serverHashFunction = memcache.cmemcache_hash = _cmemcache_hash
        except ImportError:
            pass
        else:
            return memcache.Client(servers)

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        key = self._normalize_key(key)
        if _test_memcached_key(key):
            return self._client.get(key)

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
        key = self._normalize_key(key)
        timeout = self._normalize_timeout(timeout)
        return self._client.set(key, value, timeout)

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        key = self._normalize_key(key)
        if _test_memcached_key(key):
            return self._client.delete(key) is not 0
        return False

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        res = self._client.flush_all()
        return res if res is not None else True

    def incr(self, key, delta=1, noreply=False):
        """Increments the value of a key by `delta`. If the key does not yet exists it is initialized with `delta`

        For supporting caches this is an atomic operation

        :param key: the key to increment
        :param delta: the delta to add
        :param noreply: instructs the server not reply
        :returns: The new value or ``None`` for backend errors.
        """
        key = self._normalize_key(key)
        return self._client.incr(key, delta)

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        key = self._normalize_key(key)
        return self._client.decr(key, delta)

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will automatically be deleted.

        :param key: key to set timeout
        :param timeout: timeout value in seconds
        :returns: True if timeout was set, False if key does not exist or timeout could not be set
        :rtype: boolean
        """
        # This requires the version of memcached server >= 1.4.8
        return self._client.touch(self._normalize_key(key), timeout)

    def get_key_to_value(self, *keys):
        """Like :meth:`get_values` but return a dict::
        if the given key is missing, it will be missing from the response dict.

            d = cache.get_key_to_value('foo', 'bar')
            foo = d['foo']
            bar = d['bar']

        :param keys: The function accepts multiple keys as positional arguments
        """
        key_store = {}
        have_encoded_keys = False
        for key in keys:
            encoded_key = self._normalize_key(key)
            if not isinstance(key, str):
                have_encoded_keys = True
            if _test_memcached_key(key):
                key_store[encoded_key] = key
        d = rv = self._client.get_multi(key_store.keys())
        if have_encoded_keys or self.prefix:
            rv = {}
            for key, value in d.items():
                rv[key_store[key]] = value
        return rv

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
        key = self._normalize_key(key)
        timeout = self._normalize_timeout(timeout)
        return self._client.add(key, value, timeout)

    def set_many(self, timeout=None, noreply=False, **kw):
        """Sets multiple key-value pair

        :param timeout: the cache timeout for the key.
                            If not specificed, use the default timeout.
                            If specified 0, key will never expire
        :param noreply: instructs the server to not reply
        :returns: Whether all key-value pairs have been set
        :rtype: boolean
        """
        new_data = {}
        for key, value in kw.items():
            key = self._normalize_key(key)
            new_data[key] = value
        timeout = self._normalize_timeout(timeout)
        failed_keys = self._client.set_multi(new_data, timeout)
        return not failed_keys

    def delete_many(self, noreply=False, *keys):
        """Delete multiple keys at once.

        :param keys: The function accept multiple keys as positional arguments
        :param noreply: instructs the server not reply
        :returns: Whether all given keys have been deleted
        :rtype: boolen
        """
        new_keys = []
        for key in keys:
            key = self._normalize_key(key)
            if _test_memcached_key(key):
                new_keys.append(key)
        return self._client.delete_multi(new_keys) is not 0
