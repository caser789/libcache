import six

from . import _DEFAULT_SOCKET_TIMEOUT
from . import _TCP_KEEP_ALIVE_OPTIONS
from .base import Base


class _SsdbConnectionPool(object):
    def __init__(self, connection):
        self._connection = connection

    def get_connection(self, command_name, *keys, **options):
        return self._connection

    def release(self, connection):
        return


class SSDB(Base):
    """Uses the SSDB key-value store as a cache backend.

    :param host: address of the SSDB server
    :param port: port number on which SSDB server listens to
    :param timeout: the default timeout, default 0
    :param prefix: A prefix that should be added to all keys

    Any additional keyword arguments will be passed to ``ssdb.Connection``
    """

    def __init__(self, host, port, timeout=0, prefix='', **kw):
        Base.__init__(timeout)

        # Currently most ssdb clients have not been updated and have a lot of quirks
        # There are no clients for Python 3.4+ available that support all ssdb functions properly
        import warnings
        warnings.warn('SSDB cache is deprecated, avoid using', DeprecationWarning)

        self.prefix = prefix
        kw['host'] = host
        kw['port'] = port
        if 'socket_timeout' not in kw:
            kw['socket_timeout'] = _DEFAULT_SOCKET_TIMEOUT
        try:
            # This module don't support PY3.4+
            import ssdb
            # Patch ssdb
            import datetime
            ssdb.connection.iteritems = six.iteritems
            def expire(self, name, ttl):
                if isinstance(ttl, datetime.timedelta):
                    ttl = ttl.seconds + ttl.days * 24 * 3600
                return self.execute_command('expire', name, ttl)
            ssdb.SSDB.expire = expire

            if 'socket_connect_timeout' not in kw:
                kw['socket_connect_timeout'] = _DEFAULT_SOCKET_TIMEOUT
            if 'socket_keepalive' not in kw:
                kw['socket_keepalive'] = 1
            if 'socket_keepalive_options' not in kw:
                kw['socket_keepalive_options'] = _TCP_KEEP_ALIVE_OPTIONS

            connection_pool = _SsdbConnectionPool(ssdb.Connection(**kw))
            self._client = ssdb.SSDB(connection_pool=connection_pool)
        except ImportError:
            try:
                # This module don't support socket keepalive yet, and do not support some features
                import pyssdb
                self._client = pyssdb.Client(**kw)
            except ImportError:
                raise RuntimeError('no ssdb module found')

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.get(key)

    def get_key_to_value(self, *keys):
        """Like :meth:`get_values` but return a dict::
        if the given key is missing, it will be missing from the response dict.

            d = cache.get_key_to_value('foo', 'bar')
            foo = d['foo']
            bar = d['bar']

        :param keys: The function accepts multiple keys as positional arguments
        """
        if self.prefix:
            keys = [self.prefix + key for key in keys]
        values = self._client.multi_get(*keys)
        if isinstance(values, list):
            # Handle pyssdb implementation differences
            # This function is to return keys as unicode instead of bytes, just like original ssdb
            # For pyssdb (or raw ssdb implementations), the list structure follows [key, value, key value, ...] format
            values = dict(
                [
                    (values[i*2].decode('utf-8'), values[i*2+1])
                    for i in range(len(values)//2)
                ]
            )
        if self.prefix:
            values = dict([
                (key[len(self.prefix):], value)
                for key, value in values.items()
            ])
        return values

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
        if self.prefix:
            key = self.prefix + key
        if timeout is None:
            timeout = self.timeout
        if timeout == 0:
            return self._client.set(key, value)
        return self._client.setx(key, value, timeout)

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
        if self.prefix:
            key = self.prefix + key
        if timeout is None:
            timeout = self.timeout
        result = self._client.setnx(key, value)
        if result and timeout != 0:
            result = self._client.expire(key, timeout)
        return result

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
        if self.prefix:
            new_data = {self.prefix+key: v for k, v in kw.items()}
            kw = new_data
        result = self._client.multi_set(**kw)
        if result and timeout != 0:
            for key in kw:
                result = self._client.expire(key, timeout)
        return result

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        if self.prefix:
            key = self.prefix + key
        self._client.delete(key)
        return True

    def delete_many(self, noreply=False, *keys):
        """Delete multiple keys at once.

        :param keys: The function accept multiple keys as positional arguments
        :param noreply: instructs the server not reply
        :returns: Whether all given keys have been deleted
        :rtype: boolen
        """
        if not keys:
            return True
        if self.prefix:
            keys = [self.prefix+key for key in keys]
        return self._client.multi_del(*keys)

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        status = 0
        last_key = ''
        while True:
            # ssdb default limit is 10 even if you don't put in limit, while pyssdb requires explicitly specifying limit
            keys = self._client.keys(last_key, '', 1000)
            if not keys:
                break
            # handle type conversion difference between ssdb implementations
            last_key = keys[-1]
            if isinstance(keys[0], six.binary_type):
                keys = (key.decode('utf-8') for key in keys)
            if self.prefix:
                keys = [key for key in keys if key.startswith(self.prefix)]
            status += self._client.multi_del(*keys)
        return status

    def incr(self, key, delta=1, noreply=False):
        """Increments the value of a key by `delta`. If the key does not yet exists it is initialized with `delta`

        For supporting caches this is an atomic operation

        :param key: the key to increment
        :param delta: the delta to add
        :param noreply: instructs the server not reply
        :returns: The new value or ``None`` for backend errors.
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.incr(key, delta)

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.decr(key, delta)

    def hgetall(self, key):
        """Look up hash in the cache and return all fields and values for it

        :param key: the key of hash to be looked up
        :returns: The dict value of hash, if empty, return {}
        :rtype: dict
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.hgetall(key)

    def hget(self, key, field):
        """Look up field in the hash and return the value of it.

        :param key: the key of hash to be lookup
        :param field: the field in the hash to be lookup
        :returns: the value if exists else ``None``
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.hget(key, field)

    def hset(self, key, field, value, timeout=None, noreply=False):
        """Add a new field/value to the hash in cache (overwrite value if exists)

        :param key: the key of hash to set
        :param field: the field in the hash to set
        :param value: the value for the field
        :param timeout: the cache timeout for the field.
                                        If not specified, it used the default timeout
                                        If specified 0, never expire

        :param noreply: instructs the server to not send the reply
        :returns: whether the key existed and has been set
        :rtype: boolean
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.hset(key, field, value)

    def hdel(self, key, field, noreply=False):
        """Delete field of hash from the cache

        :param key: the key of hash to delete
        :param field: the field in the hash to delete
        :param noreply: instructs the server not reply
        :returns: whether the key has been deleted
        :rtype: boolean
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.hdel(key, field)

    def hexists(self, key, field):
        """Check whether `field` is an existing field in the hash

        :param key: the key of hash
        :param field: the field to be checked
        :returns: whether `field` is an existing field in the hash
        :rtype: boolean
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.hexists(key, field)

    def hlen(self, key):
        """Get number of fields contained in the hash

        :param key: the key of hash
        :returns: the number of fields contained in the hash
        :rtype: int
        """
        if self.prefix:
            key = self.prefix + key
        return self._client.hlen(key)
