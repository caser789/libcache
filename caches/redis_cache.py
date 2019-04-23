import pickle
import platform

import six

if platform.system().lower() == 'linux':
    import socket
    _TCP_KEEP_ALIVE_OPTIONS = {
        socket.TCP_KEEPIDLE: 30,
        socket.TCP_KEEPINTVL: 5,
        socket.TCP_KEEPCNT: 5,
    }
else:
    _TCP_KEEP_ALIVE_OPTIONS = {}

from .base import Base
from . import _to_native
from . import _DEFAULT_SOCKET_TIMEOUT
from . import _DEFAULT_TIMEOUT


class Redis(Base):
    """Uses the Redis key-value store as a cache backend

    :param host: address of Redis server
    :param port: port number of Redis server
    :param unix_socket_path: unix socket file path
    :param password: password authentication for the Redis server
    :param db: db (zero-based numeric index) on Redis server to connect
    :param timeout: default timeout
    :param prefix: A prefix added to all keys

    Any additional keyword arguments will be passwed to ``redis.Redis``
    """

    def __init__(self, host, port, unix_socket_path=None, password=None, db=0, timeout=None, prefix='', default_scan_count=1000, **kw):
        Base.__init__(timeout)
        self.prefix = prefix
        self._default_scan_count = default_scan_count
        try:
            import redis
        except ImportError:
            raise RuntimeError('no redis module found')
        kwargs = dict(
            host=host,
            port=port,
            unix_socket_path=unix_socket_path,
            password=password,
            db=db,
        )
        if 'socket_timeout' not in kwargs:
            kwargs['socket_timeout'] = _DEFAULT_SOCKET_TIMEOUT
        if 'socket_connect_timeout' not in kwargs:
            kwargs['socket_connect_timeout'] = _DEFAULT_SOCKET_TIMEOUT
        if 'socket_keepalive' not in kwargs:
            kwargs['socket_keepalive'] = 1
        if 'socket_keepalive_options' not in kwargs:
            kwargs['socket_keepalive_options'] = _TCP_KEEP_ALIVE_OPTIONS

        self._client = redis.Redis(**kwargs)

    def _normalize_key(self, key):
        key = _to_native(key, 'utf-8')
        if self.prefix:
            key = self.prefix + key
        return key

    def dumps(self, value):
        """Dumps an object into a string for redis. By default it serialized
        integers as regular string and pickle dumps everyting else.
        """
        if type(value) in six.integer_types:  # pylint: disable=unidiomatic-typecheck
            return str(value).encode('ascii')
        return b'!' + pickle.dumps(value, pickle.HIGHEST_PROTOCOL)

    def loads(self, value):
        """The reversal of :meth:`dumps`. This might be called with None
        """
        if value is None:
            return None

        if value.startswith(b'!'):
            try:
                return pickle.loads(value[1:])
            except pickle.PickleError:
                return None
        try:
            return int(value)
        except ValueError:
            return value

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        return self.loads(self._client.get(self._normalize_key(key)))

    def get_values(self, *keys):
        """Get valeus by keys

        foo, bar = cache.get_values('foo', 'bar')

        Share same error handling with :meth:`get`
        :param keys: the function acception multiple keys as positional arguments
        """
        keys = [self._normalize_key(key) for key in keys]
        return [self.loads(x) for x in self._client.mget(keys)]

    def get_key_to_value(self, *keys):
        """Like :meth:`get_values` but return a dict::
        if the given key is missing, it will be missing from the response dict.

            d = cache.get_key_to_value('foo', 'bar')
            foo = d['foo']
            bar = d['bar']

        :param keys: The function accepts multiple keys as positional arguments
        """
        query_keys = [self._normalize_key(key) for key in keys]
        values = self._client.mget(query_keys)
        res = {}
        for i in range(len(keys)):
            value = values[i]
            if value is not None:
                res[keys[i]] = self.loads(value)
        return res

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
            timeout = self.default_timeout
        value = self.dumps(value)
        key = self._normalize_key(key)
        if timeout == 0:
            return self._client.set(name=key, value=value)
        return self._client.setex(name=key, value=value, time=timeout)

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
            timeout = self.default_timeout
        if timeout == 0:
            timeout = None
        key = self._normalize_key(key)
        value = self.dumps(value)
        # This requires the version of redis server >= 2.6.12, please refer
        # https://github.com/andymccurdy/redis-py/issues/387 for more details.
        return self._client.set(key, value, nx=True, ex=timeout)

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
        pipe = self._client.pipeline()
        for key, value in kw.items():
            value = self.dumps(value)
            key = self._normalize_key(key)
            if timeout == 0:
                pipe.set(name=key, value=value)
            else:
                pipe.setex(name=key, value=value, time=timeout)
        return pipe.execute()

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        self._client.delete(self._normalize_key(key))
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
        keys = [self._normalize_key(key) for key in keys]
        self._client.delete(*keys)
        return True

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        from redis.exceptions import ResponseError
        status = False
        client = self._client
        if self.prefix:
            pattern = self.prefix + '*'
            try:
                cursor = '0'
                while cursor != 0:
                    cursor, keys = client.scan(cursor=cursor, match=pattern, count=self._default_scan_count)
                    if keys:
                        status = client.delete(*keys)
            except ResponseError:
                keys = client.keys(pattern)
                if keys:
                    status = client.delete(*keys)
        else:
            status = client.flushdb()
        return status

    def incr(self, key, delta=1, noreply=False):
        """Increments the value of a key by `delta`. If the key does not yet exists it is initialized with `delta`

        For supporting caches this is an atomic operation

        :param key: the key to increment
        :param delta: the delta to add
        :param noreply: instructs the server not reply
        :returns: The new value or ``None`` for backend errors.
        """
        return self._client.incr(name=self._normalize_key(key), amount=delta)

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        return self._client.decr(name=self._normalize_key(key), amount=delta)

    def block_left_pop(self, key, timeout=0):
        """Blocking pop a value from the head of the list.

        TODO: why loads res[1], is it res[1:] ?

        :param key: the key of list
        :param timeout: blocking timeout, 0 means block indefinitely
        :returns: The popped value or None if timeout.
        """
        res = self._client.blpop([self._normalize_key(key)], timeout)
        if res is not None:
            res = self.loads(res[1])
        return res

    def block_right_pop(self, key, timeout=0):
        """Blocking pop a value from the tail of the list.

        TODO: ?

        :param key: the key of list
        :param timeout: blocking timeout, 0 means block indefinitely
        :returns: The popped value or None if timeout.
        """
        res = self._client.brpop([self._normalize_key(key)], timeout)
        if res is not None:
            res = self.loads(res[1])
        return res

    def lindex(self, key, index):
        """Return the item from list at position `index`

        :param key: the key of list
        :param index: the position, can be negative
        :returns: The value at position `index` or None of index is out of range
        """
        return self.loads(self._client.lindex(self._normalize_key(key), index))

    def llen(self, key):
        """Return the number of elements in list

        :param key: the key of list
        :returns: number of elements in list
        :rtype: int
        """
        return self._client.llen(self._normalize_key(key))

    def lpop(self, key):
        """Pop a value from the head of list

        :param key: the key of list
        :returns: The popped value or None if list is empty
        """
        return self.loads(self._client.lpop(self._normalize_key(key)))

    def lpush(self, key, value):
        """Push a value to the head of the list

        :param key: the key of list
        :param value: the value to be pushed
        :returns: Whether the value has been added to list
        :rtype: boolean
        """
        return self._client.lpush(self._normalize_key(key), self.dumps(value))

    def lrange(self, key, start=0, end=-1):
        """Return a slice of the list

        :param key: the key of list
        :param start: the start position, can be negative
        :param end: the end position, can be negative
        :returns: The values between `start` and `end`
        :rtype: list
        """
        return [self.loads(v) for v in self._client.lrange(self._normalize_key(key), start, end)]

    def ltrim(self, key, start, end):
        """Trim the list, removing all values not within the slice

        :param key: the key of list
        :param start: the start postion, can be negative
        :param end: the end postion, can be negative
        :returns: whether the list has been trimmed
        :rtype: boolean
        """
        return self._client.ltrim(self._normalize_key(key), start, end)

    def rpop(self, key):
        """Pop a value from the tail of list

        :param key: the key of list
        :returns: the popped value or None if list is empty
        """
        return self.loads(self._client.rpop(self._normalize_key(key)))

    def rpush(self, key, value):
        """Push a value to the tail of the list

        :param key: the key of list
        :param value: the value to be pushed
        :returns: whether the value has been added to list
        :rtype: boolean
        """
        return self._client.rpush(self._normalize_key(key), self.dumps(value))

    def sadd(self, key, value):
        """Add a value to the set

        :param key: the key of set to add
        :param value: the value to be added
        :returns: whether the value has been added to set
        :rtype: boolean
        """
        return self._client.sadd(self._normalize_key(key), self.dumps(value)) >= 0

    def sadd_many(self, key, *values):
        """Add multiple values to the set

        :param key: the key of set to add
        :param values: the values to be added
        :returns: whether the values has been added to set
        :rtype: boolean
        """
        return self._client.sadd(self._normalize_key(key), *[self.dumps(val) for val in values]) >= 0

    def scard(self, key):
        """Return the number of elements in set

        :param key: the key of set
        :returns: number of elements in set
        :rtype: int
        """
        return self._client.scard(self._normalize_key(key))

    def sismember(self, key, value):
        """Return whether the value is a member of set

        :param key: the key of set
        :param value: the value to be checked
        :returns: whether the `value` is a member of a set
        :rtype: boolean
        """
        return self._client.sismember(self._normalize_key(key), self.dumps(value))

    def smembers(self, key):
        """Return all the members of set

        :param key: the key of set
        :returns: all the members value of set
        :rtype: set
        """
        values = self._client.smembers(self._normalize_key(key))
        if values:
            values = set([self.loads(v) for v in value])
        return values

    def srandmember(self, key):
        """Randomly return a member of set

        :param key: the key of set
        :returns: random member or None if empty
        """
        return self.loads(self._client.srandmember(self._normalize_key(key)))

    def srem(self, key, value):
        """Remove value from set

        :param key: the key of set
        :param value: the value to be removed
        :returns: whether the `value` has been removed from set
        :rtype: boolean
        """
        return self._client.srem(self._normalize_key(key), self.dumps(value)) >= 0


    ########################################
    # Hash
    #
    # hgetall
    # hget
    # hset
    # hdel
    # hexists
    # hlen
    ########################################

    def hgetall(self, key):
        """Look up hash in the cache and return all fields and values for it

        :param key: the key of hash to be looked up
        :returns: The dict value of hash, if empty, return {}
        :rtype: dict
        """
        kv= = self._client.hgetall(self._normalize_key(key))
        if kv is not None:
            for k, v in kv.items():
                kv[k] = self.loads(v)
        return kv

    def hget(self, key, field):
        """Look up field in the hash and return the value of it.

        :param key: the key of hash to be lookup
        :param field: the field in the hash to be lookup
        :returns: the value if exists else ``None``
        """
        return self.loads(self._client.hget(self._normalize_key(key), field))

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
        self._client.hset(self._normalize_key(key), field, self.dumps(value))
        return True

    def hdel(self, key, field, noreply=False):
        """Delete field of hash from the cache

        :param key: the key of hash to delete
        :param field: the field in the hash to delete
        :param noreply: instructs the server not reply
        :returns: whether the key has been deleted
        :rtype: boolean
        """
        self._client.hdel(self._normalize_key(key), field)
        return True

    def hexists(self, key, field):
        """Check whether `field` is an existing field in the hash

        :param key: the key of hash
        :param field: the field to be checked
        :returns: whether `field` is an existing field in the hash
        :rtype: boolean
        """
        return self._client.hexists(self._normalize_key(key), field)

    def hlen(self, key):
        """Get number of fields contained in the hash

        :param key: the key of hash
        :returns: the number of fields contained in the hash
        :rtype: int
        """
        return self._client.hlen(self._normalize_key(key))

    ########################################
    # Zset
    #
    # zadd
    # zcard
    # zcount
    # zincrby
    # zrange
    # zrangebyscore
    # zrank
    # zrem
    # zremrangebyrank
    # zremrangebyscore
    # zscore
    # zunionstore
    ########################################

    def zadd(self, key, value, score):
        """Add value to sorted set.

        :param key: the key of sorted set
        :param value: the value to be added
        :param score: the score of the value
        :returns: Whether the value has been set
        :rtype: boolean
        """
        return self._client.zadd(self._normalize_key(key), self.dumps(value), score) >= 0

    def zcard(self, key):
        """Return the number of values in sorted set

        :param key: the key of sorted set
        :returns: the number of values in sorted set
        :rtype: int
        """
        return self._client.zcard(self._normalize_key(key))

    def zcount(self, key, min, max):
        """Returns the number of values in the sorted set `key` with a score between `min` and `max`

        :param key: the key of sorted set
        :param min: min score
        :param max: max score
        :returns: the number of values
        :rtype: int
        """
        return self._client.zcount(self._normalize_key(key), min, max)

    def zincrby(self, key, value, delta=1):
        """Increment the score of `value` in sorted set `key` by `delta`

        :param key: the key of sorted set
        :param value: the value to be incremented
        :param delta: increment amount
        :returns: the new score
        :rtype: int
        """
        return self._client.zincrby(self._normalize_key(key), self.dumps(value), delta)

    def zrange(self, key, start=0, end=-1, reverse=False, withscores=False):
        """Return a range of values from sorted set `key` between `start` and `end`

        :param key: the key of sorted set
        :param start: start index
        :param end: end index
        :param reverse: sorted in descending order
        :returns: if withscores is True, return a list of (value, score) pairs, otherwise return a list of values
        :rtype: list
        """
        values = self._client.zrange(self._normalize_key(key), start, end, reverse, withscores)
        if withscores:
            values = [(self.loads(v), s) for v, s in values]
        else:
            values = [self.loads(v) for v in values]
        return values

    def zrangebyscore(self, key, min, max, start=None, num=None, reverse=False, withscores=False):
        """Return a range of values from sorted set `key` with scores between `min` and `max`

        :param key: the key of sorted set
        :param min: min score
        :param max: max score
        :param start: start offset
        :param num: limit count
        :param reverse: sorted in descending order
        :param withscores: return the scores along with the values
        :returns: if withscores is True, return a list of (value, score) pairs, otherwise return a list of values
        :rtype: list
        """
        client = self._client
        key = self._normalize_key(key)
        if reverse:
            values = client.zrevrangebyscore(key, max, min, start, num, withscores)
        else:
            values = client.zrangebyscore(key, min, max, start, num, withscores)
        if withscores:
            values = [(self.loads(v), s) for v, s in values]
        else:
            values = [self.loads(v) for v in values]
        return values

    def zrank(self, key, value, reverse=False):
        """Return the rank of `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be checked
        :param reverse: sorted in descending order
        :returns: the rank of `value` in sorted set `key` or None if not existed
        :rtype: int
        """
        client = self._client
        key = self._normalize_key(key)
        value = self.dumps(value)
        if reverse:
            return client.zrevrank(key, value)
        return client.rank(key, value)

    def zrem(self, key, value):
        """Remove the `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be removed
        :returns: whether the `value` has been removed
        :rtype: boolean
        """
        return self._client.zrem(self._normalize_key(key), self.dumps(value)) >= 0

    def zremrangebyrank(self, key, start, end, reverse=False):
        """Remove the values in sorted set with scores between `start` and `end`

        :param key: the key of sorted set
        :param start: start index
        :param end: end index
        :param reverse: sorted in descending order
        :returns: the number of values removed
        :rtype: int
        """
        if reverse:
            start, end = -end-1, -start-1
        return self._client.zremrangebyrank(self._normalize_key(key), start, end)

    def zremrangebyscore(self, key, min, max):
        """Remove the values in sorted set with scores between `min` and `max`

        :param key: the key of sorted set
        :param min: min score
        :param max: max score
        :returns: the number of values removed
        :rtype: int
        """
        return self._client.zremrangebyscore(self._normalize_key(key), min, max)

    def zscore(self, key, value):
        """Return the score of `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be checked
        :returns: the score of `value` in sorted set `key` or None if not existed
        :rtype: float
        """
        return self._client.zscore(self._normalize_key(key), self.dumps(value))

    def zunionstore(self, dest, keys, aggregate=None):
        """Union multiple sorted sets into a new sorted set

        :param dest: destination sorted set
        :param keys: keys of sorted sets to be aggregated
        :param aggregate: specify how the results of the union are aggregated, defaults to SUM
        :returns: the number of elements in the resulting sorted set at destination
        :rtype: int
        """
        return self._client.zunionstore(self._normalize_key(dest), [self._normalize_key(k) for k in keys], aggregate)

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will automatically be deleted.

        :param key: key to set timeout
        :param timeout: timeout value in seconds
        :returns: True if timeout was set, False if key does not exist or timeout could not be set
        :rtype: boolean
        """
        return self._client.expire(self._normalize_key(key), timeout)
