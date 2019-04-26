import random
from .base import Base


class Replication(Base):
    """Cache with multiple caches
    """

    def __init__(self, id, primary=None, caches=None, timeout=None):
        Base.__init__(timeout)
        self._clients = []
        self._primary_cache = None
        self._id = id
        caches = caches or {}
        for i, name in enumerate(caches.keys()):
            self._clients.append(create_cache_client(name, caches[name]))
            if primary is not None and primary == name:
                self._primary_cache = self._clients[i]

    def get_client(self):
        if self._primary_cache is not None:
            return self._primary_cache
        if not self._client:
            return
        return random.choice(self._clients)

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
        result = True
        for client in self._clients:
            if not client.set(key, value, timeout, noreply):
                result = False
        return result

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
        result = True
        for client in self._clients:
            if not client.set_not_overwrite(key, value, timeout, noreply):
                result = False
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
        result = True
        for client in self._clients:
            if not client.set_many(data, timeout, noreply):
                result = False
        return result

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        client = self.get_client()
        if client is None:
            return
        return client.get(key)

    def get_values(self, *keys):
        """Get valeus by keys

        foo, bar = cache.get_values('foo', 'bar')

        Share same error handling with :meth:`get`
        :param keys: the function acception multiple keys as positional arguments
        """
        client = self.get_client()
        if client is None:
            return
        return client.get_values(keys)

    def get_key_to_value(self, *keys):
        """Like :meth:`get_values` but return a dict::
        if the given key is missing, it will be missing from the response dict.

            d = cache.get_key_to_value('foo', 'bar')
            foo = d['foo']
            bar = d['bar']

        :param keys: The function accepts multiple keys as positional arguments
        """
        client = self.get_client()
        if client is None:
            return
        return client.get_key_to_value(keys)

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        result = True
        for client in self._clients:
            if not client.delete(key, noreply):
                result = False
        return result

    def delete_many(self, noreply=False, *keys):
        """Delete multiple keys at once.

        :param keys: The function accept multiple keys as positional arguments
        :param noreply: instructs the server not reply
        :returns: Whether all given keys have been deleted
        :rtype: boolen
        """
        result = True
        for client in self._clients:
            if not client.delete_many(keys, noreply):
                result = False
        return result

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.clear():
                result = False
        return result

    def incr(self, key, delta=1, noreply=False):
        """Increments the value of a key by `delta`. If the key does not yet exists it is initialized with `delta`

        For supporting caches this is an atomic operation

        :param key: the key to increment
        :param delta: the delta to add
        :param noreply: instructs the server not reply
        :returns: The new value or ``None`` for backend errors.
        """
        result = None
        for client in self._clients:
            result = client.incr(key, delta, noreply)
            if result is None:  # Is this buggy?
                return None
        return result

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        result = None
        for client in self._clients:
            result = client.decr(key, delta, noreply)
            if result is None:
                return None
        return result

    def block_left_pop(self, key, timeout=0):
        """Blocking pop a value from the head of the list.

        :param key: the key of list
        :param timeout: blocking timeout, 0 means block indefinitely
        :returns: The popped value or None if timeout.
        """
        result = None
        for client in self._clients:
            result = client.block_left_pop(key, timeout)
        return result

    def block_right_pop(self, key, timeout=0):
        """Blocking pop a value from the tail of the list.

        :param key: the key of list
        :param timeout: blocking timeout, 0 means block indefinitely
        :returns: The popped value or None if timeout.
        """
        result = None
        for client in self._clients:
            result = client.block_right_pop(key, timeout)
        return result

    def lindex(self, key, index):
        """Return the item from list at position `index`

        :param key: the key of list
        :param index: the position, can be negative
        :returns: The value at position `index` or None of index is out of range
        """
        client = self.get_client()
        if client is None:
            return
        return client.lindex(key, index)

    def llen(self, key):
        """Return the number of elements in list

        :param key: the key of list
        :returns: number of elements in list
        :rtype: int
        """
        client = self.get_client()
        if client is None:
            return None
        return client.llen(key)

    def lpop(self, key):
        """Pop a value from the head of list

        :param key: the key of list
        :returns: The popped value or None if list is empty
        """
        result = None
        for client in self._clients:
            result = client.lpop(key)
        return result

    def lpush(self, key, value):
        """Push a value to the head of the list

        :param key: the key of list
        :param value: the value to be pushed
        :returns: Whether the value has been added to list
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.lpush(key, value):
                result = False
        return result

    def lrange(self, key, start=0, end=-1):
        """Return a slice of the list

        :param key: the key of list
        :param start: the start position, can be negative
        :param end: the end position, can be negative
        :returns: The values between `start` and `end`
        :rtype: list
        """
        client = self.get_client()
        if client is None:
            return
        return client.lrange(key, start, end)

    def ltrim(self, key, start, end):
        """Trim the list, removing all values not within the slice

        :param key: the key of list
        :param start: the start postion, can be negative
        :param end: the end postion, can be negative
        :returns: whether the list has been trimmed
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.ltrim(key, start, end):
                result = False
        return result

    def rpop(self, key):
        """Pop a value from the tail of list

        :param key: the key of list
        :returns: the popped value or None if list is empty
        """
        result = None
        for client in self._clients:
            result = client.rpop(key)
        return result

    def rpush(self, key, value):
        """Push a value to the tail of the list

        :param key: the key of list
        :param value: the value to be pushed
        :returns: whether the value has been added to list
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.rpush(key, value):
                result = False
        return result

    def sadd(self, key, value):
        """Add a value to the set

        :param key: the key of set to add
        :param value: the value to be added
        :returns: whether the value has been added to set
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.sadd(key, value):
                result = False
        return result

    def sadd_many(self, key, *values):
        """Add multiple values to the set

        :param key: the key of set to add
        :param values: the values to be added
        :returns: whether the values has been added to set
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.sadd_many(key, *values):
                result = False
        return result

    def scard(self, key):
        """Return the number of elements in set

        :param key: the key of set
        :returns: number of elements in set
        :rtype: int
        """
        client = self.get_client()
        if client is None:
            return
        return client.scard(key)

    def sismember(self, key, value):
        """Return whether the value is a member of set

        :param key: the key of set
        :param value: the value to be checked
        :returns: whether the `value` is a member of a set
        :rtype: boolean
        """
        client = self.get_client()
        if client is None:
            return
        return client.sismember(key, value)

    def smembers(self, key):
        """Return all the members of set

        :param key: the key of set
        :returns: all the members value of set
        :rtype: set
        """
        client = self.get_client()
        if client is None:
            return
        return client.smembers(key)

    def srandmember(self, key):
        """Randomly return a member of set

        :param key: the key of set
        :returns: random member or None if empty
        """
        client = self.get_client()
        if client is None:
            return
        return client.srandmember(key)

    def srem(self, key, value):
        """Remove value from set

        :param key: the key of set
        :param value: the value to be removed
        :returns: whether the `value` has been removed from set
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.srem(key, value):
                result = False
        return result

    def hgetall(self, key):
        """Look up hash in the cache and return all fields and values for it

        :param key: the key of hash to be looked up
        :returns: The dict value of hash, if empty, return {}
        :rtype: dict
        """
        client = self.get_client()
        if client is None:
            return
        return client.hgetall(key)

    def hget(self, key, field):
        """Look up field in the hash and return the value of it.

        :param key: the key of hash to be lookup
        :param field: the field in the hash to be lookup
        :returns: the value if exists else ``None``
        """
        client = self.get_client()
        if client is None:
            return
        return client.hget(key, field)

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
        result = True
        for client in self._clients:
            if not client.hset(key, field, value, timeout, noreply):
                result = False
        return result

    def hdel(self, key, field, noreply=False):
        """Delete field of hash from the cache

        :param key: the key of hash to delete
        :param field: the field in the hash to delete
        :param noreply: instructs the server not reply
        :returns: whether the key has been deleted
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.hdel(key, field, noreply):
                result = False
        return result

    def hexists(self, key, field):
        """Check whether `field` is an existing field in the hash

        :param key: the key of hash
        :param field: the field to be checked
        :returns: whether `field` is an existing field in the hash
        :rtype: boolean
        """
        client = self.get_client()
        if client is None:
            return None
        return client.hexists(key, field)

    def hlen(self, key):
        """Get number of fields contained in the hash

        :param key: the key of hash
        :returns: the number of fields contained in the hash
        :rtype: int
        """
        client = self.get_client()
        if client is None:
            return
        return client.hlen(key)

    def zadd(self, key, value, score):
        """Add value to sorted set.

        :param key: the key of sorted set
        :param value: the value to be added
        :param score: the score of the value
        :returns: Whether the value has been set
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.zadd(key, value, score):
                result = False
        return result

    def zcard(self, key):
        """Return the number of values in sorted set

        :param key: the key of sorted set
        :returns: the number of values in sorted set
        :rtype: int
        """
        client = self.get_client()
        if client is None:
            return
        return client.zcard(key)

    def zcount(self, key, min, max):
        """Returns the number of values in the sorted set `key` with a score between `min` and `max`

        :param key: the key of sorted set
        :param min: min score
        :param max: max score
        :returns: the number of values
        :rtype: int
        """
        client = self.get_client()
        if client is None:
            return
        return client.zcount(key, min, max)

    def zincrby(self, key, value, delta=1):
        """Increment the score of `value` in sorted set `key` by `delta`

        :param key: the key of sorted set
        :param value: the value to be incremented
        :param delta: increment amount
        :returns: the new score
        :rtype: int
        """
        result = True
        for client in self._clients:
            result = client.zincrby(key, value, delta)
        return result

    def zrange(self, key, start=0, end=-1, reverse=False, withscores=False):
        """Return a range of values from sorted set `key` between `start` and `end`

        :param key: the key of sorted set
        :param start: start index
        :param end: end index
        :param reverse: sorted in descending order
        :returns: if withscores is True, return a list of (value, score) pairs, otherwise return a list of values
        :rtype: list
        """
        client = self.get_client()
        if client is None:
            return
        return client.zrange(key, start, end, reverse, withscores)

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
        client = self.get_client()
        if client is None:
            return None
        return client.zrangebyscore(key, min, max, start, num, reverse, withscores)

    def zrank(self, key, value, reverse=False):
        """Return the rank of `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be checked
        :param reverse: sorted in descending order
        :returns: the rank of `value` in sorted set `key` or None if not existed
        :rtype: int
        """
        client = self.get_client()
        if client is None:
            return
        return client.zrank(key, value, reverse)

    def zrem(self, key, value):
        """Remove the `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be removed
        :returns: whether the `value` has been removed
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.zrem(key, value):
                result = False
        return result

    def zremrangebyrank(self, key, start, end, reverse=False):
        """Remove the values in sorted set with scores between `start` and `end`

        :param key: the key of sorted set
        :param start: start index
        :param end: end index
        :param reverse: sorted in descending order
        :returns: the number of values removed
        :rtype: int
        """
        result = 0
        for client in self._clients:
            client_result = client.zremrangebyrank(key, start, end, reverse)
            if client_result > result:
                result = client_result
        return result

    def zremrangebyscore(self, key, min, max):
        """Remove the values in sorted set with scores between `min` and `max`

        :param key: the key of sorted set
        :param min: min score
        :param max: max score
        :returns: the number of values removed
        :rtype: int
        """
        result = 0
        for client in self._clients:
            client_result = client.zremrangebyscore(key, min, max)
            if client_result > result:
                result = client_result
        return result

    def zscore(self, key, value):
        """Return the score of `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be checked
        :returns: the score of `value` in sorted set `key` or None if not existed
        :rtype: float
        """
        client = self.get_client()
        if client is None:
            return
        return client.zscore(key, value)

    def zunionstore(self, dest, keys, aggregate=None):
        """Union multiple sorted sets into a new sorted set

        :param dest: destination sorted set
        :param keys: keys of sorted sets to be aggregated
        :param aggregate: specify how the results of the union are aggregated, defaults to SUM
        :returns: the number of elements in the resulting sorted set at destination
        :rtype: int
        """
        result = None
        for client in self._clients:
            result = client.zunionstore(dest, keys, aggregate)
        return result

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will automatically be deleted.

        :param key: key to set timeout
        :param timeout: timeout value in seconds
        :returns: True if timeout was set, False if key does not exist or timeout could not be set
        :rtype: boolean
        """
        result = True
        for client in self._clients:
            if not client.expire(key, timeout):
                result = False
        return result


def create_cache_client():
    """
    _cache_classes = {
    	'null': NullCache,
    	'memory': MemoryCache,
    	'memcached': MemcachedCache,
    	'pymemcached': PyMemcachedCache,
    	'redis': RedisCache,
    	'rawredis': RawRedisCache,
    	'ssdb': SsdbCache,
    	'replication': ReplicationCache,
    	'distribution': DistributionCache,
    	'multilayer': MultilayerCache,
    }

    def create_cache_client(cache_id, config):
    	cache_type = config['type']
    	config['id'] = cache_id
    	if cache_type not in _cache_classes:
    		raise RuntimeError('unknown_cache_type: %s' % cache_type)
    	return _cache_classes[cache_type](config)
    """
    pass


