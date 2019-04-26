import re
from collections import defaultdict

from . import _to_native

from .base import Base

# TODO
class ConHash(object):
    pass


# TODO
HASH_METHOD_MD5 = ''

def create_cache_client():
    pass


class Distribution(Base):

    """A cache distributor based on cache key

    :param method: support following methods, default conhash
        conhash: children should have property replica, default replica is 32
        mod: require key_regex, factor. client_id = key  % factor
        div: require key_regex, factor. client_id = key / factor
        hash_mod: require factor. client_id = crc32(key) % factor
        hash_div: require factor. client_id = crc32(key) / factor
        hash_method: use 'md5' or 'crc32' to hash keys
    :param key_regex: regex used to extract base key from cache key to caculate client_id
        the first regex group matched in cache key will be used as base key
        :param factor: divisor in mod or div method
    """

    def __init__(self, id, hash_method=None, method='conhash', timeout=None):
        Base.__init__(self, timeout)
        self._client = {}
        self._id = id
        self._method = method
        if self._method in ('mod', 'div', 'hash_mod', 'hash_div'):
            self._init_method_mod_div()
        else:
            self._init_method_conhash(hash_method)

    def _init_method_conhash(self, hash_method, caches):
        if hash_method == 'md5':
            self._conhash = ConHash(hash_method=HASH_METHOD_MD5)
        else:
            self._conhash = ConHash()
        caches = caches or {}
        for i, name in enumerate(caches.keys()):
            self._client[name] = create_cache_client(name, caches[name])
            self._conhash.add_node(str(name), caches[name].get('replica', 32), i)
        self.get_client_by_key = self._get_client_by_key_conhash

    def _get_client_by_key_conhash(self, key):
        client_id = self._conhash.lookup(_to_native(key))
        return self._client.get(client_id)

    def _init_method_mod_div(self, key_regex, factor, caches):
        if self._method == 'mod' or self._method == 'div':
            self._regex = re.compile(key_regex)
        self._factor = factor
        caches = caches or {}
        for name in caches:
            self._client[int(name)] = create_cache_client(name, caches[name])
        self.get_client_by_key = {
            'mod': self._get_client_by_key_mod,
            'div': self._get_client_by_key_div,
            'hash_mod': self._get_client_by_key_hash_mod,
            'hash_div': self._get_client_by_key_hash_div,
        }[self._method]

    def _get_client_by_key_mod(self, key):
        try:
            client_id = int(self._regex.match(key).group(1)) % self._factor
        except:
            return
        return self._client.get(client_id)

    def _get_client_by_key_div(self, key):
        try:
            client_id = int(self._regex.match(key).group(1)) / self._factor
        except:
            return
        return self._client.get(client_id)

    def _get_client_by_key_hash_mod(self, key):
        client_id = crypt.crc32(_to_binary(key)) % self._factor
        return self._client.get(client_id)

    def _get_client_by_key_hash_div(self, key):
        client_id = crypt.crc32(_to_binary(key)) / self._factor
        return self._client.get(client_id)

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
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.set(key, value, timeout, noreply)

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
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.set_not_overwrite(key, value, timeout, noreply)

    def set_many(self, timeout=None, noreply=False, **kw):
        """Sets multiple key-value pair

        :param timeout: the cache timeout for the key.
                            If not specificed, use the default timeout.
                            If specified 0, key will never expire
        :param noreply: instructs the server to not reply
        :returns: Whether all key-value pairs have been set
        :rtype: boolean
        """
        query_table = defaultdict(dict)
        for key, value in kw.items():
            client = self.get_client_by_key(key)
            if client is None:
                return False
            query_table[client][key] = value
        for client, query in query_table.items():
            if not client.set_many(timeout, noreply, **query):
                return False
        return True

    def get(self, key):
        """Look up the `key` in cache and return the value of it.

        :param key: the `key` to be looked up
        :returns: the value if it exists and is readable, else ``None``.

        TODO: support __get__
        """
        client = self.get_client_by_key(key)
        if client is None:
            return
        return client.get(key)

    def get_values(self, *keys):
        """Get valeus by keys

        foo, bar = cache.get_values('foo', 'bar')

        Share same error handling with :meth:`get`
        :param keys: the function acception multiple keys as positional arguments
        """
        query_table = defaultdict(list)
        for key in keys:
            client = self.get_client_by_key(key)
            if client is None:
                return
            query_table[client].append(key)
        values = {}
        for client, query in query_table.items():
            result = client.get_values(query)
            if result is None:
                return
            values.update(result)
        return values

    def delete(self, key, noreply=False):
        """Delete `key` from cache.

        :param key: the `key` to delete.
        :param noreply: instruct the server to not reply.
        :returns: whether the key been deleted.
        :rtype: boolean

        TODO: __del__
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.delete(key, noreply)

    def delete_many(self, noreply=False, *keys):
        """Delete multiple keys at once.

        :param keys: The function accept multiple keys as positional arguments
        :param noreply: instructs the server not reply
        :returns: Whether all given keys have been deleted
        :rtype: boolen
        """
        query_table = defaultdict(list)
        for key in keys:
            client = self.get_client_by_key(key)
            if client is None:
                return False
            query_table[client].append(key)
        for client, query in query_table.items():
            if not client.delete_many(query, noreply):
                return False
        return True

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        result = True
        for client in self._client.values():
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
        client = self.get_client_by_key(key)
        if client is None:
            return
        return client.incr(key, delta, noreply)

    def decr(self, key, delta=1, noreply=False):
        """Decrements the value of a key by `delta`. If the key does not yet exists it is initialized with `-delta`.

        For supporting caches this is an atomic operation.

        :param key: the key to increment
        :param delta: the delta to subtruct
        :param noreply: instructs the server not reply
        :returns: The new value or `None` for backend errors.
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.decr(key, delta, noreply)

    def block_left_pop(self, key, timeout=0):
        """Blocking pop a value from the head of the list.

        :param key: the key of list
        :param timeout: blocking timeout, 0 means block indefinitely
        :returns: The popped value or None if timeout.
        """
        client = self.get_client_by_key(key)
        if client is None:
            return
        return client.block_left_pop(key, timeout)

    def block_right_pop(self, key, timeout=0):
        """Blocking pop a value from the tail of the list.

        :param key: the key of list
        :param timeout: blocking timeout, 0 means block indefinitely
        :returns: The popped value or None if timeout.
        """
        client = self.get_client_by_key(key)
        if client is None:
            return
        return client.block_right_pop(key, timeout)

    def lindex(self, key, index):
        """Return the item from list at position `index`

        :param key: the key of list
        :param index: the position, can be negative
        :returns: The value at position `index` or None of index is out of range
        """
        client = self.get_client_by_key(key)
        if client is None:
            return
        return client.lindex(key, index)

    def llen(self, key):
        """Return the number of elements in list

        :param key: the key of list
        :returns: number of elements in list
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return
        return client.llen(key)

    def lpop(self, key):
        """Pop a value from the head of list

        :param key: the key of list
        :returns: The popped value or None if list is empty
        """
        client = self.get_client_by_key(key)
        if client is None:
            return
        return client.lpop(key)

    def lpush(self, key, value):
        """Push a value to the head of the list

        :param key: the key of list
        :param value: the value to be pushed
        :returns: Whether the value has been added to list
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.lpush(key, value)

    def lrange(self, key, start=0, end=-1):
        """Return a slice of the list

        :param key: the key of list
        :param start: the start position, can be negative
        :param end: the end position, can be negative
        :returns: The values between `start` and `end`
        :rtype: list
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.lrange(key, start, end)

    def ltrim(self, key, start, end):
        """Trim the list, removing all values not within the slice

        :param key: the key of list
        :param start: the start postion, can be negative
        :param end: the end postion, can be negative
        :returns: whether the list has been trimmed
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.ltrim(key, start, end)

    def rpop(self, key):
        """Pop a value from the tail of list

        :param key: the key of list
        :returns: the popped value or None if list is empty
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.rpop(key)

    def rpush(self, key, value):
        """Push a value to the tail of the list

        :param key: the key of list
        :param value: the value to be pushed
        :returns: whether the value has been added to list
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.rpush(key, value)

    def sadd(self, key, value):
        """Add a value to the set

        :param key: the key of set to add
        :param value: the value to be added
        :returns: whether the value has been added to set
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.sadd(key, value)

    def sadd_many(self, key, *values):
        """Add multiple values to the set

        :param key: the key of set to add
        :param values: the values to be added
        :returns: whether the values has been added to set
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.sadd_many(key, values)

    def scard(self, key):
        """Return the number of elements in set

        :param key: the key of set
        :returns: number of elements in set
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.scard(key)

    def sismember(self, key, value):
        """Return whether the value is a member of set

        :param key: the key of set
        :param value: the value to be checked
        :returns: whether the `value` is a member of a set
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.sismember(key, value)

    def smembers(self, key):
        """Return all the members of set

        :param key: the key of set
        :returns: all the members value of set
        :rtype: set
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.smembers(key)

    def srandmember(self, key):
        """Randomly return a member of set

        :param key: the key of set
        :returns: random member or None if empty
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.srandmember(key)

    def srem(self, key, value):
        """Remove value from set

        :param key: the key of set
        :param value: the value to be removed
        :returns: whether the `value` has been removed from set
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.srem(key, value)

    def hgetall(self, key):
        """Look up hash in the cache and return all fields and values for it

        :param key: the key of hash to be looked up
        :returns: The dict value of hash, if empty, return {}
        :rtype: dict
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.hgetall(key)

    def hget(self, key, field):
        """Look up field in the hash and return the value of it.

        :param key: the key of hash to be lookup
        :param field: the field in the hash to be lookup
        :returns: the value if exists else ``None``
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
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
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.hget(key, field, value, timeout, noreply)

    def hdel(self, key, field, noreply=False):
        """Delete field of hash from the cache

        :param key: the key of hash to delete
        :param field: the field in the hash to delete
        :param noreply: instructs the server not reply
        :returns: whether the key has been deleted
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.hdel(key, field, noreply)

    def hexists(self, key, field):
        """Check whether `field` is an existing field in the hash

        :param key: the key of hash
        :param field: the field to be checked
        :returns: whether `field` is an existing field in the hash
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.hexists(key, field)

    def hlen(self, key):
        """Get number of fields contained in the hash

        :param key: the key of hash
        :returns: the number of fields contained in the hash
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.hlen(key)

    def zadd(self, key, value, score):
        """Add value to sorted set.

        :param key: the key of sorted set
        :param value: the value to be added
        :param score: the score of the value
        :returns: Whether the value has been set
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.zadd(key, value, score)

    def zcard(self, key):
        """Return the number of values in sorted set

        :param key: the key of sorted set
        :returns: the number of values in sorted set
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.zcard(key)

    def zcount(self, key, min, max):
        """Returns the number of values in the sorted set `key` with a score between `min` and `max`

        :param key: the key of sorted set
        :param min: min score
        :param max: max score
        :returns: the number of values
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.zcount(key, min, max)

    def zincrby(self, key, value, delta=1):
        """Increment the score of `value` in sorted set `key` by `delta`

        :param key: the key of sorted set
        :param value: the value to be incremented
        :param delta: increment amount
        :returns: the new score
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.zincrby(key, value, delta)

    def zrange(self, key, start=0, end=-1, reverse=False, withscores=False):
        """Return a range of values from sorted set `key` between `start` and `end`

        :param key: the key of sorted set
        :param start: start index
        :param end: end index
        :param reverse: sorted in descending order
        :returns: if withscores is True, return a list of (value, score) pairs, otherwise return a list of values
        :rtype: list
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
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
        client = self.get_client_by_key(key)
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
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.zrank(key, value, reverse)

    def zrem(self, key, value):
        """Remove the `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be removed
        :returns: whether the `value` has been removed
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return False
        return client.zrem(key, value)

    def zremrangebyrank(self, key, start, end, reverse=False):
        """Remove the values in sorted set with scores between `start` and `end`

        :param key: the key of sorted set
        :param start: start index
        :param end: end index
        :param reverse: sorted in descending order
        :returns: the number of values removed
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.zremrangebyrank(key, start, end, reverse)

    def zremrangebyscore(self, key, min, max):
        """Remove the values in sorted set with scores between `min` and `max`

        :param key: the key of sorted set
        :param min: min score
        :param max: max score
        :returns: the number of values removed
        :rtype: int
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.zremrangebyrank(key, min, max)

    def zscore(self, key, value):
        """Return the score of `value` in sorted set

        :param key: the key of sorted set
        :param value: the value to be checked
        :returns: the score of `value` in sorted set `key` or None if not existed
        :rtype: float
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.zscore(key, value)

    def zunionstore(self, dest, keys, aggregate=None):
        """Union multiple sorted sets into a new sorted set

        :param dest: destination sorted set
        :param keys: keys of sorted sets to be aggregated
        :param aggregate: specify how the results of the union are aggregated, defaults to SUM
        :returns: the number of elements in the resulting sorted set at destination
        :rtype: int
        """
        raise NotImplementedError()

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will automatically be deleted.

        :param key: key to set timeout
        :param timeout: timeout value in seconds
        :returns: True if timeout was set, False if key does not exist or timeout could not be set
        :rtype: boolean
        """
        client = self.get_client_by_key(key)
        if client is None:
            return None
        return client.expire(key, timeout)
