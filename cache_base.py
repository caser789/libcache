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

    ########################################
    # String
    ########################################

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

    ########################################
    # List
    # 
    # block_left_pop
    # block_right_pop
    # lindex
    # llen
    # lpop
    # lpush
    # lrange
    # ltrim
    # rpop
    ########################################
	def block_left_pop(self, key, timeout=0):
		"""Blocking pop a value from the head of the list.

		:param key: the key of list
		:param timeout: blocking timeout, 0 means block indefinitely
		:returns: The popped value or None if timeout.
		"""
		raise NotImplementedError()

	def block_right_pop(self, key, timeout=0):
		"""Blocking pop a value from the tail of the list.

		:param key: the key of list
		:param timeout: blocking timeout, 0 means block indefinitely
		:returns: The popped value or None if timeout.
		"""
		raise NotImplementedError()

        def lindex(self, key, index):
            """Return the item from list at position `index`

            :param key: the key of list
            :param index: the position, can be negative
            :returns: The value at position `index` or None of index is out of range
            """
            raise NotImplementedError()

        def llen(self, key):
            """Return the number of elements in list

            :param key: the key of list
            :returns: number of elements in list
            :rtype: int
            """
            raise NotImplementedError()

        def lpop(self, key):
            """Pop a value from the head of list

            :param key: the key of list
            :returns: The popped value or None if list is empty
            """
            raise NotImplementedError()

        def lpush(self, key, value):
            """Push a value to the head of the list

            :param key: the key of list
            :param value: the value to be pushed
            :returns: Whether the value has been added to list
            :rtype: boolean
            """
            raise NotImplementedError()

        def lrange(self, key, start=0, end=-1):
            """Return a slice of the list

            :param key: the key of list
            :param start: the start position, can be negative
            :param end: the end position, can be negative
            :returns: The values between `start` and `end`
            :rtype: list
            """
            raise NotImplementedError()

        def ltrim(self, key, start, end):
            """Trim the list, removing all values not within the slice

            :param key: the key of list
            :param start: the start postion, can be negative
            :param end: the end postion, can be negative
            :returns: whether the list has been trimmed
            :rtype: boolean
            """
            raise NotImplementedError()

        def rpop(self, key):
            """Pop a value from the tail of list

            :param key: the key of list
            :returns: the popped value or None if list is empty
            """
            raise NotImplementedError()

        def rpush(self, key, value):
            """Push a value to the tail of the list

            :param key: the key of list
            :param value: the value to be pushed
            :returns: whether the value has been added to list
            :rtype: boolean
            """
            raise NotImplementedError()


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
        raise NotImplementedError()

    def hget(self, key, field):
        """Look up field in the hash and return the value of it.

        :param key: the key of hash to be lookup
        :param field: the field in the hash to be lookup
        :returns: the value if exists else ``None``
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

    def hdel(self, key, field, noreply=False):
        """Delete field of hash from the cache

        :param key: the key of hash to delete
        :param field: the field in the hash to delete
        :param noreply: instructs the server not reply
        :returns: whether the key has been deleted
        :rtype: boolean
        """
        raise NotImplementedError()

    def hexists(self, key, field):
        """Check whether `field` is an existing field in the hash

        :param key: the key of hash
        :param field: the field to be checked
        :returns: whether `field` is an existing field in the hash
        :rtype: boolean
        """
        return self.hget(key, field) is not None

    def hlen(self, key):
        """Get number of fields contained in the hash

        :param key: the key of hash
        :returns: the number of fields contained in the hash
        :rtype: int
        """
        return len(self.hgetall(key))


    ########################################
    # Set
    ########################################

    def sadd(self, key, value):
        """Add a value to the set

        :param key: the key of set to add
        :param value: the value to be added
        :returns: whether the value has been added to set
        :rtype: boolean
        """
        raise NotImplementedError()

    def sadd_many(self, key, *values):
        """Add multiple values to the set

        :param key: the key of set to add
        :param values: the values to be added
        :returns: whether the values has been added to set
        :rtype: boolean
        """
        raise NotImplementedError()

    def scard(self, key):
        """Return the number of elements in set

        :param key: the key of set
        :returns: number of elements in set
        :rtype: int
        """
        raise NotImplementedError()

    def sismember(self, key, value):
        """Return whether the value is a member of set

        :param key: the key of set
        :param value: the value to be checked
        :returns: whether the `value` is a member of a set
        :rtype: boolean
        """
        raise NotImplementedError()

    def smembers(self, key):
        """Return all the members of set

        :param key: the key of set
        :returns: all the members value of set
        :rtype: set
        """
        raise NotImplementedError()

    def srandmember(self, key):
        """Randomly return a member of set

        :param key: the key of set
        :returns: random member or None if empty
        """
        raise NotImplementedError()

    def srem(self, key, value):
        """Remove value from set

        :param key: the key of set
        :param value: the value to be removed
        :returns: whether the `value` has been removed from set
        :rtype: boolean
        """
        raise NotImplementedError()


    ########################################
    # Zset
    ########################################

    ########################################
    # Other
    ########################################

    def clear(self):
        """Clears the cache. Not all caches support completely clearing the cache

        :returns: Whether the cache been cleared.
        :rtype: boolean
        """
        return True

    @property
    def raw_client(self):
        """Get raw cache server client.

        :returns: underlying cache client object.
        """
        return self._client

    def expire(self, key, timeout):
        """Set a timeout on key. After the timeout has expired, the key will automatically be deleted.

        :param key: key to set timeout
        :param timeout: timeout value in seconds
        :returns: True if timeout was set, False if key does not exist or timeout could not be set
        :rtype: boolean
        """
        raise NotImplementedError()
