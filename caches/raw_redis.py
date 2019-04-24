import six

from .redis_cache import Redis

_string_types = six.string_types + (six.binary_type,)


class RawRedis(Redis):
    """Same cache client as RedisCache, but only support string value
    """

    def __init__(self, host, port, unix_socket_path=None, password=None, db=0, timeout=None, prefix='', default_scan_count=1000, **kw):
        Redis.__init__(self, host, port, unix_socket_path=None, password=None, db=0,
                timeout=timeout, prefix=prefix, default_scan_count=default_scan_count, **kw)

    def dumps(self, value):
        if not isinstance(value, _string_types):
            raise Exception('raw_redis_unsupport_value_type:' + str(type(value)))

    def loads(self, value):
        return value
