import six
import sys
import platform

_PY2 = sys.version_info[0] == 2

if _PY2:
    _iteritems = lambda d: d.iteritems()

    def _to_native(x, charset='utf-8', errors='strict'):
        if x is None or isinstance(x, str):
            return x
        return x.encode(charset, errors)

    _to_binary = _to_native  # _to_native on Python 2 will return binary type equivalent

else:
    _iteritems = lambda d: iter(d.items())

    def _to_native(x, charset='utf-8', errors='strict'):
        if x is None or isinstance(x, str):
            return x
        return x.decode(charset, errors)

    def _to_binary(x, charset='utf-8', errors='strict'):
        if x is None or isinstance(x, six.binary_type):
            return x
        return x.encode(charset, errors)


_numeric_types = six.integer_types + (float,)

_DEFAULT_SOCKET_TIMEOUT = 3
_DEFAULT_TIMEOUT = 60


if platform.system().lower() == 'linux':
    import socket
    _TCP_KEEP_ALIVE_OPTIONS = {
        socket.TCP_KEEPIDLE: 30,
        socket.TCP_KEEPINTVL: 5,
        socket.TCP_KEEPCNT: 5,
    }
else:
    _TCP_KEEP_ALIVE_OPTIONS = {}
