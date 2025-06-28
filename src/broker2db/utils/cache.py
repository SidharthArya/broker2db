"""
Caching helper for broker2db
"""
import functools
from typing import List, Dict
import hashlib

from aiocache import cached as aiocached
from aiocache.backends.redis import RedisBackend


class BDRedisBackend(RedisBackend):
    """
    Custom Redis Backend to support authentication
    """
    def __init__(self, **kwargs):
        self.password = None
        self.endpoint = None
        super().__init__(endpoint=self.endpoint, password = self.password, **kwargs)

def serialize_item(item) -> str:
    """
    Key Serialization
    """
    if isinstance(item, List):
        return "L" + ','.join([serialize_item(i) for i in item])
    if isinstance(item, Dict):
        return "D" +\
            ','.join([f"{serialize_item(k)}:{serialize_item(v)}" for k, v in item.items()])
    return str(item)


def key_builder(*args, **kwargs):
    """
    Custom key builder
    """
    res = args[0].__name__
    res += 'A;'.join([serialize_item(a) for a in args])
    res += 'K;'.join([f"{serialize_item(k)}:{serialize_item(v)}" for k, v in kwargs.items()])
    return f"-{hashlib.md5(res.encode('utf-8')).hexdigest()}"

cached = functools.partial(aiocached, key_builder=key_builder, cache=BDRedisBackend)
