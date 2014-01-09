# encoding=utf8
#
# This class is meant to handle large-scale insertion of dicts into a text file in the Redis protocol.
#
# You can add data validation by subclassing the appropriate file and modifying the validate call.  Invalid fields
# are logged and not added to the text output.
#
# Only requires the logging function.

__author__ = 'jmahmood'
import logging
from .base import RedisProtocol


class Hset(RedisProtocol):
    """
    from pyredbulk import hset
    # HSET myhash field1 "Hello"

    with hset("/tmp/test.txt") as redis_insert:
        redis_insert(myhash, field1, "Hello")
    #
    """

    def __call__(self, hash_name, field, value, *args, **kwargs):
        def output():
            # arg_len = COMMAND + HASH_NAME + FIELD + VALUE)
            self.setup_output(4)
            self.write("HMSET")
            self.write(hash_name)
            self.write(field)
            self.write(value)

        output()
