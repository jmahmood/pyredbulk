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


class Hmset(RedisProtocol):
    """
    from pyredbulk import hmset

    hashname_fn = lambda d: d.get("name")
    dicts = [{"name": "canada", "capital": "ottawa", "population": 20000000},
            {"name": "france", "capital": "paris", "population": 50000000},
            {"name": "usa", "capital": "washington", "population": 300000000},
            {"name": "ジャパン", "capital": "とうきょう", "population": 180000000}]

    with hmset("/tmp/test.txt") as redis_insert:
        redis_insert(hashname_fn, dicts)
    #
    """

    def __call__(self, hash_name_fn, dicts, *args, **kwargs):
        def output(d):
            # arg_len = COMMAND + HASH_NAME + 2 * key_len (a key has a key + val, thus 2 entries)
            if not self.validate(d):
                raise IOError("HMSET ABORT: Failure while validating dictionary: %s\n%s" % (hash_name_fn(d), repr(d)))

            self.setup_output(2 * len(d) + 2)
            self.write("HMSET")
            self.write(hash_name_fn(d))
            for k, v in d.iteritems():
                self.write(k)
                self.write(v)

        if not hasattr(hash_name_fn, '__call__'):
            raise IOError("You must pass a function that will generate your python dict's Redis hashname")

        if isinstance(dicts, dict):
            output(dicts)

        elif not hasattr(dicts, "strip") and hasattr(dicts, "__getitem__") or hasattr(dicts, "__iter__"):
            map(output, dicts)

        else:
            raise IOError("You need to pass an /iterable/ or a /dict/ as the object you want to save.")
