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
        redis_insert(hashname_fn, dicts)"""

    def __call__(self, hash_name_fn, dicts, *args, **kwargs):
        if not hasattr(hash_name_fn, '__call__'):
            raise IOError("You must pass a function that will generate your python dict's Redis hashname")

        for input_dict in dicts:
            if not self.validate(input_dict):
                logging.error(input_dict)
                raise IOError("Failure while validating dictionary: %s" % hash_name_fn(input_dict))

            #arg_len = COMMAND + HASHNAME + 2 * key_len (a key has a key + val, thus 2 entries)
            self.setup_output(2 * len(input_dict) + 2)
            self.write("HMSET")
            self.write(hash_name_fn(input_dict))
            for k, v in input_dict.iteritems():
                self.write(k)
                self.write(v)
