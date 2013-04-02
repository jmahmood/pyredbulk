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
    hashname_fn = lambda d: d.get("name")
    dicts = [{"name": "canada", "capital": "ottawa", "population": 20000000},
            {"name": "france", "capital": "paris", "population": 50000000},
            {"name": "usa", "capital": "washington", "population": 300000000},
            {"name": "ジャパン", "capital": "とうきょう", "population": 180000000}
    ]

    with hmset("/tmp/test.txt") as redis_insert:
        redis_insert(hashname_fn, dicts)

    """

    def __call__(self, *args, **kwargs):
        if kwargs.get("fn"):
            hash_name_fn = kwargs["fn"]
        else:
            hash_name_fn = args[0]

        if not hasattr(hash_name_fn, '__call__'):
            raise IOError("You must pass a function that will generate your python dict's Redis hashname")

        if kwargs.get("dicts"):
            dicts = kwargs["dicts"]
        else:
            dicts = args[1]

        for d in dicts:
            if not self.validate(d):
                logging.error(d)
                raise IOError("Failure while validating dictionary: %s" % hash_name_fn(d))

            #arg_len = COMMAND + HASHNAME + 2 * key_len (a key has a key + val, thus 2 entries)
            self.setup_output(2 * len(d) + 2)
            self.write("HMSET")
            self.write(hash_name_fn(d))
            for k, v in d.iteritems():
                self.write(k)
                self.write(v)
