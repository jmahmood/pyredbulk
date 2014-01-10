# encoding=utf8
#
# This class is meant to handle large-scale insertion of dicts into a text file in the Redis protocol.
#
# You can add data validation by subclassing the appropriate file and modifying the validate call.  Invalid fields
# are logged and not added to the text output.
#
# Only requires the logging function.

__author__ = 'jmahmood'
from .base import RedisProtocol


class Hset(RedisProtocol):
    """
    Implements the Redis hset command

    from pyredbulk import hset
    # HSET myhash field1 "Hello"

    with hset("/tmp/test.txt") as redis_insert:
        redis_insert("myhash", "field1", "Hello")
    or

    with hset("/tmp/test.txt") as redis_insert:
        redis_insert(tuples=("myhash", "field1", "Hello"))

    or

    with hset("/tmp/test.txt") as redis_insert:
        redis_insert(tuples=[("myhash", "field1", "Hello"), ("anotherhash", "field1", "different value"), ])

    or

    with hset("/tmp/test.txt") as redis_insert:
        redis_insert(tuples=generator_fn())

    """

    def __call__(self, hash_name=False, field=False, value=False, *args, **kwargs):
        def output(tup):
            # arg_len = COMMAND + HASH_NAME + FIELD + VALUE)
            self.setup_output(4)
            self.write("HMSET")
            self.write(tup[0])
            self.write(tup[1])
            self.write(tup[2])
        t = kwargs.get("tuples")

        if t and hash_name:
            raise IOError("Invalid entry.  Please either submit a tuple, or a hash_name/field/value trio.")

        if t:
            self.validate(tuples=t)
            if isinstance(t, tuple):
                output(t)
            elif not hasattr(t, "strip") and hasattr(t, "__getitem__") or hasattr(t, "__iter__"):
                map(output, t)
            else:
                raise IOError("You must pass a tuple or an iterable of tuples as the tuples keyword argument.")

        else:
            self.validate(hash_name=hash_name, field=field, value=value)
            output((hash_name, field, value))

    def validate(self, **kwargs):
        if "tuples" in kwargs:
            if isinstance(kwargs["tuples"], tuple):
                if len(kwargs["tuples"]) < 3:
                    raise IOError("You passed a tuple with less than the three minimum fields necessary."
                                  "ex. (hash_name, field, value)."
                                  "You passed %s" % str(tuple))

                if kwargs["tuples"][0] is False:
                    raise IOError("You must pass a valid value for the hash name."
                                  "You passed %s" % str(tuple))

                if kwargs["tuples"][1] is False:
                    raise IOError("You must pass a valid value for the field."
                                  "You passed %s" % str(tuple))

            elif not hasattr(kwargs["tuples"], "strip") and  hasattr(kwargs["tuples"], "__getitem__") or hasattr(kwargs["tuples"], "__iter__"):
                check = {}
                for t in kwargs["tuples"]:
                    self.validate(tuples=t)
                    if check.get(t[0]) and check.get(t[0]).get(t[1]):
                        raise IOError("You are using the key twice in this insertion sequence.  This could lead to race conditions when inserting into redis.  Stop it.")
                    else:
                        if check.get(t[0]):
                            check[t[0]][t[1]] = 1
                        else:
                            check[t[0]] = {t[1]:1}


        elif "hash_name" in kwargs:
            t = (kwargs.get("hash_name"), kwargs.get("field"), kwargs.get("value"))
            self.validate(tuples=t)
