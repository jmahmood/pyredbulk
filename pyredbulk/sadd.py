from pyredbulk.base import RedisProtocol

__author__ = 'jawaad'


class Sadd(RedisProtocol):
    """
    from pyredbulk import sadd

    girlfriends = [("John", "Jill", "Janet"), ("Billy", "Haley", "Janet")]

    with sadd("/tmp/test.txt") as redis_insert:
        redis_insert(girlfriends)
    #
    """
    def __call__(self, tuples, *args, **kwargs):
        def output(t):
            # The first element of a tuple is the key.
            if not self.validate(t):
                raise IOError("SADD ABORT: Failure while validating tuple: %s\n%s" % (repr(t)))

            # The length 1 (the key/first item) and the rest of the items,
            # plus the name of the command (SADD)
            self.setup_output(len(t) + 1)
            self.write("SADD")
            for i in t:
                self.write(i)

        if isinstance(tuples, tuple):
            output(tuples)

        elif not hasattr(tuples, "strip") and hasattr(tuples, "__getitem__") or hasattr(tuples, "__iter__"):
            map(output, tuples)

        else:
            raise IOError("You need to pass an /iterable/ or a /tuple/ as the object you want to save.")
