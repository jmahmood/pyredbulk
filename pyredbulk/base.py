# encoding=utf8
#
# You can add data validation by subclassing the appropriate file and modifying the validate call.  Invalid fields
# are logged and not added to the text output.
#
# Only requires the logging function.
__author__ = 'jmahmood'
import sys

# Redis requires both CR and LF, regardless of the OS, for its protocol.
REDIS_PROTOCOL_LINEENDING = "\r\n"

class RedisProtocol:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        if self.filename:
            self.file = open(self.filename, "w")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.filename:
            self.file.close()

    def __call__(self, d):
        pass

    def validate(self, d):
        return True

    def output(self, v):
        if self.filename:
            self.file.write(v)
        else:
            sys.stdout.write(v)

    def setup_output(self, arg_len):
        self.output("*%d%s" % (arg_len, REDIS_PROTOCOL_LINEENDING))

    def write(self, val):
        try:
            val.decode("utf8")
        except AttributeError:
            val = str(val)

        self.output("""$%d%s%s%s""" % (len(val), REDIS_PROTOCOL_LINEENDING, val, REDIS_PROTOCOL_LINEENDING))
