# encoding=utf8
#
# You can add data validation by subclassing the appropriate file and modifying the validate call.  Invalid fields
# are logged and not added to the text output.
#
# Only requires the logging function.
__author__ = 'jmahmood'
import sys

# Redis requires both CR and LF, regardless of the OS, for its protocol.
REDIS_PROTOCOL_EOL = "\r\n"


class RedisProtocol:
    """ Constructor arg can be:
        - falsy or omitted: write output to sys.stdout
        - path (string): write output to the specified file
        - stream: write output to the given stream"""
    def __init__(self, ostream=None):
        if ostream:
            if type(ostream) is str:
                self.ostream = open(ostream, "w")
            else:
                self.ostream = ostream
        else:
            self.ostream = sys.stdout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ostream.close()

    def __call__(self, d, *args, **kwargs):
        pass

    def validate(self, d):
        return True

    def output(self, v):
        self.ostream.write(v)

    def setup_output(self, arg_len):
        self.output("*%d%s" % (arg_len, REDIS_PROTOCOL_EOL))

    def write(self, val):
        try:
            val.decode("utf8")
        except AttributeError:
            val = str(val)

        self.output("""$%d%s%s%s""" % (len(val), REDIS_PROTOCOL_EOL, val, REDIS_PROTOCOL_EOL))
