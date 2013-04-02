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


class redis_protocol(object):
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.file = open(self.filename, "w")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def validate(self, d):
        return True

    def __call__(self, d):
        pass

    def setup_output(self, arg_len):
        self.output = ["*%d" % arg_len]

    def write(self):
        self.file.write("\r\n".join(self.output))
        self.file.write("\r\n")

    def append(self, val):
        try:
            val = val.decode("utf8")
        except AttributeError:
            val = str(val)

        val = val.encode("utf8")

        self.output.append("$%d" % len(val))
        self.output.append(val)
