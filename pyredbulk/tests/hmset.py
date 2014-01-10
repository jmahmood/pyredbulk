import unittest
from pyredbulk import hmset
import subprocess
# Redis is only needed for the test suite.
import redis

__author__ = 'jawaad'


class TestHmSet(unittest.TestCase):
    """Tests related to Redis hmset pipeline insert.
    You must have redis-server running on localhost."""

    def setUp(self):
        self.conn = redis.StrictRedis("localhost")

    def test_valid_output(self):
        """This tests the exact same functionality that Evan says is working on his end."""
        data = {"jbm": 1, "rbm": 2, "sbm": 3}
        fn = lambda a: "fakekey"

        with hmset("/tmp/lol") as redis_insert:
            redis_insert(fn, data)

        f = open("/tmp/lol")

        x = subprocess.call(
            ["redis-cli",  "--pipe"], stdin=f
        )

        f.close()
        self.assertTrue(x == 0)
        self.assertTrue(self.conn.hexists("fakekey", "jbm"))

        self.assertTrue("jbm" in self.conn.hkeys("fakekey"))
        self.assertTrue("rbm" in self.conn.hkeys("fakekey"))
        self.assertTrue("sbm" in self.conn.hkeys("fakekey"))

        self.assertTrue(int(self.conn.hget("fakekey", "jbm")) == 1)
        self.assertTrue(int(self.conn.hget("fakekey", "rbm")) == 2)
        self.assertTrue(int(self.conn.hget("fakekey", "sbm")) == 3)

