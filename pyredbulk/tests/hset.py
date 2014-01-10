import subprocess
from pyredbulk import hset
import unittest
import redis

__author__ = 'jawaad'



class TestHSet(unittest.TestCase):
    """Tests related to Redis hmset pipeline insert.
    You must have redis-server running on localhost."""

    def setUp(self):
        self.conn = redis.StrictRedis("localhost")

    def test_valid_input(self):
        """Does valid base input method work?"""

        with hset("/tmp/hset_lol") as redis_insert:
            redis_insert("hset_test_1", "field1", "Hello")

        f = open("/tmp/hset_lol")

        x = subprocess.call(
            ["redis-cli",  "--pipe"], stdin=f
        )

        f.close()
        self.assertTrue(x == 0)
        self.assertTrue(self.conn.hexists("hset_test_1", "field1"))
        self.assertTrue(self.conn.hget("hset_test_1", "field1") == "Hello")

        f1 = open("/tmp/hset_lol_1", "w")

        with hset(f1) as redis_insert:
            redis_insert("hset_test_1", "field1", "Hello")

        f2 = open("/tmp/hset_lol_1")

        x = subprocess.call(
            ["redis-cli",  "--pipe"], stdin=f2
        )

        f.close()
        self.assertTrue(x == 0)
        self.assertTrue(self.conn.hexists("hset_test_1", "field1"))
        self.assertTrue(self.conn.hget("hset_test_1", "field1") == "Hello")


    def test_valid_tuple_insert(self):
        """Does tuple insertion work?"""

        with hset("/tmp/hset_lol2") as redis_insert:
            redis_insert(tuples=("hset_test_2", "field2", "Ohayou"))

        f = open("/tmp/hset_lol2")

        x = subprocess.call(
            ["redis-cli",  "--pipe"], stdin=f
        )
        f.close()
        self.assertTrue(x == 0)
        self.assertTrue(self.conn.hexists("hset_test_2", "field2"))
        self.assertTrue(self.conn.hget("hset_test_2", "field2") == "Ohayou")

    def test_valid_tuple_list_insert(self):
        """Does tuple list insertion work?"""

        with hset("/tmp/hset_lol3") as redis_insert:
            redis_insert(tuples=[("hset_test_3", "field3", "Hello"),
                                 ("hset_test_3", "field4", "different value")])

        f = open("/tmp/hset_lol3")

        x = subprocess.call(
            ["redis-cli",  "--pipe"], stdin=f
        )
        f.close()
        self.assertTrue(x == 0)
        self.assertTrue(self.conn.hexists("hset_test_3", "field3"))
        self.assertTrue(self.conn.hget("hset_test_3", "field3") == "Hello")

        self.assertTrue(x == 0)
        self.assertTrue(self.conn.hexists("hset_test_3", "field4"))
        self.assertTrue(self.conn.hget("hset_test_3", "field4") == "different value")


    def test_invalid_indeterminate_insert(self):
        """You shouldn't modify the same field of the same hash twice in one set of instructions.
        Only the last one will be applied but it is still "wrong".  (You can't be sure whether or not
         Redis will use multiple threads or process or goworkers or whatever)"""

        test_file = "/tmp/hset_4"
        with hset(test_file) as redis_insert:
            self.assertRaises(IOError, redis_insert, **{"tuples":
                                                               [("hset_test_3", "field3", "Hello"),
                                                                ("hset_test_3", "field3", "different value")]})

    def test_tuples_and_regular_insert(self):
        """If you are inserting as tuples, you insert everything as tuples.  If you are inserting manually,
        do it all manually."""
        test_file = "/tmp/hset_5"
        with hset(test_file) as redis_insert:
            self.assertRaises(IOError, redis_insert,
                              hash_name="hset_test_5",
                              field="field1",
                              value="Value 1",
                              tuples=("hset_test_5", "field2", "Value 2"))

    def test_tuple_list_and_regular_insert(self):
        """If you are inserting as a tuple list,  you insert in the tuple list.
        If you are inserting with the hash_names field, do it all that way."""

        test_file = "/tmp/hset_6"
        with hset(test_file) as redis_insert:
            self.assertRaises(IOError, redis_insert, **{"hash_name": "hset_test_6",
                                                           "field": "field",
                                                           "value": "Value",
                                                           "tuples":
                                                               [("hset_test_6", "field1", "Value 1"),
                                                                ("hset_test_6", "field2", "Value 2value")]})

    def test_invalid_tuple_insert(self):
        """You shouldn't modify the same field of the same hash twice in one set of instructions.
        Only the last one will be applied but it is still "wrong".  (You can't be sure whether or not
         Redis will use multiple threads or process or goworkers or whatever)"""

        test_file = "/tmp/hset_4"
        with hset(test_file) as redis_insert:
            self.assertRaises(IOError, redis_insert, tuples="LOL whoops")
            self.assertRaises(IOError, redis_insert, tuples=(False, "fieldsomething", "value"))
            self.assertRaises(IOError, redis_insert, tuples=("hset_test_7", False, "value"))
            self.assertRaises(IOError, redis_insert, tuples=("hset_test_7", "field1"))
            self.assertRaises(IOError, redis_insert, tuples=[("hset_test_7", "field2", "Value 1"),
                                                             ("hset_test_8", "field3")])

