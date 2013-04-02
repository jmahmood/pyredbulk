# encoding=utf8
import math
from pyredbulk import hmset

hashname_fn = lambda d: d.get("name")
math_hashname_fn = lambda d: d.get("rad")
dicts = [{"name": "canada", "capital": "ottawa", "population": 20000000},
         {"name": "france", "capital": "paris", "population": 50000000},
         {"name": "usa", "capital": "washington", "population": 300000000},
         {"name": "ジャパン", "capital": "とうきょう", "population": 180000000}]

def math_generator_fn():
    i = 0.1
    while i < math.pi:
        yield {"sin": math.sin(i), "cos": math.cos(i), "rad": i}
        i += 0.1

# constructor arg can be a file path to write to
with hmset("/tmp/test.txt") as redis_insert:
    redis_insert(hashname_fn, dicts)

# constructor arg can be a stream to append to
with hmset(open("/tmp/math_test.txt", "a")) as redis_insert:
    redis_insert(math_hashname_fn, math_generator_fn())

# constructor arg can be omitted to write to stdout
with hmset() as redis_insert:
    redis_insert(hashname_fn, dicts)
