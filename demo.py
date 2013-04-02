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

with hmset("/tmp/test.txt") as redis_insert:
    redis_insert(hashname_fn, dicts)

with hmset("/tmp/math_test.txt") as redis_insert:
    redis_insert(math_hashname_fn, math_generator_fn())

with hmset(False) as redis_insert:
    redis_insert(hashname_fn, dicts)
