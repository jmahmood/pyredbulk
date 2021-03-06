pyredbulk
==========

In Python, generate a bulk insertion file for adding large amounts of data to Redis.

Version: *0.4*
Implements: _hmset_, _hset_, _sadd_

Installation
============
pip install pyredbulk

How?
====

sadd
----
    from pyredbulk import sadd

    girlfriends = [("John", "Jill", "Janet"), ("Billy", "Haley", "Janet")]

    with sadd("/tmp/test.txt") as redis_insert:
        redis_insert(girlfriends)

    # From prompt
    # cat /tmp/test.txt | redis-cli --pipe

- Currently set to deal with data in tuples, but that is likely to change to sets in the future.


hset
-----
    from pyredbulk import hset

    # HSET myhash field1 "Hello"

    with hset("/tmp/test.txt") as redis_insert:
        redis_insert("myhash", "field1", "Hello")

    # From prompt
    # cat /tmp/test.txt | redis-cli --pipe


hmset
-----
    from pyredbulk import hmset

    hashname_fn = lambda d: d.get("name")
    dicts = [{"name": "canada", "capital": "ottawa", "population": 20000000},
             {"name": "france", "capital": "paris", "population": 50000000},
             {"name": "usa", "capital": "washington", "population": 300000000},
             {"name": "ジャパン", "capital": "とうきょう", "population": 180000000}]

    with hmset("/tmp/test.txt") as redis_insert:
      redis_insert(hashname_fn, dicts)

    # From prompt
    # cat /tmp/test.txt | redis-cli --pipe

- If your data is in tuples, you can try creating a new class by overriding the "hmset" class; the append files will try to take care of making sure data is in UTF8 when it is being passed to the text file.

- If you are using a generator for the dicts array, this should ideally take very little memory, as the actual dict itself will only contain the information for that line, and it will be then saved immediately to the database.

- The constructor argument of hmset can be a path to write to a file, or omitted to write to stdout, or any kind of stream object to write to.

- If you are having problems with the Japanese insertion above, try running it in iPython or creating a script to run it; the regular Python repl sometimes gives me problems with Japanese input.

Why?
----

I love the Python "with" command.  No need to clutter your code with scaffolding, especially when you can hide it in an __enter__ and __exit__ method.  I also love Python dicts; they are easy to use, and you can pretty much import any kind of data as a dict.

You should be able to use this without having Redis on the server running the command.

The main purpose for this is to use with very large datasets that need to be inserted onto your redis server.  I have to move large amounts of data from MySQL to an in-memory Redis DB, for which I wrote this tool.  I hope you find it helpful.

Special Thanks
--------------
Thanks Kal and Janos who were kind enough to look at my code so I didn't look like an idiot when releasing it.

License
=======

Copyright (c) 2013 Jawaad Mahmood

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

