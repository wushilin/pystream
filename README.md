# A java like stream for python
A stream is like java, everything is lazy, using iterative approach, we don't build large list in memory. 

A stream is like a list with cursor, it can only be consumed once! What if I need it again? build a new stream object

A stream can be easily pipelined. you can call them like a chain. e.g. mapped stream can be further reduced or filtered.

```python
Stream(range(0, 1000)).filter(lambda x: mod(x, 2) == 0).map(lambda x:x//2).count() # You know what this does!
```

It has also parallel processing capability, make parallel programming easy.

Imaging managing many downloads can be done this way
```python
# This can replace most of the use case of parallem processing
Stream(urls).parallel_map(lambda x:download_to_file(x, "/tmp"), thread_count=10)

# or you can reuse thread pool. 
Stream(urls).parallel_map(lambda x:download_to_file(x, "/tmp"), thread_pool=my_thread_pool_executor)
```

## Installing
```
$ pip3 install pystream-wushilin
```

## Importing
```python
from pystream.pystream import Stream
```

## Using
### Creating: stream can be created from iterable, or iterator
```python
# Create stream from iterable (e.g. collections)
stream = Stream(range(0, 100))
stream = Stream([1,2,3,4,5,6])
dict1 = {'k1': 'v1', 'k2': 'v2'}

key_stream = Stream(dict1.keys())
value_stream.map(lambda k: dict1[k])

# Create stream from iterator!
string = "hello, world"
iterator = iter(string)
stream = Stream(iterator)

# Create stream from a file (read lines as stream):
# You have to use with the "WITH" keyword so stream can be closed
with Stream.from_file_lines("example.txt") as stream:
   # use stream

# Stream can optionally attach a begin function and exit function so with keywords can be done gracefully.
# begin_func is executed before the consumption
# exit_func is executed after stream is out of scope so cleanup can be done.
# This is useful for stream that is from network, or from file, or from database that has something to clean up
with Stream(dbcursor, begin_func=lambda x:print("I am executed before stream is consumed"), exit_func=dbcursor.close) as stream:
  stream.to_list() # or other ways you like

# Creating from generator func, this stream is unbouded. don't reduce on them(count, sum, max, min etc), don't parallel map
# but map, filter, limit etc is fine, since map, filter are lazy.
# if your generator is bounded, raise StopIteration after last element then this stream is unbounded.
s1 = Stream.generate(lambda:5) # infinite number of 5s, if you count, it hangs!
s1.limit(1000).sum() # should be 5000

a = 1
b = 1
def fib():
	global a
	global b
  a, b = b, a+b
  if a > 100000000000:
    raise StopIteration
	return a
# Generating stream from fibonacci sequence, up to 100000000000. 
# Generating is not an infinite loop, it is lazy!
Stream.generate(fib).limit(10).for_each(print)
1
2
3
5
8
13
21
34
55
89
# Quiz: How to generate random strings?


```

### Using stream
```python
# Mapping
Stream([1,2,3]).map(lambda x: x+1).for_each(print)
2
3
4

# Mapping in parallel. Note this consumes the entire stream, and return result in the original order. If it is infinite stream, this will cause out of memory error
    def slow_map(x):
        """ A slow mapping function that takes 2 seconds """
        sleep(2)
        return x * 2

    Stream.generate(lambda:5).limit(10).parallel_map(slow_map).for_each(print) # default using 10 threads
    Stream.generate(lambda:5).limit(10).parallel_map(slow_map, thread_count=20).for_each(print) # using 20 threads to map concurrently
		thread_pool = ThreadPoolExecutor(max_workers=50)
    Stream.generate(lambda:5).limit(10).parallel_map(slow_map, thread_pool=thread_pool).for_each(print) # re-use thread pool

		# All of above calls will take 2 seconds, instead of 20 seconds if executed in map instead of parallel_map
# Filtering
Stream(range(0, 55)).filter(lambda x: x>50).for_each(print)
51
52
53
54

# Limiting
Stream(range(0, 1000000)).limit(5).for_each(print)
0
1
2
3
4

# Skipping
Stream(range(0, 100)).skip(95).for_each(print)
95
96
97
98
99

# Summing
Stream(range(0,5)).sum() # 10 (0 + 1 + 2 + 3 + 4)

# Max/Min
Stream(range(0, 5)).max() # 4
Stream(range(0, 5)).min() # 0

# Reducing
Stream(range(0, 5)).reduce(lambda x, y: x + y) # 10 -> same as sum!

# Reading from file from_file_lines
with Stream.from_file_lines("readme.txt").with_index() as stream:
  stream.for_each(print)

(0, <line1>)
(1, <line2>)
(2, <line3>) ...

# With index
Stream([1,3,5,7,9]).with_index().for_each(print)
(0, 1)
(1, 3)
(2, 5)
(3, 7)
(4, 9)

# Counting
Stream(range(0, 100)).count() # 100 (0...99)

# Concating stream
s1 = Stream([1,2,3])
s2 = Stream([4,5,6])
(s1 + s2).count() # 6
s1.concat(s2).count() #6
# Note: if you do both of above, second line will be 0 since first one consumed s1 and s2 already.

# visiting with a func
Stream([1,2,3,4,5]).for_each(print)
1
2
3
4
5


# convert to list
list1 = Stream(range(0, 5)).to_list() # [0, 1, 2, 3, 4]
list2 = list(stream) # [0,1,2,3,4] since the stream itself is iterable

# picking from tuple for each element
stream = Stream(range(0, 10, 2)) # 0, 2, 4, 6, 8
indexed_stream = stream.with_index() # (0, 0), (1, 2), (2, 4), (3, 6), (4, 8)
indexed_stream.pick(0) # 0, 1, 2, 3, 4
indexed_stream.pick(1) # 0, 2, 4, 6, 8
indexed_stream.pick(3) # Index Out of Bound error

# Reducing
Stream(range(0, 5)).reduce(lambda x, y: x * y) # 0 (0 * 1 * 2 * 3 * 4) 

# Flatten
Stream([1,2],[3,4]).flatten() # [1,2,3,4]

# Packing
Stream([1,2,3,4,5,6,7]).pack(2) # [[1,2], [3,4], [5,6], [7, None]]
Stream([1,2,3,4,5,6,7].pack(3).flatten()) # [1,2,3,4,5,6,7,None,None]
# If None not desired, filter them yourself.

# Flap map
# When your mapping returns a list, this call flattens it.
Stream([[2, 5], [3, 3]]).flat_map(lambda x: [x[0] for _ in range(x[1])]).for_each(print) # gives you 5 x 2s and 3 x 3s [2,2,2,2,2,3,3,3]

# Ordering
Stream([4,3,2,1,5]).ordered(reverse=true).for_each(print) # [5,4,3,2,1]

# Uniq
Stream([1,1,2,3,4,4,5]).uniq().for_each(print) # [1,2,3,4,5]

# Repeating
print (Stream([1,2,3,4,5]).repeat(3).to_list()) # repeats by default repeat 2 times. this gives you [1,2,3,4,5,1,2,3,4,5,1,2,3,4,5]

# To Set
Stream([1,2,3,4,5,5,6]).to_set() # {1,2,3,4,5,6}

# To map stream (package of 2, or more!) When package has more than 2 elements, key is first of pack, value is rest of pack.
Stream(["k1", "v1", "k2", "v2"]).pack(2).to_maps() # [{k1:v1}, {k2:v2}]

# To a single map
Stream(["k1", "v1", "k2", "v2"]).pack(2).to_map() # [{k1:v1,k2:v2}]

Stream(["k1", "v1", "k2", "v2"]).to_map() # value error!
Stream(["k1", "v1", "k2", "v2"]).to_maps() # value error!

print(Stream(["k1", "v1", "k2", "v2"]).pack(3).to_map()) # {'k1': ['v1', 'k2'], 'v2': [None, None]}

# Repeating stream
Stream(["I love python"]).repeat(20) #["I love python", .... "I love python"] (20 times)


# Spliting stream

# Note that pystreams can be split, however, first splitted stream is primary, others are secondary.
# Secondary has such behaviors:
# Consuming of secondary depends on consumption from primary, and it blocks for ever until elements of first is consumed.
# Elements are available in stream as soon as primary consumes it.
# begin_func, exit_func are attached to primary only. If you split the file stream, please make sure the first 1 (s1 is closed.)
#
    s1, s2, s3, s4 = Stream([1,2,3,4,5], lambda:print("Begin"), lambda:print("end")).split(4)
    
    def slow_consume(name, stream):
        iter = stream.__iter__()
        while True:
            try:
                i = iter.__next__()
                print(f"{name} => consumed {i}")
            except StopIteration:
                break
            sleep(1)

    def consume_asap(name, stream):
        for i in stream:
            print(f"{name} => consumed {i}")

    with s1:
      with s2:
        with s3:
          with s4:
            t1 = threading.Thread(target=slow_consume, args=("s1", s1))
            t2 = threading.Thread(target=consume_asap, args=("s2", s2))
            t3 = threading.Thread(target=consume_asap, args=("s3", s3))
            t4 = threading.Thread(target=consume_asap, args=("s4", s4))
            Stream([t1, t2, t3, t4]).for_each(lambda x: x.start())
            Stream([t1, t2, t3, t4]).for_each(lambda x: x.join())
```
