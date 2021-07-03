# A java like stream for python

## Installing
```
$ pip3 install pystream-wushilin
```

## Importing
```python
from pystream import pystream

```

## Using
### Creating: stream can be created from iterable, or iterator
```python
# Create stream from iterable (e.g. collections)
stream = pystream.Stream(range(0, 100))
stream = pystream.Stream([1,2,3,4,5,6])
dict1 = {'k1': 'v1', 'k2': 'v2'}

key_stream = Stream(dict1.keys())
value_stream.map(lambda k: dict1[k])

# Create stream from iterator!
string = "hello, world"
iterator = iter(string)
stream = pystream.Stream(iterator)

# Create stream from a file:
with pystream.Stream.from_file_lines("example.txt") as stream:
   # use stream

# Creating from generator func
s1 = pystream.Stream.generate(lambda:5) # infinite number of 5s, if you count, it hangs!
s1.limit(1000).sum() # should be 5000

a = 1
b = 1
def fib():
	global a
	global b
  a, b = b, a+b
	return a

pystream.Stream.generate(fib).limit(10).for_each(print)
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



```

### Using stream
```python
# Mapping
pystream.Stream([1,2,3]).map(lambda x: x+1).for_each(print)
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
pystream.Stream(range(0, 55)).filter(lambda x: x>50).for_each(print)
51
52
53
54

# Limiting
pystream.Stream(range(0, 1000000)).limit(5).for_each(print)
0
1
2
3
4

# Skipping
pystream.Stream(range(0, 100)).skip(95).for_each(print)
95
96
97
98
99

# Summing
pystream.Stream(range(0,5)).sum() # 10 (0 + 1 + 2 + 3 + 4)

# Max/Min
pystream.Stream(range(0, 5)).max() # 4
pystream.Stream(range(0, 5)).min() # 0

# Reducing
pystream.Stream(range(0, 5)).reduce(lambda x, y: x + y) # 10 -> same as sum!

# Reading from file from_file_lines
with pystream.Stream.from_file_lines("readme.txt").with_index() as stream:
  stream.for_each(print)

(0, <line1>)
(1, <line2>)
(2, <line3>) ...

# With index
pystream.Stream([1,3,5,7,9]).with_index().for_each(print)
(0, 1)
(1, 3)
(2, 5)
(3, 7)
(4, 9)

# Counting
pystream.Stream(range(0, 100)).count() # 100 (0...99)

# Concating stream
s1 = pystream.Stream([1,2,3])
s2 = pystream.Stream([4,5,6])
(s1 + s2).count() # 6
s1.concat(s2).count() #6
# Note: if you do both of above, second line will be 0 since first one consumed s1 and s2 already.

# visiting with a func
pystream.Stream([1,2,3,4,5]).for_each(print)
1
2
3
4
5


# convert to list
list1 = pystream.Stream(range(0, 5)).to_list() # [0, 1, 2, 3, 4]
list2 = list(stream) # [0,1,2,3,4] since the stream itself is iterable

# picking from tuple for each element
stream = pystream.Stream(range(0, 10, 2)) # 0, 2, 4, 6, 8
indexed_stream = stream.with_index() # (0, 0), (1, 2), (2, 4), (3, 6), (4, 8)
indexed_stream.pick(0) # 0, 1, 2, 3, 4
indexed_stream.pick(1) # 0, 2, 4, 6, 8
indexed_stream.pick(3) # Index Out of Bound error

# Reducing
pystream.Stream(range(0, 5)).reduce(lambda x, y: x * y) # 0 (0 * 1 * 2 * 3 * 4) 


```
