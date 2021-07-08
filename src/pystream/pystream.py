from concurrent.futures import ThreadPoolExecutor
from time import sleep
from collections.abc import Sequence 
from threading import Thread, Lock
import queue
import threading


class GeneratorIterator:
    def __init__(self, func):
        self.func = func

    def __next__(self):
        result = self.func()
        return result


class PeekIterator:
    def __init__(self, src, func):
        self.func = func
        self.src = src

    def __next__(self):
        result = self.src.__next__()
        self.func(result)
        return result


class SkipIterator:
    def __init__(self, src, skip=0):
        self.src = src
        self.skip = skip
        self.skip_count = 0

    def __next__(self):
        while self.skip_count < self.skip:
            self.src.__next__()
            self.skip_count += 1

        return self.src.__next__()


class LimitIterator:
    def __init__(self, src, limit=None):
        self.src = src
        self.limit = limit
        self.count = 0

    def __next__(self):
        if self.limit is not None and self.count >= self.limit:
            raise StopIteration

        next_val = self.src.__next__()
        self.count += 1
        return next_val


class MapperIterator:
    def __init__(self, src, mapper):
        self.src = src
        self.mapper = mapper

    def __next__(self):
        next_val = self.src.__next__()
        mapped_val = self.mapper(next_val)
        return mapped_val


class ConcatIterator:
    def __init__(self, src1, src2):
        self.src1 = src1
        self.src2 = src2
        self.src1_over = False

    def __next__(self):
        if not self.src1_over:
            try:
                return self.src1.__next__()
            except StopIteration:
                self.src1_over = True

        return self.src2.__next__()

class MapCollectorIterator:
    def __init__(self, src):
        self.src = src
    
    def _seq_but_not_str(self, obj):
        return isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray))

    def __next__(self):
        dict = {}
        next = self.src.__next__()
        
        if((not self._seq_but_not_str(next)) or len(next) < 2):
            raise ValueError("Expect package of 2 at least")
        
        key = next[0]
        value = next[1]
        if(len(next) > 2):
            value = next[1:]
        dict[key] = value
        return dict

class PackageIterator:
    def __init__(self, src, count=2):
        self.src = src
        self.count = count
        
    def __next__(self):
        result = list(None for _ in range(self.count))
        for i in range(0, self.count):
            try:
                next = self.src.__next__()
                result[i] = next
            except StopIteration:
                if i == 0:
                    raise StopIteration
                else:
                    return result
        return result

class FlattenIterator:
    def __init__(self, src):
        self.src = src
        self.current = None
    
    def to_iterator(self, what):
        """Convert object to iterator, if it is a collection, iterator of it is returned
        if it is already a iterator, returns it"""
        try:
            return iter(what)
        except TypeError:
            if hasattr(what, "__next__"):
                # If itself is an iterator, return itself
                return what
            else:
                # return a solo list of the plain value
                return iter([what])

    def __next__(self):
        if self.current is None:
            self.current = self.to_iterator(self.src.__next__())
            return self.__next__()

        
        try:
            return self.current.__next__()
        except StopIteration:
            next = self.src.__next__()
            self.current = self.to_iterator(next)
            return self.__next__()




class IndexedIterator:
    def __init__(self, src):
        self.src = src
        self.index = 0

    def __next__(self):
        next_val = self.src.__next__()
        index = self.index
        self.index += 1
        return index, next_val


class FilterIterator:
    def __init__(self, src, filter):
        self.src = src
        self.filter = filter

    def __next__(self):
        next_val = self.src.__next__()
        filter_bool = self.filter(next_val)
        if filter_bool:
            return next_val
        else:
            return self.__next__()


class FileChunkIterator:
    def __init__(self, filename, buffer):
        self.filename = filename
        self.fd = open(filename, "rb")
        self.buffer = buffer

    def __next__(self):
        count = self.fd.readinto(self.buffer)
        if count is None or count == 0:
            raise StopIteration
        return self.buffer[:count]

    def close(self):
        self.fd.close()


class FileLineIterator:
    def __init__(self, filename):
        self.filename = filename
        self.fd = open(filename, "r")

    def __next__(self):
        line = self.fd.readline()
        if line == "":
            raise StopIteration
        return line.rstrip()

    def close(self):
        self.fd.close()

class BlockingQueueIterator:
    def __init__(self, queue, last):
        self.queue = queue
        self.last = last

    def __next__(self):
        next = self.queue.get()
        if next == self.last:
            raise StopIteration
        else:
            return next


class IterativeIterator:
    def __init__(self, seed, func):
        self.seed = seed
        self.func = func

    def __next__(self):
        result = self.seed
        self.seed = self.func(self.seed)
        return result


class EndawareIterator:
    def __init__(self, src, queue, last):
        self.src = src
        self.queue = queue
        self.last = last
        
    def __next__(self):
        try:
            next = self.src.__next__()
            for q in self.queue:
                q.put(next)
            return next
        except StopIteration:
            for q in self.queue:
                q.put(self.last)
            raise StopIteration

class Stream:
    def __init__(self, src, begin_func=None, exit_func=None):
        """Create a stream from an iterator (e.g. iter(list)), or an iterable (e.g. list)
        Optionally, pass in a begin func (when used in WITH keyword), and exit_func (for cleanup)
        Just like "with Stream.from_file_lines("/tmp/a.txt") as stream: xxxx"
        """
        try:
            self.src = iter(src)
        except TypeError:
            self.src = src
        self.begin_func = begin_func
        self.exit_func = exit_func

    def __enter__(self):
        """Called for context enter"""
        if self.begin_func is not None:
            self.begin_func()
        return self

    def __exit__(self, typ, val, tb):
        """Called for cleanup in WITH context"""
        if self.exit_func is not None:
            self.exit_func()

    def __iter__(self):
        """The stream itself is an iterable object, using the passed in iterator"""
        return self.src

    def map(self, mapper):
        """Return a new stream after applying mapper function. Stream is NOT consumed!"""
        return Stream(MapperIterator(self.src, mapper), self.begin_func, self.exit_func)

    def parallel_map(self, mapper, thread_pool=None, thread_count=10):
        """same as map, but do in parallel, this consumes the stream instantly, and return only after all mapper are done,

        if thread_pool is given, use that pool. Otherwise, use a new thread pool,with max thread set to thread_count
        Order is preserved
        """
        if thread_pool is not None:
            def future_mapper(x):
                return thread_pool.submit(mapper, x)
            return Stream(self.map(future_mapper).to_list(), self.begin_func, self.exit_func).map(lambda x:x.result())
        else:
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                def future_mapper(x):
                    return executor.submit(mapper, x)
                return Stream(self.map(future_mapper).to_list(), self.begin_func, self.exit_func).map(lambda x:x.result())

    def filter(self, filterfunc):
        """Return a new stream after applying filter function. Stream is NOT consumed!"""
        return Stream(FilterIterator(self.src, filterfunc), self.begin_func, self.exit_func)

    def limit(self, count):
        """Return a new stream after applying limit on number of elements. Stream is NOT consumed!"""
        return Stream(LimitIterator(self.src, count), self.begin_func, self.exit_func)

    def sum(self):
        """Reduce the stream using addition, thus getting the sum. None if no elements in the stream.
        Stream will be exhausted after summing"""
        return self.reduce(lambda a, b: a + b)


    def pack(self, count=2):
        """Package stream in list of count. Default is 2
        ["key1", "value1", "key2", "value2"].package(2) => [["key1", "value1"], ["key2", "value"2]]

        Partial list (last batch) will have remaining items set to None
        """
        return Stream(PackageIterator(self.src, count), self.begin_func, self.exit_func)

    def to_maps(self):
        """Packaged stream (stream of tuples) can be converted to stream of dictionaries.
        Each dictionary contains a single entry
        ["key1", "value1", "key2", "value2"].package(2).to_maps() => [{"key1", "value1"}, {"key2", "value"2}]

        If each entry has more than 2, the key is first, value is the list without first

        [[1,2,3],[4,5,6,7]].to_maps() => [{1:[2,3]}, {4:[5,6,7]}]
        """
        return Stream(MapCollectorIterator(self.src), self.begin_func, self.exit_func)

    def to_map(self):
        """Packaged strea can be converted to a single dictionary. Earlier entries might be overwritten"""
        result = {}
        def dump(mapobj):
            for key, val in mapobj.items():
                result[key] = val

        self.to_maps().for_each(dump)
        return result

    def to_set(self):
        return set(self)

    def max(self):
        """Find max in stream. Stream is exhausted!"""
        def max_cmp(a, b):
            if a> b:
                return a
            return b
        return self.reduce(max_cmp)

    @staticmethod
    def from_file_lines(filename):
        """Create a stream of lines from a file. This must be used in the
        WITH context or file handles will not be closed"""
        file_iter = FileLineIterator(filename)
        result = Stream(file_iter, None, lambda: file_iter.close())
        return result

    @staticmethod
    def from_file_chunks(filename, buffer=None, buffer_size=4096):
        """Read file in binary, by chunks. Each stream element is a byte array
        Stream exhausts when file is fully read
        If buffer (bytearray) is specified, buffer is used.
        Otherwise, if buffer_size is specified, new bytearray(buffer_size) would be used.
        buffer_size, when not specified, defaults to 4096 bytes.

        This should be faster than allocating new buffer on every new read.

        This iterator re-uses buffer, so it should be much faster with file.read!
        """
        if buffer is None:
            buffer = bytearray(buffer_size)
        file_iter = FileChunkIterator(filename, buffer)
        return Stream(file_iter, None, file_iter.close)

    @staticmethod
    def generate(func):
        return Stream(GeneratorIterator(func))

    @staticmethod
    def iterate(seed, func):
        """Create stream who's first value is seed. subsequent values are repeated application of seed to the func."""
        return Stream(IterativeIterator(seed, func))

    def min(self):
        """Similar to max, but find minimum. This consumes the stream"""
        def min_cmp(a, b):
            if a > b:
                return b
            return a

        return self.reduce(min_cmp)

    def count(self):
        """Count the stream, this consumes the stream"""
        result = 0
        for i in self:
            result+=1
        return result

    def flat_map(self, mapper):
        """Perform a map function, where result might be list, then list is expanded and elements are inserted into stream
        [1,2,3,4,5].flat_map(lambda x: [x for i in range(3)]) => [1,1,1,2,2,2,3,3,3,4,4,4,5,5,5] 
        """
        return self.map(mapper).flatten()


    def flatten(self):
        """If stream is a stream of iterable objects (e.g. list, or iterators), flatten them in to single stream
        [[1,2,3],[4,5],[6], 7] => [1,2,3,4,5,6,7]
        
        Flatten only does one level of expansion. 
        Iterable => expanded 
        Iterators => consumed and expanded
        plain value => copied
        nested lists => expanded one level only
        [[[[[1,2,3]]]],4,5].flatten() = [[[[1,2,3]]], 4, 5]
        [[[[1,2,3]]], 4, 5].flatten() = [[[1,2,3]], 4, 5]
        [[[1,2,3]],4,5].flatten() = [[1,2,3], 4, 5]
        [[1,2,3], 4, 5].flatten() = [1, 2, 3, 4, 5]
        """
        return Stream(FlattenIterator(self.src), self.begin_func, self.exit_func)

    def with_index(self):
        """Handy utility that mappes element to (index, element) tuple, so you know the index of element when accessing
        Note that if you do with_index().with_index().with_index(), you will get nested tuples, which may not be what
        you wanted"""
        return Stream(IndexedIterator(self.src), self.begin_func, self.exit_func)

    def peek(self, func):
        """Attach a consumer function on consumption. When a stream element is consumed, the peek function is called
        with the consumed element. e.g. stream.peek(print).count(), will get element count,but also print the element out"""
        return Stream(PeekIterator(self.src, func), self.begin_func, self.exit_func)

    def __add__(self, other):
        """Supports stream concatination. stream1 + stream2 = stream1.concat(stream2)"""
        return self.concat(other)

    def for_each(self, func):
        """Consume the stream and for each element, call the function to"""
        while True:
            try:
                next_value = self.src.__next__()
                func(next_value)
            except StopIteration:
                break

    def ordered(self, key=None, reverse=False):
        """Order this stream using comparator. If none, using built in comparator. 
        Note that this consumes all elements! It put elements in stream. Don't use on unbounded streams like generator
        """
        consumed = list(self)
        consumed.sort(key=key, reverse=reverse)
        return Stream(consumed, self.begin_func, self.exit_func)

    def uniq(self):
        """Create stream of unique elements. Order is preserved.
        Note that this consumes all elements! Don't use on unbounded stream or super large streams."""
        emitted = set()

        def emit_filter(x):
            if x in emitted:
                return False
            else:
                emitted.add(x)
                return True

        return self.filter(emit_filter)

    def to_list(self):
        """Convert stream to list. alternatively, list(stream) does the samething, since stream itself is iterable.
        This consumes the stream"""
        return list(self)

    def repeat(self, times=2):
        """Repeat this stream for time times, this consumes stream once, but future repeats are iterative (not fully im memory)
        Stream([1,2,3,4,5]).repeat(100000000) won't cause memory error by itself, but if you order it, it will die for sure.
        """
        consumed = list(self)
        result = Stream(consumed, self.begin_func, self.exit_func)

        for i in range(times - 1):
            result = result.concat(Stream(consumed))
        
        return result

    def skip(self, skip):
        """Create a new stream with first N elements skipped. This does not consume stream"""
        return Stream(SkipIterator(self.src, skip), self.begin_func, self.exit_func)

    def pick(self, index=0):
        """When stream elements are tuple, or lists, pick the nth element from the tuple/list.
        stream.with_index().pick(0) gives stream of indexes
        stream.with_index().pick(1) is same as original stream, but wasted cpu cycles, why not? """

        return self.map(lambda x:x[index])

    def consume_all(self):
        """Consume all elements, and discard them"""
        self.for_each(lambda x: None)

    def concat(self, next):
        """Concats two streams and get a new stream. First stream is consumed first, then second stream
        Begin/end functions are composed, first is called then second

        This doesn't consume the stream"""
        def new_begin():
            if self.begin_func is not None:
                self.begin_func()
            if next.begin_func is not None:
                next.begin_func()

        def new_exit():
            if self.exit_func is not None:
                self.exit_func()
            if self.exit_func is not None:
                next.exit_func()
        return Stream(ConcatIterator(self.src, next.src), new_begin, new_exit)

    def split(self, count=2):
        """Split this stream into count streams. First one is primary stream, subsequent ones' consumption is depend on consumption by first one.
        The elements are available on second one only after first one had consumed it.
        Remember to consume first split stream.

        If base stream is consumed, s1, s2 will have no elements to consume from.

        begin_func & end_func are attached to primary stream.
        """
        qs = [queue.Queue() for _ in range(count - 1)]
        last = object()

        def peek_func_for(i):
            def peek_func(x):
                qs[i].put(x)
            return peek_func
        
        result = [None for _ in range(count)]
        result[0] = Stream(EndawareIterator(self.src, queue=qs, last=last), self.begin_func, self.exit_func)
        
        for i in range(0, count - 1):
            result[i + 1] = Stream(BlockingQueueIterator(qs[i], last))
        
        return result

    def reduce(self, reducer):
        """Reducing stream to a single element using reducer. A reducer is a function that takes two arguments
        and returns 1 result. e.g. lambda a,b: a + b is an add reducer. It will get sum"""
        first_value = None
        try:
            first_value = self.src.__next__()
        except StopIteration:
            return first_value

        result = first_value
        while True:
            try:
                next_value = self.src.__next__()
                result = reducer(result, next_value)
            except StopIteration:
                break
        return result


if __name__ == "__main__":
    s1 = Stream(range(0, 100), lambda:print("hello"), lambda:print("bye")).map(lambda i:i+1).skip(50).limit(10)
    with s1:
        print(s1.to_list())
    #print(list(s1))
    #print(list(s1))
    s2 = Stream(iter(range(100, 105)))
    #print(list(s2))

    print(Stream(range(1,10)).count())
    print("1 + ... + 100=")
    print((s1 + s2).peek(print).sum())
    tuple = (1,2,3,4,7)
    with Stream(tuple) as stream:
        stream.with_index().pick(0).filter(lambda x: x < 2).for_each(print)

    with Stream.from_file_lines("./src/pystream/pystream.py").limit(5) as stream1:
        with Stream.from_file_lines("./src/pystream/pystream.py").skip(5) as stream2:
            print(f"File has {((stream1 + stream2).with_index().pick(0).count())} lines")

    dict1 = {'k1': 'v1', 'k2': 'v2'}

    key_stream = Stream(dict1.keys())
    key_stream.map(lambda k: dict1[k]).for_each(
        print
    )
    pairs_stream = Stream(dict1.values())
    pairs_stream.for_each(print)

    string = "hello, world"
    iterator = iter(string)
    stream = Stream(iterator)
    stream.for_each(print)

    print(Stream.generate(lambda:5).limit(1000).sum())
    a = 1
    b = 1
    def fib():
        global a
        global b
        a, b = b, a+b
        return a
    
    Stream.generate(fib).limit(10).for_each(print)

    def slow_map(x):
        sleep(2)
        return x * 2

    Stream.generate(lambda:5).limit(10).parallel_map(slow_map).for_each(print)

    Stream([[1,2], [3,4], 5, [[6,7]]]).flatten().flatten().pack(3).for_each(print)

    Stream(dict1.items()).flatten().pack(2).to_maps().for_each(print)

    print(Stream(dict1.items()).flatten().pack(2).concat(Stream(["k1", "v1_new", "k4", "v4"]).pack(2)).to_map())

    print(Stream.generate(lambda: 5).limit(10).to_set())

    with Stream([1,2,3,4,5], lambda:print("begin"),lambda: print("end")).flat_map(lambda x: [x for _ in range(3)]).uniq() as stream:
        print(stream.to_list())

    Stream([[2, 5], [3, 3]]).for_each(print)
    Stream([[2, 5], [3, 3]]).flat_map(lambda x: [x[0] for _ in range(x[1])]).for_each(print)

    Stream([4,3,2,1,5]).ordered(reverse=True).for_each(print)

    Stream([1,1,2,3,4,4,5]).uniq().for_each(print) # [1,2,3,4,5]

    print (Stream([1,2,3,4,5]).repeat(100).to_list())
    try:
        Stream(["k1", "v1", "k2", "v2"]).to_maps().for_each(print)
    except ValueError:
        print("Glad it caused error")

    print(Stream(["k1", "v1", "k2", "v2"]).pack(3).to_map())

    Stream(["I love python"]).repeat(3).for_each(print)

    s1, s2, s3, s4 = Stream([1,2,3,4,5],lambda:print("Begin"), lambda:print("end")).split(4)
    
    def slow_consume(name, stream):
        for i in stream:
            print(f"{name} => consumed {i}")
            sleep(1)

    def consume_asap(name, stream):
        for i in stream:
            print(f"{name} => consumed {i}")

    with s1, s2, s3, s4:
        t1 = threading.Thread(target=slow_consume, args=("s1", s1))
        t2 = threading.Thread(target=consume_asap, args=("s2", s2))
        t3 = threading.Thread(target=consume_asap, args=("s3", s3))
        t4 = threading.Thread(target=consume_asap, args=("s4", s4))
        Stream([t1, t2, t3, t4]).for_each(lambda x: x.start())
        Stream([t1, t2, t3, t4]).for_each(lambda x: x.join())

    # if you run s2.for_each, it will block forever!
    def adder(x):
        return x + 1
    
    Stream.iterate(1, adder).limit(10).for_each(print)

    # Copy file
    with Stream.from_file_chunks("src/pystream/pystream.py").skip(0) as stream:
        with open("output.py", "wb") as output:
            stream.for_each(lambda x: output.write(x))

