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
    def generate(func):
        return Stream(GeneratorIterator(func))

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

    def to_list(self):
        """Convert stream to list. alternatively, list(stream) does the samething, since stream itself is iterable.
        This consumes the stream"""
        return list(self)

    def skip(self, skip):
        """Create a new stream with first N elements skipped. This does not consume stream"""
        return Stream(SkipIterator(self.src, skip), self.begin_func, self.exit_func)

    def pick(self, index=0):
        """When stream elements are tuple, or lists, pick the nth element from the tuple/list.
        stream.with_index().pick(0) gives stream of indexes
        stream.with_index().pick(1) is same as original stream, but wasted cpu cycles, why not? """

        return self.map(lambda x:x[index])

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

    with Stream.from_file_lines("pystream.py").limit(5) as stream1:
        with Stream.from_file_lines("pystream.py").skip(5) as stream2:
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