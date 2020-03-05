#!/usr/bin/env python
from multiprocessing import Array, Pool

def init(counters_): # called for each child process
    global counters
    counters = counters_

def search_function (value): # assume it is CPU-intensive task
    ls_1995 = []
    if "1995" in value:
        ls_1995.append(value)
    return ls_1995

if __name__ == '__main__':
    counters = Array('i', [0]*3)
    pool = Pool(initializer=init, initargs=[counters])
    values = (line.split()[3] for line in textfile)
    for value, result, error in pool.imap_unordered(search_function, values,
                                                    chunksize=1000):
        if error is not None:
            print('value: {value}, error: {error}'.format(**vars()))
    pool.close()
    pool.join()
    print(list(counters))