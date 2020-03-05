import threading
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import os
import signal
from itertools import islice
import sys
import random
import numpy as np
import random, string
import time, sys
import datetime
import traceback
import argparse
from io import SEEK_END
import multiprocessing as mp
import os
import itertools
from datetime import datetime
import errno


def current_time():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def test(write, file_name, output_dir, data, chunk_number):
    print('batch {}: running  with  pid {}'.format(chunk_number, os.getpid()))
    even = []
    odd = []
    for line in data:
        if int(line) % 2 == 0:
            even.append("{}\n".format(line))
        else:
            odd.append("{}\n".format(line))
    all_groups = [even, odd]
    write(all_groups, file_name, output_dir, chunk_number)
    return len(data)


def split_dummy_test(file_name, output_dir, data, chunk_number, params):
    print(file_name, output_dir, chunk_number, params)


def split_data_by_year(file_name, output_dir, data, chunk_number, params):
    # print('process started', file_name, chunk_number, len(data))
    ls_1995_1996 = []
    for i in range(1995, 2006):
        ls_1995_1996.append([])
    for line in data:
        sys.stdout.write("\r" + random.choice(string.ascii_letters))
        sys.stdout.flush()
        if "1995" in line:
            ls_1995_1996[0].append(line)
        elif "1996" in line:
            ls_1995_1996[1].append(line)
        elif "1997" in line:
            ls_1995_1996[2].append(line)
        elif "1998" in line:
            ls_1995_1996[3].append(line)
        elif "1999" in line:
            ls_1995_1996[4].append(line)
        elif "2000" in line:
            ls_1995_1996[5].append(line)
        elif "2001" in line:
            ls_1995_1996[6].append(line)
        elif "2002" in line:
            ls_1995_1996[7].append(line)
        elif "2003" in line:
            ls_1995_1996[8].append(line)
        elif "2004" in line:
            ls_1995_1996[9].append(line)
        elif "2005" in line:
            ls_1995_1996[10].append(line)

    with ProcessPoolExecutor(max_workers=1) as pe:
        feature = pe.submit(write_to_file, file_name, output_dir, chunk_number)
        feature.add_done_callback(write_to_file_callback)

    # sync write_to_file(ls_1995_1996, file_name, output_dir, chunk_number)
    return file_name, chunk_number, len(data), params


def write_to_file_callback(future):
    print('files written to disk', future.result())


def time_milli_sec():
    x = lambda: int(round(time.time() * 1000))
    return x()


def write_to_file(lines, file_name, output_dir, chunk_number):
    # print("in writing file chunk {}".format(chunk_number))
    year = 1995
    lst_files = []
    for line_set in lines:
        dir_path = os.path.join(output_dir, str(year), os.path.basename(file_name))
        try:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        # path = os.path.join(dir_path, "{}.part.{}".format(time_milli_sec(),str(chunk_number)))
        path = os.path.join(dir_path, "{}.{}.part.{}".format(current_time(), str(year), str(chunk_number)))
        lst_files.append((path,chunk_number,len(lines)))
        if (os.path.exists(path)):
            print("file path exit {}".format(path))
        path = path.replace("\\\\", "\\")
        # time.sleep(10000)
        with open(path, 'w') as fs:
            fs.writelines(line_set)
        year = year + 1

    return lst_files


def callback_function(future):
    # print('process finished', future.result())
    x = future.result()


def parse_lines(lines, file_name, output_dir):
    split_data_by_year(write_to_file, file_name, output_dir, lines)


def chunks(list_values, chunk_size):
    chunk_size = max(1, chunk_size)
    return (list_values[i:i + chunk_size] for i in range(0, len(list_values), chunk_size))


def chunks2(list_values, size):
    it = iter(list_values)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))
    return item


def test_batch_execution(file_name, output_dir):
    futures = []
    multi_lines = [str(i) for i in range(1, 40)]
    chunked_data = chunks(multi_lines, 10)
    chunk_number = 0
    with ProcessPoolExecutor(max_workers=10) as e:
        for data in chunked_data:
            future = e.submit(test, write_to_file, file_name, output_dir, data, chunk_number)
            future.add_done_callback(callback_function)
            futures.append(future)
            chunk_number = chunk_number + 1

    results = [f.result() for f in futures]
    # collect results
    # total_lns = np.sum(results)#[count(col) for col in results]
    # print("total lines {}".format(total_lns))
    print(file_name + "all threads finished")


def parse_lines2(lines, num_workers):
    parameters = [1, 2, 3]
    et = ProcessPoolExecutor(max_workers=num_workers)

    futures = []

    for line in lines:
        for params in parameters:
            future = et.submit(split_data_by_year, write_to_file, params, line)
            futures.append(future)


def start_process():
    global parser, args
    parser = argparse.ArgumentParser(description='Parallel text processor')
    parser.add_argument('--num_workers', '-n', default=8, type=int)
    parser.add_argument('--chunk_size', '-c', default=8, type=int)
    parser.add_argument('--no_of_file_in_parallel', '-f', default=8, type=int)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--input_dir', type=str, required=True)
    args = parser.parse_args()
    main(args.num_workers, args.chunk_size, args.no_of_file_in_parallel, args.input_dir, args.output_dir)


def get_file_size(file_path):
    size = os.path.getsize(file_path)
    return size


def convert_bytes(size, unit=None):
    if unit == "KB":
        return ('File size: ' + str(round(size / 1024, 3)) + ' Kilobytes')
    elif unit == "MB":
        return ('File size: ' + str(round(size / (1024 * 1024), 3)) + ' Megabytes')
    elif unit == "GB":
        return ('File size: ' + str(round(size / (1024 * 1024 * 1024), 3)) + ' Gigabytes')
    else:
        return ('File size: ' + str(size) + ' bytes')


def main(num_workers, chunk_size, no_of_file_in_parallel, input_dir, output_dir):
    print("{} workers".format(num_workers))
    output_dir = os.path.join(output_dir, "1995_2005_ds")
    if not os.path.exists(output_dir):
        print("output will be here {}", output_dir)
        os.makedirs(output_dir)
    # for each input file
    files = os.listdir(input_dir)
    file_worker = no_of_file_in_parallel

    file_futures = []
    with ProcessPoolExecutor(max_workers=file_worker) as fe:
        for file_name in files:
            file_future = fe.submit(process_file, chunk_size, file_name, input_dir, num_workers, output_dir)
            file_futures.append(file_future)
        file_results = [f.result() for f in file_futures]
        print(file_results)
        # process_file(chunk_size, file_name, input_dir, num_workers, output_dir)


def process_file(chunk_size, file_name, input_dir, num_workers, output_dir):
    futures = []
    try:
        file_name = os.path.join(input_dir, file_name)
        size = get_file_size(file_name)
        print("processing file {}, size {} <=> size{}, started at {}".format(file_name, convert_bytes(size, "MB"),
                                                                             convert_bytes(size, "GB"),
                                                                             datetime.utcnow()))
        chunk_number = 0
        with open(file_name) as file:
            while True:
                lines = list(islice(file, chunk_size))
                if not lines:
                    break
                batch_size = int(chunk_size / num_workers)
                chunked_data = chunks(lines, batch_size)
                with ProcessPoolExecutor(max_workers=num_workers) as e:
                    for data in chunked_data:
                        future = e.submit(split_data_by_year, file_name, output_dir, data,
                                          chunk_number, ("started at {}".format(current_time()), chunk_number))
                        future.add_done_callback(callback_function)
                        futures.append(future)
                        chunk_number = chunk_number + 1

        results = [f.result() for f in futures]
        # total_lns = np.sum(results)  # [count(col) for col in results]
        # print("total lines {}".format(total_lns))
        print("{} finished at {}".format(file_name, datetime.utcnow()))
    except Exception as e:
        traceback.print_exc()


# main(1,500000, 'E:/download/test/output',"E:/download/1995_2005_ds")

if __name__ == '__main__':
    # test_batch_execution('one_file', "E:/download/output_files")
    start_process()
    print("in main")
