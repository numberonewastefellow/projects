import threading
import time
import pickle
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

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def ngram5_validation(file_name, output_dir, data, chunk_number, params):
    for line in data:
        sys.stdout.write("\r sa->" + random.choice(string.ascii_letters))
        sys.stdout.flush()
        line = line.lower()
        words = line.split()
        if len(words)!=8:
            print ("ngram is not 8 words => {}".format(line))

        if not (len(words)>5) & (is_int(words[5])):
            print ("year {} is not valid for ==> {}".format(words[5],line))

def split_data_by_word(file_name, output_dir, lines, chunk_number, params):
    # print('process started', file_name, chunk_number, len(data))
    my_word_list = ['muslim', 'islam']
    words_result = []
    the_year_wise = {}
    word_year_wise = {}
    year_index_in_ngram = 5
    n5grm_length = 8

    for line in lines:
        sys.stdout.write("\r" + random.choice(string.ascii_letters))
        sys.stdout.flush()
        line = line.lower()

        for wd in my_word_list:
            if wd in line:
                # specified word moved to seperate dataset
                words_result.append(line)
                break

        # word 'the' counts year wise
        if "the" in line:
            t_w = line.split()
            if len(t_w) != n5grm_length:
                break
            year = 0
            if is_int(t_w[year_index_in_ngram]):
                year = t_w[year_index_in_ngram]
            if year == 0:
                break
            local_the = 0
            for w in t_w[0:5]:
                if "the" in w:
                    local_the = local_the + 1

            if local_the > 0:
                if is_int(t_w[6]):
                    no_of_occ = int(t_w[6])
                    local_the = local_the * no_of_occ
                    if year in the_year_wise:
                        the_year_wise[year] = the_year_wise[year] + local_the
                    else:
                        the_year_wise[year] = local_the

        #words count year wise
        non_of_the_word_found = True
        for word in my_word_list:
            if word in line:
                t_w = line.split()
                if len(t_w) != n5grm_length:
                    break
                year = 0
                if is_int(t_w[year_index_in_ngram]):
                    year = t_w[year_index_in_ngram]
                if year == 0:
                    break
                local_word = 0
                for w in t_w[0:5]:
                    if word in w:
                        local_word = local_word + 1
                # if (local_word > 0 &  one_of_the_word_found == False) or (one_of_the_word_found & local_word>1):
                if local_word > 0:
                    if is_int(t_w[6]):
                        no_of_occ = int(t_w[6])
                        if non_of_the_word_found == False & local_word > 1:
                            continue

                        local_word = local_word * no_of_occ
                        non_of_the_word_found = False
                        if year in word_year_wise:
                            word_year_wise[year] = word_year_wise[year] + local_word
                        else:
                            word_year_wise[year] = local_word



  #  with ProcessPoolExecutor(max_workers=1) as pe:
   #     feature = pe.submit(write_to_file,words_result, file_name, output_dir+"words_ds", chunk_number)
    if len(words_result) > 0:
        write_to_file( words_result, file_name, output_dir + "words_ds", chunk_number)
   # with ProcessPoolExecutor(max_workers=1) as pt:
   #     feature = pt.submit(write_to_file, the_year_wise, file_name, output_dir+"year_wise_count", chunk_number)
    if len(the_year_wise)>0:
        write_to_file_pickle(  the_year_wise, file_name, output_dir + "the_year_wise_count", chunk_number)

   # with ProcessPoolExecutor(max_workers=1) as px:
   #     feature = px.submit(write_to_file, word_year_wise, file_name, output_dir+"word_wise_count", chunk_number)
    if len(word_year_wise) > 0:
        write_to_file_pickle( word_year_wise, file_name, output_dir + "words_year_wise_count", chunk_number)

    # sync write_to_file(ls_1995_1996, file_name, output_dir, chunk_number)

    return file_name, chunk_number, len(lines), params

def write_to_file_pickle(data, file_name, output_dir, chunk_number):
    if len(data) == 0:
        return True

    dir_path = os.path.join(output_dir, os.path.basename(file_name))
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    path = os.path.join(dir_path, "{}.part.{}.pickle".format(current_time(), str(chunk_number)))
    if (os.path.exists(path)):
        print("file path exit {}".format(path))
    path = path.replace("\\\\", "\\")

    with open(path, 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
    return  True



def write_to_file_text(data, file_name, output_dir, chunk_number):
    if len(data) == 0:
        return True

    dir_path = os.path.join(output_dir, os.path.basename(file_name))
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    path = os.path.join(dir_path, "{}.part.{}.txt".format(current_time(), str(chunk_number)))
    if (os.path.exists(path)):
        print("file path exit {}".format(path))
    path = path.replace("\\\\", "\\")

    with open(path, 'w') as f:
        f.write(data)
    return  True

def write_to_file_callback(future):
    print('files written to disk')


def time_milli_sec():
    x = lambda: int(round(time.time() * 1000))
    return x()


def write_to_file(lines, file_name, output_dir, chunk_number):
        if len(lines) == 0:
            return True
        dir_path = os.path.join(output_dir,  os.path.basename(file_name))
        try:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        path = os.path.join(dir_path, "{}.part.{}".format(current_time(), str(chunk_number)))
        if os.path.exists(path):
            print("file path exit {}".format(path))
        path = path.replace("\\\\", "\\")

        with open(path, 'w') as fs:
            fs.writelines(lines)
        return True


def callback_function(future):
    # print('process finished', future.result())
    x = future.result()





def chunks(list_values, chunk_size):
    chunk_size = max(1, chunk_size)
    return (list_values[i:i + chunk_size] for i in range(0, len(list_values), chunk_size))





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
        #print(file_results)
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
                #batch_size = int(chunk_size / num_workers)
                chunked_data = chunks(lines, chunk_size)
                with ProcessPoolExecutor(max_workers=num_workers) as e:
                    for data in chunked_data:
                        future = e.submit(split_data_by_word, file_name, output_dir, data,
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




def process_file_no_parallel(chunk_size, file_name, input_dir, num_workers, output_dir):
    output = []
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
                for data in chunked_data:
                        future = split_data_by_word( file_name, output_dir, data,chunk_number, ("started at {}".format(current_time()), chunk_number))
                        output.append(future)
                        chunk_number = chunk_number + 1

        results = [f.result() for f in output]
        # total_lns = np.sum(results)  # [count(col) for col in results]
        # print("total lines {}".format(total_lns))
        print("{} finished at {}".format(file_name, datetime.utcnow()))
    except Exception as e:
        traceback.print_exc()




#main(1,500000, 'E:/download/test/output',"E:/download/1995_2005_ds")

if __name__ == '__main__':
    # test_batch_execution('one_file', "E:/download/output_files")#
        start_process()
        print("in main")
