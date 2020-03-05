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
        if len(words) != 8:
            print("ngram is not 8 words => {}".format(line))

        if not (len(words) > 5) & (is_int(words[5])):
            print("year {} is not valid for ==> {}".format(words[5], line))


def split_data_by_word_test_case():
    moods_dict = prepare_moods_dict("E:/download/proj/n5grm/small_ds/emotion-lexicon-master/eng/")
    data =  ["1947-1958 ( islamabad : threat	2008	2	2", "1992_num )_. the_det islamic_adj threat_noun	1995	7	7","1035_noun ,_. islamabad_adj ;_. _end_	1970	2	2"]
    split_data_by_word("fname.txt",moods_dict, "debug_output/",data, 0,0)


def split_data_by_word(file_name, moods_dict, output_dir, lines, chunk_number, params):
    # print('process started', file_name, chunk_number, len(data))
    my_word_list = moods_dict  # {'muslim': ['one', 'two'], 'islam': ['three', 'four']}
    words_result = []
    mixed_polarity_key = 'mixed_polarity'
    nutral_key = 'nutral'
    mood_year_wise = {}
    year_index_in_ngram = 5
    n5grm_length = 8
    pos = ['adj','noun','adp','adv','conj','det','num','pron','prt','verb']
    pos_check = True
    for line in lines:
        sys.stdout.write("\r{}=>{}".format(random.choice(string.ascii_letters), chunk_number))
        sys.stdout.flush()
        line = line.lower()



        # validate the ngram data
        t_w = line.split()
        if len(t_w) != n5grm_length:
            break
        year = 0
        if is_int(t_w[year_index_in_ngram]):
            year = t_w[year_index_in_ngram]
        if year == 0:
            break
        if not is_int(t_w[6]):
            break

        # word moods counts year wise
        # words count year wise
        # add counts to temp group, later we will find how many groups and mixed or nuteral
        mood_group_wise_count = {}
        for k_group, v_words in my_word_list.items():
            local_word = 0
            for word in v_words:
                if word in line:
                    for w in t_w[0:5]:
                        pos_found = False
                        if pos_check & (not pos_found):
                            for p in pos:
                                if pos_found:
                                    break
                                if p in w:
                                    pos_word = w.split('_')
                                    for pow in pos_word:
                                        if p == pow:
                                            pos_found = True
                                            break


                        if (word == w) & (not pos_found):
                          local_word = local_word + 1
                    # if (local_word > 0 &  one_of_the_word_found == False) or (one_of_the_word_found & local_word>1):
            if local_word > 0:
                no_of_occ = int(t_w[6])


                local_word = local_word * no_of_occ

                if k_group in mood_group_wise_count:
                    mood_group_wise_count[k_group] = mood_group_wise_count[k_group] + local_word
                else:
                    mood_group_wise_count[k_group] = local_word

                # if year in mood_year_wise:
                #    mood_year_wise[year][k_group] = mood_year_wise[year][k_group] + local_word
                # else:
                #    mood_year_wise[year][k_group] = local_word

        # count mood wise count to see if the diffent moods are availae or single mood
        c = sum(mood_group_wise_count.values())

        # seperated ds for moods related lines
        if c > 0:
            words_result.append(line)

        # if ngram not contains any mood related word
        if len(mood_group_wise_count) == 0:
            k_group = nutral_key
            c = 1
        elif len(mood_group_wise_count) > 1:
            k_group = mixed_polarity_key
        else:
            k_group = list (mood_group_wise_count)[0]

        if year not in mood_year_wise:
            mood_year_wise[year] = {}

        if k_group not in mood_year_wise[year]:
            mood_year_wise[year][k_group] = 0

        mood_year_wise[year][k_group] = mood_year_wise[year][k_group] + c

    if len(words_result) > 0:
        write_to_file(words_result, file_name, output_dir + "moods_words_ds", chunk_number)

    if len(mood_year_wise) > 0:
        write_to_file_pickle(mood_year_wise, file_name, output_dir + "moods_year_wise_count", chunk_number)

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
    return True


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
    return True


def write_to_file_callback(future):
    print('files written to disk')


def time_milli_sec():
    x = lambda: int(round(time.time() * 1000))
    return x()


def write_to_file(lines, file_name, output_dir, chunk_number):
    if len(lines) == 0:
        return True
    dir_path = os.path.join(output_dir, os.path.basename(file_name))
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
    parser.add_argument('--moods_dir', type=str, required=True)
    args = parser.parse_args()
    main(args.num_workers, args.chunk_size, args.no_of_file_in_parallel, args.input_dir, args.output_dir,
         args.moods_dir)


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


def write_dict_as_csv(dic, file_name):
    pass


def write_dict_as_pickle(dic, file_name):
    with open(file_name, 'wb') as f:
        pickle.dump(dic, f, pickle.HIGHEST_PROTOCOL)


def write_text_to_file(content, file_name):
    with open(file_name, 'w') as f:
        f.writelines(content)


def main(num_workers, chunk_size, no_of_file_in_parallel, input_dir, output_dir, moods_input_dir):
    print("{} workers".format(num_workers))
    if not os.path.exists(output_dir):
        print("output will be here {}", output_dir)
        os.makedirs(output_dir)
    # for each input file
    files = os.listdir(input_dir)
    file_worker = no_of_file_in_parallel

    print("preparing moods dictionary from {}".format(moods_input_dir))
    moods_dict = prepare_moods_dict( moods_input_dir)

    write_text_to_file(str(moods_dict), os.path.join(output_dir, "moods_dict.txt"))
    write_dict_as_pickle(moods_dict, os.path.join(output_dir, "moods_dict.pkl"))


    if (num_workers <= 1) or (chunk_size == 0):
        print("programe is running in single thread mode")
        for file_name in files:
            print("processing file ".format(file_name))
            process_file_no_parallel(0, file_name, input_dir, 1, output_dir, moods_dict)
    else:
        file_futures = []
        with ProcessPoolExecutor(max_workers=file_worker) as fe:
            for file_name in files:
                file_future = fe.submit(process_file, chunk_size, file_name, input_dir, num_workers, output_dir,
                                        moods_dict)
                file_futures.append(file_future)
            file_results = [f.result() for f in file_futures]
            # print(file_results)
            # process_file(chunk_size, file_name, input_dir, num_workers, output_dir)


def prepare_moods_dict( moods_input_dir):
    moods_dict = {}
    for f in os.listdir(moods_input_dir):
        paath = os.path.join(moods_input_dir, f)
        print("reading dictionary {}".format(paath))
        if os.path.isfile(paath):
            (fn, ext) = os.path.splitext(f)
            print("preparing mood dict for mood type {}".format(fn))
            with open(paath) as ff:
                mood_l = ff.readline().split()
                mood_l = list(set(mood_l))
                moods_dict[fn] = mood_l
                print("{} has {} words".format(fn, len(mood_l)))
    return  moods_dict

def process_file(chunk_size, file_name, input_dir, num_workers, output_dir, moods_dict):
    futures = []
    try:
        file_name = os.path.join(input_dir, file_name)
        size = get_file_size(file_name)
        print("params ==>", chunk_size, file_name, input_dir, num_workers, output_dir)
        print("processing file {}, size {} <=> size{}, started at {}".format(file_name, convert_bytes(size, "MB"),
                                                                             convert_bytes(size, "GB"),
                                                                             datetime.utcnow()))
        chunk_number = 0
        print("programme running in parrllel mode")

        with open(file_name) as file:
            while True:
                lines = list(islice(file, chunk_size))
                if not lines:
                    break
                # batch_size = int(chunk_size / num_workers)
                chunked_data = chunks(lines, chunk_size)
                with ProcessPoolExecutor(max_workers=num_workers) as e:
                    for data in chunked_data:
                        print("{} chunk started in parallel".format(chunk_number))
                        future = e.submit(split_data_by_word, file_name,moods_dict, output_dir, data,
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


def process_file_no_parallel(chunk_size, file_name, input_dir, num_workers, output_dir, moods_dict):
    output = []
    try:
        file_name = os.path.join(input_dir, file_name)
        size = get_file_size(file_name)
        print("processing file {}, size {} <=> size{}, started at {}".format(file_name, convert_bytes(size, "MB"),
                                                                             convert_bytes(size, "GB"),
                                                                             datetime.utcnow()))
        chunk_number = 0
        with open(file_name) as file:
            lines = file.readlines()
            res = split_data_by_word(file_name, moods_dict, output_dir, lines, 0,
                                     ("started at {}".format(current_time()), 0))

        print("{} finished at {}".format(file_name, datetime.utcnow()))
    except Exception as e:
        traceback.print_exc()


#main(1, 500000, 1, 'E:/download/proj/n5grm/small_ds/data/moods_ds/', "E:/download/proj/n5grm/small_ds/data/output","E:/download/proj/n5grm/small_ds/emotion-lexicon-master/eng")

if __name__ == '__main__':
    # test_batch_execution('one_file', "E:/download/output_files")#
    #split_data_by_word_test_case()
    start_process()
    print("in main")
