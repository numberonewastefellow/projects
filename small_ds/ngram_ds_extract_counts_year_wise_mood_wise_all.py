import argparse
import datetime
import errno
import ntpath
import os
import pickle
import random
import shutil
import string
import sys
import time
import traceback
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import islice

import pandas as pd


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
    data = ["1947-1958 ( islamabad : threat	2008	2	2",
            "1992_num )_. the_det islamic_adj threat_noun	1995	7	7",
            "1035_noun ,_. islamabad_adj ;_. _end_	1970	2	2"]
    split_data_by_word("fname.txt", moods_dict, "debug_output/", data, 0, 0)


def split_data_by_word(file_name, moods_dict, can_check_pos, output_dir, lines, chunk_number, isparallel, params):
    # print('process started', file_name, chunk_number, len(data))
    my_word_list = moods_dict  # {'muslim': ['one', 'two'], 'islam': ['three', 'four']}
    words_result = []
    mixed_polarity_key = 'mixed_polarity'
    nutral_key = 'neutral'
    mood_year_wise = {}
    year_index_in_ngram = 5
    n5grm_length = 8
    pos_check = "yes" in can_check_pos
    no_of_lines = len(lines)
    islamic_words = ['muslim', 'islam', 'muslims', "muslim's"]
    mood_year_wise_word_wise = {}
    for line in lines:
        sys.stdout.write("\r{}=>{}==>{}".format(random.choice(string.ascii_letters), chunk_number, (no_of_lines)))
        sys.stdout.flush()
        line = line.lower()
        no_of_lines = no_of_lines - 1
        # validate the ngram data
        ngrams = line.split()
        if len(ngrams) != n5grm_length:
            break
        year = 0
        if is_int(ngrams[year_index_in_ngram]):
            year = ngrams[year_index_in_ngram]
        if year == 0:
            break
        if not is_int(ngrams[6]):
            break

        no_of_occ = int(ngrams[6])

        if year not in mood_year_wise:
            mood_year_wise[year] = {}
        # the count
        # islamic words count
        #the_islamic_counts(islamic_words, line, mood_year_wise, ngrams, year, pos_check)

        # word moods counts year wise
        # words count year wise
        # add counts to temp group, later we will find how many groups and mixed or nuteral
        mood_group_wise_count = {}
        for k_group, v_moods in my_word_list.items():
            for mood in v_moods:
                local_word = 0
                if mood in line:
                    for ngram in ngrams[0:5]:
                        pos_found = False
                        if pos_check:
                            pos_found = pos_in_word(ngram)
                            if pos_found & pos_check:
                                continue

                        if (mood == ngram) & pos_check & (not pos_found):
                            local_word = local_word + 1
                            # print("pos found ==>"+k_group+"==> " + mood +"==> "+ str(local_word))
                        elif (mood == ngram) & (not pos_check):
                            local_word = local_word + 1
                        if local_word > 0:
                            if year not in mood_year_wise_word_wise:
                                mood_year_wise_word_wise[year] = {}
                            if k_group not in mood_year_wise_word_wise[year]:
                                mood_year_wise_word_wise[year][k_group] = {}
                            if mood not in mood_year_wise_word_wise[year][k_group]:
                                mood_year_wise_word_wise[year][k_group][mood] = local_word
                            else:
                                mood_year_wise_word_wise[year][k_group][mood] = mood_year_wise_word_wise[year][k_group][
                                                                                    mood] + local_word

                if local_word == 0:
                    continue
                local_word = local_word * no_of_occ

                if k_group in mood_group_wise_count:
                    mood_group_wise_count[k_group] = mood_group_wise_count[k_group] + local_word
                else:
                    mood_group_wise_count[k_group] = local_word

        # count mood wise count to see if the diffent moods are availae or single mood
        c = sum(mood_group_wise_count.values())
        if c > 0:
            pass
            # print("c count ==> " +str(c))
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
            k_group = list(mood_group_wise_count)[0]

        if k_group not in mood_year_wise[year]:
            mood_year_wise[year][k_group] = 0

        mood_year_wise[year][k_group] = mood_year_wise[year][k_group] + c

    suffix = '_with_out_pos'
    if pos_check:
        suffix = '_with_pos'

    if len(words_result) > 0:
        write_to_file(words_result, file_name, output_dir + "moods_words_ds" + suffix, chunk_number)

    if len(mood_year_wise) > 0:
        fpath = write_to_file_pickle(mood_year_wise, file_name, output_dir + "moods_year_wise_count" + suffix,
                                     chunk_number)
        write_to_file_pickle(mood_year_wise_word_wise, file_name + "count_mood_word_wise",
                             output_dir + "moods_year_wise_count" + suffix, chunk_number)
        print(fpath)

        # if not parallel, then it is single file, we write it here
        if not isparallel:
            moods_output_pickle_to_excel(fpath, os.path.join(os.path.dirname(fpath), "moods_year_wise_count.xlsx"))

    return file_name, chunk_number, len(lines), params


def moods_output_pickle_to_excel(input_file, output_file):
    moods = pd.read_pickle(input_file)
    df = pd.DataFrame.from_dict(moods, orient='index')
    df = df.sort_index(axis=1)
    df.fillna(0, inplace=True)
    df.to_excel(output_file)
    df.to_csv("{}.csv".format(output_file))


def the_islamic_counts(islamic_words, line, mood_year_wise, ngrams, year, pos_check):
    if 'the' or 'muslim' or 'islam' in line:

        mood_year_wise[year]['the'] = 0
        mood_year_wise[year]['islam'] = 0

        the_count = 0
        islamic_count = 0
        for ngram in ngrams[0:5]:
            if pos_check:
                if pos_in_word(ngram):
                    continue
            if 'the' == ngram:
                the_count = the_count + 1
            else:
                for isl in islamic_words:
                    if isl == ngram:
                        islamic_count = islamic_count + 1

        if the_count > 0:
            mood_year_wise[year]['the'] = mood_year_wise[year]['the'] + the_count
        if islamic_count > 0:
            mood_year_wise[year]['islam'] = mood_year_wise[year]['islam'] + the_count


def pos_in_word(w):
    pos = ['adj', 'noun', 'adp', 'adv', 'conj', 'det', 'num', 'pron', 'prt', 'verb']
    for p in pos:
        if p in w:
            pos_word = w.split('_')
            for pow in pos_word:
                if p == pow:
                    return True
    return False


def write_to_file_pickle(data, file_name, output_dir, chunk_number):
    if len(data) == 0:
        return ""

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
    return path


def write_to_file_text(data, file_name, output_dir, chunk_number):
    if len(data) == 0:
        return ""

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
    return path


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
    parser.add_argument('--can_check_pos', type=str, required=True)

    args = parser.parse_args()
    main(args.num_workers, args.chunk_size, args.no_of_file_in_parallel, args.input_dir, args.output_dir,
         args.moods_dir, args.can_check_pos)


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


def main(num_workers, chunk_size, no_of_file_in_parallel, input_dir, output_dir, moods_input_dir, can_check_pos):
    print("{} workers".format(num_workers))
    if not os.path.exists(output_dir):
        print("output will be here {}", output_dir)
        os.makedirs(output_dir)
    # for each input file
    files = os.listdir(input_dir)
    file_worker = no_of_file_in_parallel

    print("preparing moods dictionary from {}".format(moods_input_dir))
    moods_dict = prepare_moods_dict(moods_input_dir, output_dir)

    write_text_to_file(str(moods_dict), os.path.join(output_dir, "moods_dict.txt"))
    write_dict_as_pickle(moods_dict, os.path.join(output_dir, "moods_dict.pkl"))

    if (num_workers <= 1) or (chunk_size == 0):
        print("programe is running in single thread mode")
        for file_name in files:
            print("processing file ".format(file_name))
            process_file_no_parallel(0, file_name, input_dir, 1, output_dir, moods_dict, can_check_pos)
    else:
        file_futures = []
        with ProcessPoolExecutor(max_workers=file_worker) as fe:
            for file_name in files:
                file_future = fe.submit(process_file, chunk_size, file_name, input_dir, num_workers, output_dir,
                                        moods_dict, can_check_pos)
                file_futures.append(file_future)
            file_results = [f.result() for f in file_futures]
            # print(file_results)
            # process_file(chunk_size, file_name, input_dir, num_workers, output_dir)


def prepare_moods_dict(moods_input_dir, output_dir):
    moods_dict = []
    for f in os.listdir(moods_input_dir):
        paath = os.path.join(moods_input_dir, f)
        print("reading dictionary {}".format(paath))
        if os.path.isfile(paath):
            (fn, ext) = os.path.splitext(f)
            print("preparing mood dict for mood type {}".format(fn))
            with open(paath) as ff:
                emotions = {}
                mood_l = ff.readline().split()
                mood_l = list(set(mood_l))
                emotions[fn] = mood_l
                moods_dict.append(emotions)
                print("{} has {} words".format(fn, len(mood_l)))

    dict_duplicate, lst_duplicates = find_duplicate_moods_actross_all_emotions(moods_dict)
    print("{} been duplicate removed from dict", len(lst_duplicates))
    write_dict_as_pickle(dict_duplicate, os.path.join(output_dir, "duplicate_moods_across_all_moods.pickle"))
    write_dict_as_pickle(list(lst_duplicates), os.path.join(output_dir, "duplicate_moods_words_list.pickle"))

    # removing duplicate moods from all emotions
    uniq_dict = {}
    for i in range(0, len(moods_dict)):
        dic1 = moods_dict[i]
        for k, v in dic1.items():
            for d in lst_duplicates:
                try:
                    v.remove(d)
                except ValueError:
                    pass

        uniq_dict[k] = set(v)

    return uniq_dict


def find_duplicate_moods_actross_all_emotions(moods_dict):
    duplicate_cross_moods = {}
    all_duplicate_words = set()
    for x in range(0, len(moods_dict)):
        dic1 = moods_dict[x]
        for y in range(0, len(moods_dict)):
            dp = []
            if x == y:
                continue
            dic2 = moods_dict[y]

            for k, v in dic1.items():
                for a, b in dic2.items():
                    match = set(v) & set(b)

                    if (len(match) > 0):
                        dp.append(match)
                        all_duplicate_words.update(match)
            duplicate_cross_moods["{}_{}".format(k, a)] = dp

    return duplicate_cross_moods, all_duplicate_words


def process_file(chunk_size, file_name, input_dir, num_workers, output_dir, moods_dict, can_check_pos):
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
            lines = get_lines(file,40000)  # list(islice(file, chunk_size))
            print("total lines are {}".format(len(lines)))
            # batch_size = int(chunk_size / num_workers)
            chunked_data = chunks(lines, chunk_size)
            with ProcessPoolExecutor(max_workers=num_workers) as e:
                for data in chunked_data:
                    print("{} chunk started in parallel".format(chunk_number))
                    # split_data_by_word(file_name, moods_dict, can_check_pos, output_dir, lines, 0,False,("started at {}".format(current_time()), 0))
                    future = e.submit(split_data_by_word, file_name, moods_dict, can_check_pos, output_dir, data,
                                      chunk_number, True, ("started at {}".format(current_time()), chunk_number))
                    future.add_done_callback(callback_function)
                    futures.append(future)
                    chunk_number = chunk_number + 1

        results = [f.result() for f in futures]
        suffix = '_with_out_pos'
        if "yes" in can_check_pos:
            suffix = '_with_pos'
        print("ouput folder {}".format(output_dir))
        temp_dir = os.path.join(output_dir , "moods_year_wise_count" + suffix,"words_ds_is_mu")
        print("temp folder {}".format(temp_dir))
        output_file = os.path.join(output_dir, "moods_year_wise_count.pickle")
        print("output file  {}".format(output_file))
        #mood year wise
        merge_and_update_counts_dict_from_pickle(temp_dir, output_file)
        moods_output_pickle_to_excel(output_file, os.path.join(output_dir, "moods_year_wise_count.xlsx"))

        #merging dataset
        temp_dir = os.path.join(output_dir, "moods_words_ds" + suffix, "words_ds_is_mu")
        merge_files_in_folder_shutil(temp_dir,output_dir,'dataset'+suffix)
        output_file = os.path.join(output_dir, "moods_word_wise_year_wise_count.pickle")

        # word wise count
        temp_dir = os.path.join(output_dir, "moods_year_wise_count" + suffix, "words_ds_is_mucount_mood_word_wise")
        merge_and_update_counts_nested_dict_from_pickle(temp_dir, output_file)
        moods_output_pickle_to_excel(output_file, os.path.join(output_dir, "moods_word_wise_year_wise_count.xlsx"))
        # total_lns = np.sum(results)  # [count(col) for col in results]
        # print("total lines {}".format(total_lns))
        print("{} finished at {}".format(file_name, datetime.utcnow()))
    except Exception as e:
        traceback.print_exc()

def merge_files_in_folder_shutil(input_floder, output_folder,output_filename):
    outfilename = os.path.join(output_folder, 'all_' + str((int(time.time()))))

    with open(outfilename, 'wb') as outfile:
        for filename in os.listdir(input_floder):
            if filename == outfilename:
                # don't want to copy the output into the output
                continue
            with open(os.path.join(input_floder,filename), 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)
    print("finishd..")


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def merge_and_update_counts_dict_from_pickle(input_folder, ouput_file_name):
    print("in merge dict , input folder "+input_folder)
    pkl_file_list = os.listdir(input_folder)
    #pkl_file_list = os.listdir(os.path.join(input_folder,pkl_file_list[0]))
    print("list dir finished")
    All_dict = Counter({})
    for pkl_file in pkl_file_list:

        pkl_file = os.path.join(input_folder, pkl_file)
        print("pickle file "+pkl_file)
        if os.path.exists(pkl_file):
            with  open(pkl_file, "rb") as pickle_in:
                dict_i = pickle.load(pickle_in)
                for k, v in dict_i.items():
                    All_dict = All_dict + Counter(v)

    with  open(ouput_file_name, "wb") as pickle_out:
        pickle.dump(dict(All_dict), pickle_out)
        pickle_out.close()



def merge_and_update_counts_nested_dict_from_pickle(input_folder, ouput_file_name):
    print("in merge dict , input folder "+input_folder)
    pkl_file_list = os.listdir(input_folder)
    #pkl_file_list = os.listdir(os.path.join(input_folder,pkl_file_list[0]))
    print("list dir finished")
    All_dict = Counter({})
    for pkl_file in pkl_file_list:

        pkl_file = os.path.join(input_folder, pkl_file)
        print("pickle file "+pkl_file)
        if os.path.exists(pkl_file):
            with  open(pkl_file, "rb") as pickle_in:
                dict_i = pickle.load(pickle_in)
                for k, v in dict_i.items():
                    for kk,vv in v.items():
                        All_dict = All_dict + Counter(vv)

    with  open(ouput_file_name, "wb") as pickle_out:
        pickle.dump(dict(All_dict), pickle_out)
        pickle_out.close()


def process_file_no_parallel(chunk_size, file_name, input_dir, num_workers, output_dir, moods_dict, can_check_pos):
    output = []
    try:
        file_name = os.path.join(input_dir, file_name)
        size = get_file_size(file_name)
        print("processing file {}, size {} <=> size{}, started at {}".format(file_name, convert_bytes(size, "MB"),
                                                                             convert_bytes(size, "GB"),
                                                                             datetime.utcnow()))
        chunk_number = 0
        lines = []
        line_number = 0
        with open(file_name) as file:
            lines = get_lines(file)

            res = split_data_by_word(file_name, moods_dict, can_check_pos, output_dir, lines, 0, False,
                                     ("started at {}".format(current_time()), 0))

        print("{} finished at {}".format(file_name, datetime.utcnow()))
    except Exception as e:
        traceback.print_exc()


def get_lines(file, no_of_lines_to_read=0):
    if no_of_lines_to_read == 0:
        return file.readlines()

    lines = []
    line_number = 0
    for line in file:
        lines.append(line)
        line_number = line_number + 1
        if (line_number == no_of_lines_to_read):
            break
    print("no of lines {}".format(line_number))
    return lines


# num_workers, chunk_size, no_of_file_in_parallel, input_dir, output_dir, moods_input_dir, can_check_pos
# main(10, 500000, 10, 'E:/download/proj/n5grm/small_ds/data/moods_ds/', "E:/download/proj/n5grm/small_ds/data/output",     "E:/download/proj/n5grm/small_ds/emotion-lexicon-master/eng_no_brotherhood/", "yes")

if __name__ == '__main__':
    # test_batch_execution('one_file', "E:/download/output_files")#
    # split_data_by_word_test_case()
    start_process()
    # prepare_moods_dict("E:\\download\\proj\\n5grm\\small_ds\\emotion-lexicon-master\\eng_no_brotherhood\\")
    print("in main")
