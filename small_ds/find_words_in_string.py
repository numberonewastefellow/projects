import argparse
import time
import timeit
from concurrent.futures.process import ProcessPoolExecutor


def words_in_string(word_list, a_string):
    return set(word_list).intersection(a_string.split())

def words_in_string_approch_2(word_list,content):
    vals = content.split()
    for wl in word_list:
        for v in vals:
            return  wl.lower() == v.lower()
    return False

def find_words():
    my_word_list = ['one', 'two', 'three']
    a_string = 'one two three'
    if words_in_string(my_word_list, a_string):
        print('One or more words found!')


def find_words_multi_lines(lines):
    result = []
    my_word_list = ['banking', 'members', 'based', 'hardness','system']
    for line in lines:
        found =  words_in_string(my_word_list, line)
        if len(found)>0:
            #print(found)
            result.append((found,line))

    return result

def find_words_multi_lines_aproch_two(lines):
    result = []
    my_word_list = ['banking', 'members', 'based', 'hardness','system']
    for line in lines:
        found =  words_in_string_approch_2(my_word_list, line)
        if  found:
            #print(found)
            result.append((found,line))

    return result


def chunks(list_values, chunk_size):
    chunk_size = max(1, chunk_size)
    return (list_values[i:i + chunk_size] for i in range(0, len(list_values), chunk_size))



def read_from_file_with_chunked(file_path):

    with open(file_path) as fp:
        results = []
        lines = fp.readlines()

        chunked_data = chunks(lines, 10)
        start = time.time()
        for chunk in chunked_data:
            result= find_words_multi_lines(chunk)
            if(len(result)>0):
                results.append(result)
                #print(result)
    end = time.time()
    #print(results)
    print(f"Runtime of the program is chunked {end - start}")


def read_from_file_with_chunked_approch_two(file_path):

    with open(file_path) as fp:
        results = []
        lines = fp.readlines()

        chunked_data = chunks(lines, 10)
        start = time.time()
        for chunk in chunked_data:
            result= find_words_multi_lines_aproch_two(chunk)
            if(len(result)>0):
                results.append(result)
                #print(result)
    end = time.time()
    #print(results)
    print(f"Runtime of the program is chunked approch two {end - start}")

def read_from_file_with_chunked_parallel(file_path):

    with open(file_path) as fp:
        results = []
        lines = fp.readlines()
        futures = []
        chunked_data = chunks(lines, 10)
        start = time.time()
        with ProcessPoolExecutor(max_workers=10) as e:
            for chunk in chunked_data:
                future = e.submit(find_words_multi_lines, chunk)
                futures.append(future)

            results = [  f.result() for f in futures]
            #print(results)
    end = time.time()
    #print(results)
    print(f"Runtime of the program is parllel {end - start}")

def read_from_file_with_chunked_parallel_approch_two(file_path):

    with open(file_path) as fp:
        results = []
        lines = fp.readlines()
        futures = []
        chunked_data = chunks(lines, 10)
        start = time.time()
        with ProcessPoolExecutor(max_workers=10) as e:
            for chunk in chunked_data:
                future = e.submit(find_words_multi_lines_aproch_two, chunk)
                futures.append(future)

            results = [  f.result() for f in futures]
            #print(results)
    end = time.time()
    #print(results)
    print(f"Runtime of the program is parllel approach two {end - start}")



def read_from_file_no_chunked(file_path):

    with open(file_path) as fp:
        results = []
        lines = fp.readlines()
        start = time.time()
        result= find_words_multi_lines(lines)
        if len(result)>0:
           results.append(result)
    end = time.time()
    #print(results)
    print(f"Runtime of the program is no chunked {end - start}")

def start_process(file_name):

    # test_batch_execution('one_file', "E:/download/output_files")
    read_from_file_with_chunked_approch_two(file_name)
    read_from_file_with_chunked(file_name)
    read_from_file_no_chunked(file_name)
    read_from_file_with_chunked_parallel(file_name)
    read_from_file_with_chunked_parallel_approch_two(file_name)

#start_process(file_name = "E:/download/eng-all-5gram-2012-extracted/8/googlebooks-eng-all-5gram-20120701-jo")

if __name__ == '__main__':
    print("in main")

    global parser, args
    parser = argparse.ArgumentParser(description='Parallel text processor')
    parser.add_argument('--file_name', type=str, required=True)
    args = parser.parse_args()
    file_name = args.file_name
    start_process(file_name=file_name)
