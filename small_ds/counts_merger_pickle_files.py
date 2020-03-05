import argparse
from concurrent.futures.process import ProcessPoolExecutor
import shutil

from pickle_reading_writing import PickleReadWrite
import os
import pandas as pd


def read_pickles_from_dir(input_folder):
    files = os.listdir(input_folder)
    pkl = PickleReadWrite()
    output = pkl.read_from_dir(files)
    return output


def merge_dict_to_data_frame(lst_of_dict):
    return pd.DataFrame(lst_of_dict)


def data_frame_to_csv(data_frame, file_path):
    data_frame.to_csv(file_path)


def data_frame_to_pickle(data_frame, file_path):
    pkl = PickleReadWrite()
    pkl.write_pickle_to_data_frame(file_path, data_frame)


def merge_text_files_to_another_file(input_folder, output_file_name):
    if not os.path.isdir(input_folder):
        return True

    print("processing folder " + input_folder)
    files = os.listdir(input_folder)
    with open(output_file_name, 'w') as outfile:
        for fname in files:
            with open(os.path.join(input_folder, fname)) as infile:
                for line in infile:
                    outfile.write(line)
    return True


def merge_pickles_to_another_pickle(input_folder, output_folder, output_file_name):
    print("input folder {}".format(input_folder))
    print("outupt folder {}".format(output_folder))
    print("outupt folder {}".format(output_file_name))
    files = os.listdir(input_folder)
    pkl = PickleReadWrite()
    pcikles = pkl.read_list_of_pickles(files, input_folder)

    path = os.path.join(output_folder, "pickle", "{}".format(output_file_name))
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    pkl.write_list_dict_to_dict("{}.dict.pickle".format(path), pcikles)

    df = merge_dict_to_data_frame(pcikles)
    path = os.path.join(output_folder, "dataframe", "{}".format(output_file_name))
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    pkl.write_pickle_to_data_frame("{}.dataframe.pickle".format(path), df)

    path = os.path.join(output_folder, "csv", "{}".format(output_file_name))
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    pkl.write_pickle_to_data_frame_to_csv("{}.dataframe.csv".format(path), df)


def merge_pickle_files_in_each_dir(input_folder, output_folder, num_workers):
    print("running merge pickle  files")
    list_subfolders_with_paths = [f.path for f in os.scandir(input_folder) if f.is_dir()]
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    file_futures = []
    with ProcessPoolExecutor(max_workers=num_workers) as fe:
        for folder in list_subfolders_with_paths:
            if os.path.isdir(folder):
                file_future = fe.submit(merge_pickles_to_another_pickle, folder, output_folder,
                                        os.path.basename(folder))
                file_futures.append(file_future)
        file_results = [f.result() for f in file_futures]


def merge_pickle_files_in_each_dir_no_parallel(input_folder, output_folder, num_workers):
    print("running merge pickle  files no parallel")
    list_subfolders_with_paths = [f.path for f in os.scandir(input_folder) if f.is_dir()]
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    for folder in list_subfolders_with_paths:
        if os.path.isdir(folder):
            print("reading pkls from {}".format(folder))
            merge_pickles_to_another_pickle(folder, output_folder, os.path.basename(folder))
    return True


def merge_text_files_in_each_dir_no_parallel(input_folder, output_folder, num_workers):
    print("running merge text files")
    list_subfolders_with_paths = [f.path for f in os.scandir(input_folder) if f.is_dir()]
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

        for folder in list_subfolders_with_paths:
            if os.path.isdir(folder):
                merge_text_files_to_another_file(folder, os.path.join(output_folder,
                                                                      "merged_{}".format(os.path.basename(folder))))


def merge_text_files_in_each_dir(input_folder, output_folder, num_workers):
    print("running merge text files")
    list_subfolders_with_paths = [f.path for f in os.scandir(input_folder) if f.is_dir()]
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    file_futures = []
    with ProcessPoolExecutor(max_workers=num_workers) as fe:
        for folder in list_subfolders_with_paths:
            if os.path.isdir(folder):
                file_future = fe.submit(merge_text_files_to_another_file, folder,
                                        os.path.join(output_folder, "merged_{}".format(os.path.basename(folder))))
                file_futures.append(file_future)
        file_results = [f.result() for f in file_futures]


def merge_text_files(input_dir, output_dir, num_workers):
    merge_text_files_in_each_dir(input_dir, output_dir, num_workers)
    pass


def merge_picke_files(input_dir, output_dir, num_workers):
    merge_pickle_files_in_each_dir_no_parallel(input_dir, output_dir, num_workers)
    pkl = PickleReadWrite()
    # merge_pickles_to_another_pickle(os.path.join(output_dir,"pickle"), output_dir,"all")
    in_dir = os.path.join(output_dir, "pickle")
    ouput_file = os.path.join(output_dir, "all.pkl")
    print("Creating full picke file in this dir {} with file name {} ".format(in_dir, ouput_file))
    pkl.merge_and_update_counts_dict_from_pickle(in_dir, ouput_file)
    # pkl.merge_and_update_counts_dict_from_pickle(os.path.join(output_dir, "dataframe", "all.dataframe.pickle"), os.path.join(output_dir, "all.dataframe.pickle"))
    # pkl.merge_and_update_counts_dict_from_pickle(os.path.join(output_dir, "pickle", "all.dict.pickle"), os.path.join(output_dir, "all.dict.pickle"))


def moveFiles(source, dest):
    shutil.move(source, dest)


def main(num_workers, input_dir, output_dir, mode):
    print("{} workers".format(num_workers))
    # output_dir = os.path.join(output_dir, "1995_2005_ds")
    if not os.path.exists(output_dir):
        print("output will be here {}", output_dir)
        os.makedirs(output_dir)

    if "text" in mode:
        with ProcessPoolExecutor(max_workers=num_workers) as px:
            px.submit(merge_text_files, input_dir, output_dir, num_workers)
    if "pickle" in mode:
        with ProcessPoolExecutor(max_workers=num_workers) as pp:
            pp.submit(merge_picke_files, input_dir, output_dir, num_workers)


def main_no_parallel(num_workers, input_dir, output_dir, mode):
    print("running in single thred")
    if not os.path.exists(output_dir):
        print("output will be here {}", output_dir)
        os.makedirs(output_dir)

    if "text" in mode:
        merge_text_files(input_dir, output_dir, num_workers)
    if "pickle" in mode:
        merge_picke_files(input_dir, output_dir, num_workers)


def start_process():
    print("--mode pickle or text")
    global parser, args
    parser = argparse.ArgumentParser(description='Parallel text processor')
    parser.add_argument('--num_workers', '-n', default=8, type=int)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--input_dir', type=str, required=True)
    parser.add_argument('--mode', type=str, required=True)
    args = parser.parse_args()
    main_no_parallel(args.num_workers, args.input_dir, args.output_dir, args.mode)


# merge_text_files_in_each_dir("E:\\DataScience\\projects\\nlp\\5grm\\data\\ngrm_txt",  "E:\\DataScience\\projects\\nlp\\5grm\\data\\merged_txt_files")

# merge_pickle_files_in_each_dir_test("E:\\DataScience\\projects\\nlp\\5grm\\data\\ngrm_pickle",  "E:\\DataScience\\projects\\nlp\\5grm\\data\\merged_pkl_files")

# merge_picke_files("E:\\download\\counts\\the_year_wise_count", "E:\\download\\counts\\the_year_wise_merged_2", 0)

# merge_pickles_to_another_pickle()

#merge_picke_files("", "E:\\download\\counts\\words_year_wise_merged\\", 0)

# merge_pickles_to_another_pickle()

#merge_picke_files("", "E:\\download\\counts\\words_year_wise_merged\\", 0)

if __name__ == '__main__':
    start_process()
