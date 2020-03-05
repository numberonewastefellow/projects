import argparse
import os

import pandas as pd

file_name = 'E:\\download\\moods_ds_output_without_pos\\moods_year_wise_count\\words_ds_is_mu\\2020-07-14 105257.447.part.0.pickle'
output_filename = 'E:\\download\\moods_ds_output_without_pos\\moods_year_wise_count\\words_ds_is_mu\\moods_counts_output_without_pos.xlsx'


def moods_count_output_to_excel(input_folder):
    for file in os.listdir(input_folder):
        moods = pd.read_pickle(file)
        df = pd.DataFrame.from_dict(moods, orient='index').transpose()
        df1 = df.sort_index(axis=1)
        df1.fillna(0, inplace=True)
        df.to_excel(output_filename)


def start_process():
    global parser, args
    parser = argparse.ArgumentParser(description='Parallel text processor')

    parser.add_argument('--input_dir', type=str, required=True)
    args = parser.parse_args()
    moods_count_output_to_excel( args.input_dir)


if __name__ == '__main__':
    # test_batch_execution('one_file', "E:/download/output_files")#
    #split_data_by_word_test_case()
    start_process()
    print("in main")
