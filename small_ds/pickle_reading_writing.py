import pickle
import os
from collections import Counter

import pandas as pd

class PickleReadWrite:

    def write_to_pickle(self, file_name: str, content: object) -> bool:
        with open(file_name, 'wb') as handle:
            pickle.dump(content, handle, protocol=pickle.HIGHEST_PROTOCOL)
        return True

    def read_from_pickle(self, file_name: str) -> object:
        with open(file_name, 'rb') as handle:
            return pickle.load(handle)

    def read_list_of_pickles(self, lst_of_files: list, folder) -> list:
        content = {}
        for f in lst_of_files:
            with open(os.path.join(folder, f), 'rb') as handle:
                data = pickle.load(handle)
                content[f] = data
        return content

    def write_pickle_to_data_frame(self, file_path: str, data_frame: pd.DataFrame) -> bool:
        data_frame.to_pickle(file_path, protocol=pickle.HIGHEST_PROTOCOL)
        return True

    def write_list_dict_to_dict(self, file_nme, dic):
        self.write_to_pickle(file_nme, dic)

    def merge_and_update_counts_dict_from_pickle(self,input_folder, ouput_file_name):
        pkl_file_list = os.listdir(input_folder)
        All_dict = Counter({})
        for pkl_file in pkl_file_list:
            pkl_file = os.path.join(input_folder, pkl_file)
            if os.path.exists(pkl_file):
                print("updaitng pkl counts from files {}".format(pkl_file))
                with  open(pkl_file, "rb") as pickle_in:
                    dict_i = pickle.load(pickle_in)
                    for k, v in dict_i.items():
                        All_dict = All_dict + Counter(v)

        with  open(ouput_file_name, "wb") as pickle_out:
            print("Writing all pick dic {}".format(ouput_file_name))
            pickle.dump(dict(All_dict), pickle_out)
            pickle_out.close()

    def add(cls):
        pass

    def write_pickle_to_data_frame_to_csv(self, file_name, df: pd.DataFrame):
        df.to_csv(file_name)
