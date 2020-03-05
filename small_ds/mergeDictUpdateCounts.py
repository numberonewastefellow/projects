import os
import pickle
from collections import Counter



def merge_and_update_counts_dict_from_pickle(input_folder, ouput_file_name):
    pkl_file_list = os.listdir(input_folder)
    All_dict = Counter({})
    for pkl_file in pkl_file_list:
        pkl_file = os.path.join(input_folder,pkl_file)
        if os.path.exists(pkl_file):
            with  open(pkl_file, "rb") as pickle_in:
                dict_i = pickle.load(pickle_in)
                for k,v in dict_i.items():
                    All_dict = All_dict + Counter(v)

    with  open(ouput_file_name, "wb") as pickle_out:
        pickle.dump(dict(All_dict), pickle_out)
        pickle_out.close()

def test_case():
    dict1 = {'a': 4, 'b': 3, 'c': 0, 'd': 4}
    dict2 = {'a': 1, 'b': 8, 'c': 5}
    # just creating two pickle files:
    pickle_out = open("dict1.pickle", "wb")
    pickle.dump(dict1, pickle_out)
    pickle_out.close()
    pickle_out = open("dict2.pickle", "wb")
    pickle.dump(dict2, pickle_out)
    pickle_out.close()
    # Here comes:
    pkl_file_list = ["dict1.pickle", "dict2.pickle"]
    All_dict = Counter({})
    for pkl_file in pkl_file_list:
        if os.path.exists(pkl_file):
            pickle_in = open(pkl_file, "rb")
            dict_i = pickle.load(pickle_in)
            All_dict = All_dict + Counter(dict_i)
    print(dict(All_dict))
    with  open("All_dict.pickle", "wb") as pickle_out:
        pickle.dump(dict(All_dict), pickle_out)
        pickle_out.close()

merge_and_update_counts_dict_from_pickle("E:\\download\\counts\\words_year_wise_merged\\pickle","E:\\download\\counts\\words_year_wise_merged\\all.pkl")
#test_case()