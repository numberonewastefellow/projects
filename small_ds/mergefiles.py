import random, string
import time, sys
import datetime
import traceback
import argparse
from io import SEEK_END
import multiprocessing as mp
import os

import time, glob, shutil


import shutil




def mergeFiles(input_floder, output_folder, output_filename):
    with open(os.path.join(output_folder, output_filename), 'w') as outfile:
        for fname in os.listdir(input_floder):
            with open(os.path.join(input_floder, fname), 'r') as infile:
                # for line in infile:
                outfile.writelines(infile.readlines())
    print("finishd..")




def merge_files_in_folder_shutil(input_floder, output_folder,output_filename):
    outfilename = os.path.join(output_folder, 'all_' + str((int(time.time()))))

    with open(outfilename, 'wb') as outfile:
        for filename in os.listdir(input_floder):
            if filename == outfilename:
                # don't want to copy the output into the output
                continue
            with open(filename, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)
    print("finishd..")

# mergeFiles('E:/download/proj/n5grm/small_ds/multiproc_output','E:/download/proj/n5grm/small_ds/','output_merged.txt')
