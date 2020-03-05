import os, multiprocessing as mp
import random, string
import time, sys
import datetime
import traceback
import argparse
from io import SEEK_END
import os


def write_to_file(fname, ds, output_dir, start, stop):
    year = 1995
    for ls in ds:
        dirpath = os.path.join(output_dir, str(year), os.path.basename(fname))
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        filepath = os.path.join(dirpath, "{}_{}_{}".format(year, start, stop))
        outF = open(filepath, "w", encoding='utf-8')
        outF.writelines(ls)
        outF.close()
        year = year + 1


def write_to_Single_file(fname, ds, output_dir,start,stop):
    filepath = os.path.join(output_dir, os.path.basename(fname)+"_generated"+str(start)+"_"+str(stop)+".txt")
    outF = open(filepath, "w", encoding='utf-8')
    outF.writelines(ds)
    outF.close()


# process file function
def processfile(filename,  output_dir, start=0, stop=0):
    count = 0
    ls_1995_1996 = []
    for i in range(1995, 2006):
        ls_1995_1996.append([])

    if start == 0 and stop == 0:
        print('... process entire file...')
    else:
        with open(filename, 'r') as fh:
            fh.seek(start)
            lines = fh.readlines(stop - start)
            for line in lines:
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

        #write_to_file(fname, ls_1995_1996, output_dir, start, stop)
        write_to_Single_file(filename,lines,output_dir,start,stop)
    return len(lines)


if __name__ == "__main__":
    output_dir = "E:/download/proj/n5grm/small_ds/multiproc_output"
    filename = "E:/download/test/output/googlebooks-eng-all-5gram-20120701-np"
    # get file size and set chuck size
    filesize = os.path.getsize(filename)
    cpu_count = 8
    split_size = 1024 #2 * 1024 * 1024

    print("split size {}".format(split_size))
    print("filesize size {}".format(filesize))


    # determine if it needs to be split
    if filesize > split_size:

        # create pool, initialize chunk start location (cursor)
        pool = mp.Pool(cpu_count)
        cursor = 0
        results = []
        with open(filename, 'r') as fh:

            # for every chunk in the file...
            for chunk in range(filesize // split_size):

                # determine where the chunk ends, is it the last one?
                if cursor + split_size > filesize:
                    end = filesize
                else:
                    end = cursor + split_size

                # seek to end of chunk and read next line to ensure you 
                # pass entire lines to the processfile function
                fh.seek(end)
                fh.readline()

                # get current file location
                end = fh.tell()

                # add chunk to process pool, save reference to get results
                proc = pool.apply_async(processfile, args=[filename, output_dir,cursor, end])
                results.append(proc)

                # setup next chunk
                cursor = end

        # close and wait for pool to finish
        pool.close()
        pool.join()

        # iterate through results
        total_lines = 0
        for proc in results:
            processfile_result = proc.get()
            total_lines =total_lines+ processfile_result;
        print(total_lines)

    else:
        print("small file it iwl never executed...")
