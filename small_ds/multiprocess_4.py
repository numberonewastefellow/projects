
import random, string
import time, sys
import datetime
import traceback
import argparse
from io import SEEK_END
import multiprocessing as mp
import os


from fileSplitter import cleavebytes

def write_to_file(fname, ds, output_dir,chunk):
    year = 1995
    for ls in ds:
        dirpath = os.path.join(output_dir, str(year), os.path.basename(fname))
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        filepath = os.path.join(dirpath, "{}_{}".format(year, chunk))
        outF = open(filepath, "w", encoding='utf-8')
        outF.writelines(ls)
        outF.close()
        year = year + 1

def process_lines(proc,fname,output_dir):
    ls_1995_1996 = []
    linecount = 0
    for i in range(1995, 2006):
        ls_1995_1996.append([])
        chunk,reader = proc
        print("chunk {}".format(chunk))
        with reader.open() as src:
            for line in src:
                linecount = linecount +1
                #print(line)
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

        write_to_file(fname, ls_1995_1996, output_dir,chunk)
        return linecount

def writechunk(i, reader):
    with reader.open() as src, open('out{}'.format(i), 'wb') as dst:
        dst.write(src.read())
        return 'Chunk #{} was {} bytes'.format(i, src.end - src.start)

def writechunk_cb(result):
    print(result)



def main(num_workers, input_dir,output_dir):
    print("{} workers".format(num_workers))
    #pool = mp.Pool(processes=num_workers)

    # for each input file
    for fname in os.listdir(input_dir):
        readers = cleavebytes(fname, num_workers * 1024 * 1024)
        tasks = []
        pool = mp.Pool()
        for i, reader in enumerate(readers):
            tasks.append((i, reader))
        linecounts = 0
        for task in tasks:
            # pool.apply_async(writechunk, args=task, callback=writechunk_cb)
            ##results = [pool.apply(summarize, args=(fname, start, stop, output_dir)) for start, stop in start_stops]
            #pool.apply_async(process_lines, args=(task,fname,output_dir), callback=writechunk_cb)
            linecounts += process_lines(task,fname,output_dir)
            print("totl lines ==> {}  in {}".format(fname, linecounts))
        pool.close()
        pool.join()

        #write_to_file(fname,results,output_dir,0,1)

        # collect results
        #results = [sum(col) for col in zip(*results)]
        #for col in zip (*results):
        #    print(col)
        #print(results)



def start_process():
    global parser, args
    parser = argparse.ArgumentParser(description='Parallel text processor')
    parser.add_argument('--num_workers', '-n', default=8, type=int)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--input_dir', type=str, required=True)
    args = parser.parse_args()
    main(args.num_workers, args.input_dir, args.output_dir)



if __name__ == "__main__":
    start_process()
    #main(args.num_workers, args.sam_files)