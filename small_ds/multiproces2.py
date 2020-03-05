import random, string
import time, sys
import datetime
import traceback
import argparse
from io import SEEK_END
import multiprocessing as mp
import os

#
# Worker process
#
def summarize(fname, start, stop,output_dir):
    """
    Process file[start:stop]

    start and stop both point to first char of a line (or EOF)
    """
    ls_1995_1996 = []
    for i in range (1995,2006):
        ls_1995_1996.append([])

    with open(fname, newline='', encoding='utf-8') as inf:
        # jump to start position
        pos = start
        inf.seek(pos)

        for line in inf:
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

            pos += len(line)
            if pos >= stop:
                break
        write_to_file(fname, ls_1995_1996, output_dir, start, stop)
    return ls_1995_1996



def splitFileInChunk(fname, start, stop,output_dir):
    """
    Process file[start:stop]

    start and stop both point to first char of a line (or EOF)
    """
    lastline = ""
    lines = []
    with open(fname, newline='', encoding='utf-8') as inf:
        # jump to start position
        pos = start
        inf.seek(pos)

        for line in inf:
            sys.stdout.write("\r" + random.choice(string.ascii_letters))
            sys.stdout.flush()
            lines.append(line)
            pos += len(line)
            if pos >= stop:
                lastline = line
                break
        write_to_Single_file(fname, lines, output_dir)
    return lastline


def write_to_Single_file(fname, ds, output_dir):
    filepath = os.path.join(output_dir, os.path.basename(fname)+"_generated")
    outF = open(filepath, "a", encoding='utf-8')
    outF.writelines(ds)
    outF.close()

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

def log(msg,fl):
    fl.write("{}\n".format(msg))

def error_log(msg,efl):
    efl.write("{}\n".format(msg))
#python multiproces2.py --num_workers 100 --output_dir multiproc_output/ --input_dir /data/eng-all-5gram-2012-extracted/10/

def main(num_workers, input_dir,output_dir):
    hs = open("log.txt", "a")
    error_file = open("error_log.txt", "a")
    print("{} workers".format(num_workers))
    pool = mp.Pool(processes=num_workers)

    # for each input file
    for fname in os.listdir(input_dir):
        try:
            print(fname)
            log("{},{}".format(datetime.datetime.now(),fname),hs)
            fname = os.path.join(input_dir,fname)
            print("{} processing started".format(fname))
            # decide how to divide up the file
            with open(fname, encoding='utf-8') as inf:
                # get file length
                inf.seek(0, SEEK_END)
                f_len = inf.tell()
                print("file lenght {}".format(f_len))
                # find break-points
                starts = [0]
                for n in range(1, num_workers):
                    # jump to approximate break-point
                    inf.seek(n * f_len // num_workers)
                    # find start of next full line
                    inf.read()
                    # store offset
                    starts.append(inf.tell())

            stops = starts[1:] + [f_len]
            start_stops = zip(starts, stops)
            print("Solving {}".format(fname))
            #results = [pool.apply(summarize, args=(fname, start, stop, output_dir)) for start, stop in start_stops]
            #results = [pool.apply(splitFileInChunk, args=(fname, start, stop, output_dir)) for start, stop in start_stops]
            for start, stop in  start_stops:

                lastline = splitFileInChunk(fname, start, stop, output_dir)
                print(lastline)
            print(fname + " finished")
        except Exception as e:
            traceback.print_exc()
            error_log("error"+e,error_file)

    error_file.close()
    hs.close()
        #write_to_file(fname,results,output_dir,0,1)

        # collect results
        #results = [sum(col) for col in zip(*results)]
        #for col in zip (*results):
        #    print(col)
        #print(results)


main(50, 'E:/download/test/output',"multiproc_output")


def start_process():
    global parser, args
    parser = argparse.ArgumentParser(description='Parallel text processor')
    parser.add_argument('--num_workers', '-n', default=8, type=int)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--input_dir', type=str, required=True)
    args = parser.parse_args()
    main(args.num_workers, args.input_dir, args.output_dir)


#if __name__ == '__main__':
#if __name__ == "__main__":
#    start_process()
    #main(args.num_workers, args.sam_files)