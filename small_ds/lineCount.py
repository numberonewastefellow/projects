import argparse
import os


def file_lengthy(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1




def main(input_dir):
    for file in os.listdir(input_dir):
        count = file_lengthy(file)
        print(count)


def start_process():
    global parser, args
    parser = argparse.ArgumentParser(description='Parallel text processor')
    parser.add_argument('--input_dir', type=str, required=True)
    args = parser.parse_args()
    main( args.input_dir)




if __name__ == '__main__':
    # test_batch_execution('one_file', "E:/download/output_files")
    start_process()
    print("in main")
