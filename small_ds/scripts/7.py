

from dask import dataframe as dd
import datetime


def time_exe():
    now = datetime.datetime.now()
    print (now.strftime("%Y-%m-%d %H:%M:%S"))

def chunk_filtering_yearwise_data(data_):
    return data_[(data_[5]>1994) & (data_[5] <2006)]

chunksize = 64000000*1 #64000000 is equl to 64 MB, making it as around 512 mb 


original_files_dir = "E:/download/eng-all-5gram-2012-extracted/7/"
dataset_path = "E:/download/proj/n5grm/small_ds/yearwise/"


import os
start = 1995
end = 2005
step = 1


folderpath = dataset_path


def split_data_year_wise(startyear,stopyear,yearsetp,basepath,fname,dataset):
    print("start time")
    time_exe()
    stopyear= stopyear+1
    for i in range(startyear, stopyear, yearsetp):
        year_dd = dataset[dataset[5]==i]
        path = os.path.join(basepath,str(i),fname)
        if not os.path.exists(path):
            os.makedirs(path)
        print("processing year "+str(i))
        year_dd.to_parquet(path,engine='pyarrow')
        #year_dd.to_csv(path)
    print("finisheed time")
    time_exe()


def process_start():
    for filename in os.listdir(original_files_dir):
        print("file processing started "+ filename)
        from dask import dataframe as dd
        df = dd.read_csv(os.path.join(original_files_dir,filename),
                                sep='\s+',
                                header=None, blocksize=chunksize,error_bad_lines=False,
                                encoding='utf-8',engine='python')
        split_data_year_wise(start,end,step,folderpath,filename,df)

def main():
    print("starting......")
    process_start()

if __name__ == "__main__":
    main()

