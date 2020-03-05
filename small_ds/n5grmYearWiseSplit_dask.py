

from dask import dataframe as dd
import datetime


def time_exe():
    now = datetime.datetime.now()
    return (now.strftime("%Y-%m-%d %H:%M:%S"))

def chunk_filtering_yearwise_data(data_):
    return data_[(data_[5]>1994) & (data_[5] <2006)]

chunksize = 64000000*8 #64000000 is equl to 64 MB, making it as around 512 mb 


input_dir = "E:/download/eng-all-5gram-2012-extracted/11/"
output_dir = "E:/download/proj/n5grm/small_ds/yearwise/"


import os
start = 1995
end = 2005
step = 1

def add(x,y):
    return x+y

def split_data_year_wise(startyear,stopyear,yearsetp,fname,dataset):
    print("start time {} file {}".format(time_exe(),fname))


    stopyear= stopyear+1
    for i in range(startyear, stopyear, yearsetp):
        year_dd = dataset[dataset[5]==i]
        path = os.path.join(output_dir,str(i),fname)
        if not os.path.exists(path):
            os.makedirs(path)
        print("processing year "+str(i))
        year_dd.to_parquet(path,engine='pyarrow')
        del year_dd
        #year_dd.to_csv(path)
    print("finisheed time {}".format(time_exe()))



def process_start():
    for filename in os.listdir(input_dir):
        print("file processing started "+ filename)
        from dask import dataframe as dd
        dfd = dd.read_csv(os.path.join(input_dir,filename),
                                usecols=[0,1,2,3,4,5],
                                sep='\s+',
                                header=None, blocksize=chunksize,error_bad_lines=False,
                                encoding='utf-8',engine='python')
        split_data_year_wise(start,end,step,filename,dfd)

def main():
    print("starting......")
    print("input path {}".format(input_dir))
    print("output path {}".format(output_dir))
    process_start()




print("hi")
if __name__ == "__main__":
    main()

