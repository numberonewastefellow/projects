#!/usr/bin/env python
# coding: utf-8

# In[1]:


from dask import dataframe as dd


# In[2]:


get_ipython().run_cell_magic('time', '', 'import datetime\nimport os\nfrom tqdm import tqdm\n')


# In[3]:



get_ipython().run_cell_magic('time', '', 'def time_exe():\n    now = datetime.datetime.now()\n    print (now.strftime("%Y-%m-%d %H:%M:%S"))')


# In[4]:


time_exe()


# In[5]:


def chunk_filtering_yearwise_data(data_):
    return data_[(data_[5]>1994) & (data_[5] <2006)]


# In[6]:



get_ipython().run_cell_magic('time', '', "onemb ='googlebooks-eng-all-5gram-20120701-xq'")


# In[7]:


dfd = dd.read_csv(onemb,
                    usecols=[0,1,2,3,4,5],
                            sep='\s+',
                            header=None, blocksize=64000000,error_bad_lines=False,
                            encoding='utf-8',engine='python')


# In[8]:


dfd.head()


# In[9]:


#dfd[5].isna().sum().compute()


# In[10]:


dfd[5].value_counts().compute()


# In[11]:


import matplotlib.pyplot as plt
import seaborn as sns


# In[12]:


dfd95_2005 = dfd[(dfd[5]>1994) & (dfd[5]<2006)]


# In[13]:


year_count  = dfd95_2005[5].value_counts().compute()


# In[14]:



#city_count = city_count[:10,]
plt.figure(figsize=(25,5))
sns.barplot(year_count.index, year_count.values, alpha=0.8)
plt.title('Words count year wise')
plt.ylabel('Number of Words', fontsize=12)
plt.xlabel('Years', fontsize=12)
plt.show()


# In[15]:


#df.drop([c for c in df.columns if df[c].isna().any().compute()], axis = 1)
#dfd[0]=dfd[0].str.lower()


# In[16]:


#df.x.value_counts()
#dfd[5].value_counts().compute()


# In[18]:


dfd.tail()


# In[19]:


start = 1995
end = 2005
step = 1


# In[20]:


import os


# In[ ]:


ls


# In[22]:


folderpath = 'yearwise'


# In[ ]:





# In[ ]:





# # writnd 1995 to 2005 in single parque

# year_dd.shape[0].compute()

# In[23]:


year_dd = dfd[ (dfd[5]>1994)&(dfd[5]<2006)]


# In[24]:


path = os.path.join(folderpath,"1995_2005",onemb)
print(path)


# In[25]:


year_dd.to_parquet(path,engine='pyarrow')


# # reading single parquet file

# In[26]:


read_year_dd = dd.read_parquet(path, engine='pyarrow')


# In[27]:


#convert dask data frame to pandas data frame
read_year_ddf = read_year_dd.compute()


# # plot graph after eading data from parquet

# In[28]:


read_year_ddf.head()


# for index, row in read_year_ddf.iterrows(): 
#     print (row[0], row[5]) 

# for i in range(len(read_year_ddf)) : 
#   print(read_year_ddf.iloc[i, 0], read_year_ddf.iloc[i, 2])

# In[ ]:





# In[29]:


read_year_ddf.iloc[:,5].value_counts()


# In[30]:


read_year_count = read_year_ddf.iloc[:,5].value_counts()
plt.figure(figsize=(25,5))
sns.barplot(read_year_count.index, read_year_count.values, alpha=0.8)
plt.title('Words count year wise')
plt.ylabel('Number of Words', fontsize=12)
plt.xlabel('Years', fontsize=12)
plt.show()


# year_dd.to_parquet('googlebooks-eng-all-5gram-20120701-co_1995_2005_pyarrow',engine='pyarrow')

# In[31]:


def split_data_year_wise(startyear,stopyear,yearsetp,basepath):
    print("start time")
    time_exe()
    stopyear= stopyear+1
    for i in range(startyear, stopyear, yearsetp):
        year_dd = dfd[dfd[5]==i]
        path = os.path.join(basepath,str(i),onemb)
        if not os.path.exists(path):
            os.makedirs(path)
        print("{}=>{}".format(i,path))
        year_dd.to_parquet(path,engine='pyarrow')
        #year_dd.to_csv(path)
        del year_dd
    print("finisheed time")
    time_exe()


# def split_data_year_wise(startyear,stopyear,yearsetp,basepath):
#     print('im in')
#     print(startyear)
#     print(stopyear)
#     for i in range(startyear, stopyear, yearsetp):
#         print(i)

# In[32]:


split_data_year_wise(start,end,step,folderpath)


# In[ ]:





# In[33]:


year_dd.head()


# In[ ]:





# # REading parquet file

# In[34]:


from os import listdir
from os.path import isfile, join


# In[36]:


import os
dirs = []
for dirpath, dirs, files in os.walk(folderpath):
    if(dirpath


# In[37]:



for()
read_dfd = dd.read_parquet('yearwise/1995/googlebooks-eng-all-5gram-20120701-xq', engine='pyarrow')


# In[ ]:


import pandas as pd


# In[ ]:


df1= read_dfd.compute()


# In[ ]:


df1.head()


# In[ ]:





#  year_dd = dd[dd[5]==1996]

# year_dd.head()

# dd.tail()

# In[ ]:


get_ipython().system('jupyter nbconvert --to script n5grmYearWiseSplit_dask_working.ipynb')


# dd.shape[0].compute()

# top_links_grouped_dask = dfd.loc[
#     dfd[‘referrer_type’].isin([‘link’]), 
#     [‘coming_from’,’article’, ‘n’]]\
#         .groupby([‘coming_from’, ‘article’])

# #d1 = dd.loc[dd['0']].groupby(['5'])
# dd.groupby([5])[6].count().compute()
