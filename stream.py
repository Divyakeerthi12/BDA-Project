#!/usr/bin/env python
# coding: utf-8

# In[3]:


get_ipython().system('pip install pyspark')


# In[5]:


get_ipython().system('pip install nb_black')


# In[6]:


get_ipython().run_line_magic('load_ext', 'nb_black')


# In[7]:


from pyspark.sql import SparkSession


# In[9]:


import pyspark.sql.functions as F


# In[12]:


import pyspark.sql.types as T


# In[14]:


spark = SparkSession.builder.getOrCreate()


# In[15]:


df = spark.read.csv("train.csv", header=True, inferSchema=True)


# In[16]:


df.columns


# In[17]:


df.show(3)


# In[2]:


from pyspark.sql import Row


# In[18]:


from pyspark import SparkContext


# In[24]:


from pyspark.sql.session import SparkSession


# In[34]:


from pyspark.streaming import StreamingContext


# In[35]:


def j(rdd):
    df1=rdd.collect()


# In[36]:


count=0


# In[37]:


def j(rdd):
    df1=rdd.collect()
    if df1!=[]:
        d = json.loads(df1[0])
        print(d)
        dictList= lambda x: d[x]
        df=sc.parallelize(list(map(dictList,d))).map(convert_to_row).toDF(['Dates','Category','Descript','DayOfWeek','PdDistrict','Resolution','Address','X','Y'])
        df.show()
        df1=preprocess(df)
        df1.show()
        mlp(df1)
        mnb(df1)
        kmeans(df1)
        df1.unpersist()


# In[ ]:


sc=SparkContext('local[2]',appName="crime")


# In[38]:


ss=SparkSession(sc)


# In[39]:


ssc=StreamingContext(sc,2)


# In[40]:


dataStream=ssc.socketTextStream('localhost',6100)


# In[41]:


words=dataStream.flatMap(lambda line : line.split('}\}'))


# In[43]:


words.foreachRDD(lambda x:j(x))


# In[ ]:


ssc.start()


# In[ ]:


ssc.awaitTermination(2000)


# In[ ]:


ssc.stop()

