#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install --upgrade pip


# In[1]:


from pyspark.sql import SparkSession


# In[2]:


from pyspark.context import SparkContext


# In[3]:


from pyspark.sql.functions import * 


# In[4]:


from pyspark.sql.types import * 


# In[5]:


from pyspark.sql import SQLContext


# In[6]:


import sys


# In[7]:


from pyspark.ml.linalg import DenseMatrix, Vectors


# In[8]:


from pyspark.ml.stat import Correlation


# In[9]:


from pyspark.ml.classification import NaiveBayes


# In[10]:


from pyspark.sql.functions import col


# In[11]:


from pyspark.ml.feature import FeatureHasher


# In[12]:


from pyspark.ml.feature import StringIndexer


# In[13]:


from pyspark.sql.functions import abs


# In[14]:


from pyspark.ml import Pipeline


# In[15]:


from pyspark.ml.feature import VectorAssembler, StandardScaler


# In[16]:


from pyspark.ml.feature import MinMaxScaler


# In[17]:


from pyspark.ml.functions import vector_to_array


# In[18]:


from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from telnetlib import Telnet
import time
import json
import pickle
import socket
import argparse
import numpy as np
import pandas as pd


# In[19]:


from tqdm.auto import tqdm
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from collections import OrderedDict
from preprocess import preprocess
from preprocess import mlp
from preprocess import sgd
from preprocess import mnb
from preprocess import kmeans


# In[20]:


import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession
from sklearn.neural_network import MLPClassifier
import os
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


# In[21]:


def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))

count=0


# In[22]:


def j(rdd):
    df1=rdd.collect()
    if df1!=[]:
        
        d = json.loads(df1[0])
        
        dictList= lambda x: d[x]
        df=sc.parallelize(list(map(dictList,d))).map(convert_to_row).toDF(['Dates','Category','Descript','DayOfWeek','PdDistrict','Resolution','Address','X','Y'])  
        
        df1=preprocess(df)
        df1.show()
        mlp(df1)
        sgd(df1)
        mnb(df1)
        kmeans(df1)
        df1.unpersist()


# In[23]:


sc=SparkContext('local[2]',appName="crime")


# In[24]:


ss=SparkSession(sc)


# In[25]:


ssc=StreamingContext(sc,2)


# In[26]:


dataStream=ssc.socketTextStream('localhost',6100)


# In[27]:


words=dataStream.flatMap(lambda line : line.split('}\}'))


# In[28]:


words.foreachRDD(lambda x:j(x))


# In[29]:


ssc.start()


# In[ ]:


ssc.awaitTermination(500)


# In[ ]:


ssc.stop()


# In[ ]:




