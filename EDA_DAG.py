#!/usr/bin/env python
# coding: utf-8

# In[5]:


import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd


# In[7]:


default_arg={
    'owner':'suriya',
    'start_date':dt.datetime(2023,4,14),
    'retries':1,
    'retry_delay':dt.timedelta(minutes=5)
}


# In[ ]:


def clean_scooter():
    df=pd.read_csv('scooter.csv')
    df.drop(columns=['region_id'],inplace=True)
    df.columns=[x.lower() for x in df.columns]
    df['started_at']=pd.to_datetime(df['started_at'],format="%m/%d/%Y %H:%M")
    df.to_csv('cleanscooter.csv')


# In[8]:


def filter_data():
    df=pd.read_csv('cleanscooter.csv')
    from_=df['started_at']>'2019-05-23'
    to=df['started_at']<'2019-06-03'
    startend=pd.DataFrame(df[to & from_])
    startend.to_csv('may23-june3.csv')


# In[ ]:


with DAG('CleanData',default_args=default_args,schedule_interval=timedelta(minutes=5)) as dag:
    cleandata=PythonOperator(task_id='cleandata',python_callable=clean_scooter)
    selectdat=PythonOperator(task_id='filter data',python_callable=filter_data)
    copyfile=BashOperator(task_id='copy file',bash_command='/home/suriya/pract/Python/DATA/data_engineer/Data Engineering with Python/airflow_EDA/may23-june3.csv /home/suriya/Desktop')
cleandata>>selectdat>>copyfile

