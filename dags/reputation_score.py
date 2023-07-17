import pandas as pd
import numpy as np
import os
import graphscope

from sklearn import preprocessing
from tqdm import tqdm
from scipy.stats import kendalltau

import graphscope.nx as nx

min_max_scaler = preprocessing.MinMaxScaler()

sess=graphscope.session()

# data_path need to load into vinyeard
data_df = pd.read_csv(data_path)
data_df['gas_used']=data_df['gas_used']*data_df['gas_price']

def logistic(x,
             a=2,
             b=1 / 64):
    return a / (1.0 + np.exp(-b * x))

def normalize(df):
    normalized_df=(df-df.min())/(df.max()-df.min())
    return normalized_df

print('initally',len(data_df),'txns')

data_df = data_df[(data_df['from_address']!=data_df['to_address'])]# remove loop
print('after removing loop',len(data_df),'txns')

#data_df = data_df.drop_duplicates(subset=['block_number','from_address','to_address'])# remove duplicate 
data_df = data_df.drop_duplicates(subset=['from_address','to_address'], keep='last')# remove duplicate 

print('for all txns with same address(from and to), only take the lastest one:',len(data_df),'txns')

data_df=data_df.dropna(subset=['from_address'])
print('after remove nan in from_address',len(data_df))

data_df=data_df.dropna(subset=['to_address'])
print('after remove nan in to_address',len(data_df))


activate_time=90
blockperday_set=28800 # ~ 2 second to generate each block
block_min=data_df['block_number'].min()
block_max=data_df['block_number'].max()
print(block_min,block_max)

data_df['block_date']=((data_df['block_number']-block_min-activate_time)/blockperday_set).astype(int) 

a=1
b=1/64
data_df['age_weight']=a / (1.0 + np.exp(-b * data_df['block_date']))

G = nx.DiGraph()
G=nx.from_pandas_edgelist(data_df, 'from_address', 'to_address', create_using=nx.DiGraph, edge_attr='age_weight')
print('total',G.number_of_nodes(),'nodes and',G.number_of_edges())

# data processing df_data_remove_loop
# extract features
to_weights_df = data_df[['to_address', 'age_weight']]

# group receiver
node_age_weights = to_weights_df.groupby('to_address').sum()

# sum calculation
# sum of weights
s_weights = float(node_age_weights["age_weight"].sum())

# personalized dict
personalized_weights = {k: v/s_weights for k, v in node_age_weights["age_weight"].items()}

pr = nx.pagerank(G,
                 alpha=0.5,
                 personalization=personalized_weights,
                 #personalization=in_degree_dict,
                 weight='age_weight')

pr_col_name='Reputaiton score (x10e6)'
pr_col_name_log='Reputation (Log Scaled)'
pr_df=pd.DataFrame(pr.items(),columns=['Address',pr_col_name])
pr_df[pr_col_name]=pr_df[pr_col_name]*1000000
# Get in and out degree before sorting
pr_df=pr_df.sort_values(pr_col_name,ascending=False)
pr_df=pr_df.reset_index()
# scale the result

pr_df['Ranking']=pr_df.index+1

pr_df.to_csv('result/pagerank_result.csv',index=None)

from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

with DAG(
    dag_id="calculate_reputation_score",
    schedule=None,
    catchup=False
) as dag: