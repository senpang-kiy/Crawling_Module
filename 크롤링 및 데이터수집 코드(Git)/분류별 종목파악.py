from os import listdir, getpid
from os.path import isfile, join
import pandas as pd
import numpy as np
from multiprocessing import Pool, Process
import json
import os
import keras
from keras.models import Sequential
from keras import layers
from keras.optimizers import RMSprop
from keras.optimizers import Nadam
from keras.optimizers import Adam
from keras.optimizers import adagrad
from datetime import datetime as time
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from keras.layers import LeakyReLU
from sklearn.ensemble import VotingClassifier
from matplotlib import pyplot
from keras.models import load_model
from sklearn.preprocessing import MinMaxScaler


data = pd.read_excel('./GICS.xlsx')
df = data[['Symbol','Name','한국표준산업분류10차(대분류)']]
data.columns

#%%


CF = df['한국표준산업분류10차(대분류)'].unique()
CF_list = []

for cf_name in CF:

    mask = (df['한국표준산업분류10차(대분류)'] == cf_name)
    result  = df.loc[mask,:]
    CF_list.append(result)
#%%
cf_data = {'한국표준산업분류10차(대분류)':[],'종목개수':[]}
for i in range(len(CF_list)):

    cf_data['한국표준산업분류10차(대분류)'].append(CF[i])
    cf_data['종목개수'].append(len(CF_list[i]))
cf_df = pd.DataFrame(cf_data)    
cf_df.to_excel('C:\\Users\\john\\2Digit_GICS_분류_1단계.xlsx')    

