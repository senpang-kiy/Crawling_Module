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
'''파일 load'''
file_names = os.listdir('C:\\Users\\john\\one_min_stock_data')[1:]
data = pd.read_pickle('C:\\Users\\john\\one_min_stock_data\\'+file_names[0])


'''개별 종목 OHCLV전처리'''
split = lambda x : x[:8]                              # datetime 날짜까지 나오게 자른다
data['datetime'] = data['datetime'].apply(split)      # 데이터 프레임에 적용
times = data['datetime'].unique()                     # 날짜 종류 추출


def data_mask(data):   # 데이터 필터 
    
    df = data
    grf_list=[(df.low >= df.open) & (df.high <= df.close)& (df.close > df.open),
             (df.low >= df.open) & (df.high > df.close)& (df.close > df.open),
             (df.low < df.open) & (df.high <= df.close)& (df.close > df.open),
             (df.low < df.open) & (df.high > df.close)& (df.close > df.open),
             (df.low < df.close) & (df.high > df.open)& (df.close < df.open), # 위아래꼬리 음봉
             (df.low >= df.close) & (df.high > df.open)& (df.close < df.open),
             (df.low < df.close) & (df.high <= df.open)& (df.close < df.open),
             (df.low >= df.close) & (df.high <= df.open)& (df.close < df.open), 
             (df.open == df.close) & (df.low == df.close) & (df.high != df.close),
             (df.open == df.close) & (df.high == df.close) & (df.low != df.close),
             (df.open == df.close) & (df.low != df.close) & (df.high != df.close) ]

    grf_name = ['꼬리없는 양봉','윗꼬리 양봉','아래꼬리 양봉','위아래꼬리 양봉','위아래꼬리 음봉','윗꼬리 음봉','아래꼬리 음봉','꼬리없는 음봉','윗꼬리 도지','아래꼬리 도지','십자형 도지']
    for i in range(len(grf_list)):
        mask = grf_list[i]
        data[grf_name[i]] = mask.astype(int)
    
    return data        
    
data_mask(data)


train = []

for time in times:   # 날짜별로 분류
    
    mask = (data['datetime'] == time)
    result  = data.loc[mask,:]
    train.append(result)


'''일자별 봉 빈도수 카운트 및 데이터프레임 작업'''
grf_data = {'꼬리없는 양봉':[],'윗꼬리 양봉':[],'아래꼬리 양봉':[],'위아래꼬리 양봉':[],'위아래꼬리 음봉':[],'윗꼬리 음봉':[],'아래꼬리 음봉':[],'꼬리없는 음봉':[],'윗꼬리 도지':[],'아래꼬리 도지':[],'십자형 도지':[]}
bong_list = ['꼬리없는 양봉','윗꼬리 양봉','아래꼬리 양봉','위아래꼬리 양봉','위아래꼬리 음봉','윗꼬리 음봉','아래꼬리 음봉','꼬리없는 음봉','윗꼬리 도지','아래꼬리 도지','십자형 도지']

for i in range(len(train)):
    A = train[i][['꼬리없는 양봉','윗꼬리 양봉','아래꼬리 양봉','위아래꼬리 양봉','위아래꼬리 음봉','윗꼬리 음봉','아래꼬리 음봉','꼬리없는 음봉','윗꼬리 도지','아래꼬리 도지','십자형 도지']].sum()

    for k in range(len(A)):
        grf_data[bong_list[k]].append(A[k])

df2 = pd.DataFrame(grf_data)

df2['datetime'] = 'time'
for i in range(len(times)):
    df2['datetime'][i] = times[i]

df2 = df2[['datetime','꼬리없는 양봉','윗꼬리 양봉','아래꼬리 양봉','위아래꼬리 양봉','위아래꼬리 음봉','윗꼬리 음봉','아래꼬리 음봉','꼬리없는 음봉','윗꼬리 도지','아래꼬리 도지','십자형 도지']]
print(df2)

y = []
for i in range(len(train)-1): # 라벨링작업
    result = int(train[i]['close'].tail(1)) - int(train[i+1]['close'].tail(1)) # 그날종가 - 다음날종가 상승 1 하락 -1
    if result < 0:
        y.append(0)
    else:
        y.append(1)
print(y)
print(df2)       
#%%
from sklearn import preprocessing
from sklearn.neighbors import KNeighborsClassifier
from sklearn import metrics
from sklearn.metrics import accuracy_score



X = df2[['꼬리없는 양봉','윗꼬리 양봉','아래꼬리 양봉','위아래꼬리 양봉','위아래꼬리 음봉','윗꼬리 음봉','아래꼬리 음봉','꼬리없는 음봉','윗꼬리 도지','아래꼬리 도지','십자형 도지']]
X1 = X[:-1]
X2 = preprocessing.StandardScaler().fit(X1).transform(X1)

ys = np.array(y)
train_ = X.shape[0]
train_num = int(train_*0.8)


X_train = X2[:train_num]
X_label = ys[:train_num]
y_test = X2[train_num:] 
y_label = ys[train_num:]

X_train = X_train.reshape(X_train.shape[0],-1,X_train.shape[-1])
y_test = y_test.reshape(y_test.shape[0],-1,y_test.shape[-1])

#%%
print(X_train.shape)
print(X_label.shape)
print(y_test.shape)
print(y_label.shape)


#%%
callback_list = [ keras.callbacks.EarlyStopping(monitor='val_acc', patience=10,),
                 keras.callbacks.ModelCheckpoint(filepath = './basic_model.h5',monitor='val_loss', save_best_only=True)
                 ]
model = Sequential()
model.add(layers.Flatten(input_shape=(1, X_train.shape[-1])))
model.add(layers.Dense(32, activation='relu'))
model.add(layers.Dense(1 , activation='sigmoid'))
model.compile(loss='binary_crossentropy', optimizer=RMSprop() ,metrics=['acc'])
model.fit(X_train,
          X_label, 
          epochs = 100, 
          batch_size = 32, 
          callbacks = callback_list, 
          validation_split = 0.2)

model_new1 = keras.models.load_model('basic_model.h5') # 산업 종목 모델
acc_model1 = model_new1.evaluate(y_test,y_label, verbose=1)
print(acc_model1)

#%%
model_gru_bidirectional = Sequential()

GRU_callback_list = [ keras.callbacks.EarlyStopping(monitor='val_loss', patience=10,),
                  keras.callbacks.ModelCheckpoint(filepath = './Bidirectional_GRU_model.h5',monitor='val_loss', save_best_only=True)
                ]

model_gru_bidirectional.add(layers.Bidirectional(layers.GRU(32,
                                                            dropout= 0.1, 
                                                            recurrent_dropout = 0.5),                         
                                                 input_shape=(None, X_train.shape[-1])))

model_gru_bidirectional.add(layers.Dense(1,activation='sigmoid'))

model_gru_bidirectional.compile(loss='binary_crossentropy', optimizer=RMSprop() ,metrics=['acc'])
history_gru_bidirectional = model_gru_bidirectional.fit(X_train,
                                                        X_label, 
                                                        epochs = 100, 
                                                        batch_size = 32, 
                                                        callbacks = GRU_callback_list, 
                                                        validation_split = 0.2)


model_new2 = keras.models.load_model('Bidirectional_GRU_model.h5') # 산업 종목 모델
acc_model2 = model_new2.evaluate(y_test,y_label, verbose=1)
print(acc_model2)


#%%
knn = KNeighborsClassifier(n_neighbors = 25)
knn.fit(X_train,y_train)
y_hat = knn.predict(X_test)
accuracy = accuracy_score( y_test, y_hat)
print('정확도: ',accuracy)

#%%
a = []
b = []
c = []
for i in range(100):
    X_train,X_test,y_train,y_test = train_test_split(a,y,test_size=0.3, random_state = i)
    for k in range(1,100):
        knn = KNeighborsClassifier(n_neighbors = k)
        knn.fit(X_train,y_train)
        y_hat = knn.predict(X_test)
        accuracy = accuracy_score( y_test, y_hat)
        print('정확도: ',accuracy, ' count=',i)
        a.append(accuracy)
        b.append(i)
        c.append(k)
#%%
at = a.index(max(a))
print(b[at])
print(c[at])   


     