from numpy import array
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Flatten
import keras
from keras.layers.convolutional import MaxPooling1D
from keras.models import Model
from keras.layers import Conv1D, Dense, MaxPool1D, Flatten, Input
from keras.layers import BatchNormalization
from keras.layers import AveragePooling1D
from keras.optimizers import RMSprop
from keras.optimizers import Nadam
from keras.models import Model
from keras.layers import Input, Dense, LSTM, multiply, concatenate, Activation, Masking, Reshape
from keras.layers import Conv1D, BatchNormalization, GlobalAveragePooling1D, Permute, Dropout
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from layer_utils import AttentionLSTM
from os import listdir, getpid
from os.path import isfile, join
import pandas as pd
import numpy as np
import keras
from keras import layers
from keras.optimizers import RMSprop
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
from add_indicator import create_Indicators
from FICS_ohlcv import all_info_data, sector_filter, Market_cap_filter, sector_ohlcv, file_name_print, event_info
from time import sleep
#%%

FICS_file_route = 'D:\\FICS_module\\2Digit_FICS(seibro)_Crawlling_2020_0721 .xlsx'
m1_data_dir_route = 'D:\\FICS_module\\1m_filter_data'
h1_data_dir_route = 'D:\\FICS_module\\h1_filter_data'

dir_route = h1_data_dir_route                              # 파일 루트를 지정해준다.
df = all_info_data(FICS_file_route,dir_route)             # 전체 join file (산업분류및 코드,시가총액,파일명)  
code_list = list(df['code'])


sector_kinds = '업종(중분류)'
code = code_list[1]
dir_route = h1_data_dir_route # 파일 루트를 지정해준다.


sector_name,file_name = event_info(df,code,sector_kinds)  # 종목 해당 섹터명, 파일명 출력
df1 = sector_filter(df,sector_kinds,sector_name)          # 위에서 출력된 섹터만 필터되서 출력해준다
df2 = Market_cap_filter(df1)                              # 섹터데이터를 넣어주면 시가총액이 계산되서 데이터프레임으로 들어가 출력

type(df2)                      

add_ohlcv = sector_ohlcv(df2,h1_data_dir_route)           # 시총비율이 계산되어 모든값을 합한 산업ohlcv 가 출력
event = pd.read_pickle(dir_route + '\\' + file_name).drop('date',axis=1)      # 해당 종목 데이터 프레임 출력



event_columns = event.columns  

for i in event_columns:                                                            # object 값을 float 처리 한다.
    event[i] = event[i].apply(float)
    
    
momentom = create_Indicators(event)                    # null값이 엄청많은건 지운다    
sector_ohlcv = pd.concat([event,add_ohlcv] , axis=1)                         # 섹터 + 개별 종목

columns = momentom.columns                                                   
for i in columns:                                                            # object 값을 float 처리 한다.
    momentom[i] = momentom[i].apply(float)

split_price = momentom.isnull().sum().max()                                  # 최대 null 값을 출력
momentom = momentom[split_price:].reset_index().drop('index',axis=1)         # null 값만큼 앞부분 제거
add_ohlcv = add_ohlcv[split_price:].reset_index().drop('index',axis=1)       # row 값을 동일하게 맞쳐주도록 momontom 과 split을 한다.
print(momentom.isnull().sum())                                               # null 값 확인
momentom_result = pd.concat([momentom,add_ohlcv] , axis=1)                            # 두개의 데이터프레임을 합친다.


# Batch data를 생성한다.
def create_Data(data, window_size):
    df = data
    size = window_size
    window_data = []
    
    # 정규화
    for i in df.columns:
        df[i] = (df[i] - df[i].mean()) / df[i].std()
    
    
    # train data 생성
    window_data = []
    for i in range(len(df) - size):
        result = np.array(df[i:i+window_size])
        window_data.append(result)
        
        
        
    # label data 생성
    window_data_label = []
    
    A = list(data.columns).index('close')
    for k in range(len(window_data)-1):
        
        now_close_price = window_data[k][-1][A]
        next_close_price = window_data[k+1][-1][A]
        
        if next_close_price - now_close_price > 0:
            window_data_label.append(1)
        else:
            window_data_label.append(0)
    
    
    train_data = np.array(window_data)
    label_array = np.array(window_data_label)
    label_data = label_array.reshape(len(window_data_label),-1)
    
    
    return train_data[:-1] , label_data


def train_test_split(train,label,split_size):
    
    # train test 데이터를 나눈다
    split_value = int(len(train) * split_size)
    
    train_data = train[:split_value]
    train_label = label[:split_value]
    test_data = train[split_value:]
    test_label = label[split_value:]
    
    return train_data, train_label ,test_data ,test_label


x,y = create_Data(momentom_result,7)
x1,y1 = create_Data(event,7)
x2,y2 = create_Data(sector_ohlcv,7)


train_data, train_label ,test_data ,test_label = train_test_split(x, y, 0.7)
train_data_event, train_label_event ,test_data1 ,test_label1 = train_test_split(x1, y1, 0.7)
train_data2, train_label2 ,test_data2 ,test_label2 = train_test_split(x2, y2, 0.7)

 
callback_list = [ keras.callbacks.EarlyStopping(monitor='val_acc', patience=10,),
                 keras.callbacks.ModelCheckpoint(filepath = './basic_model.h5',monitor='val_loss', save_best_only=True)]
ip = Input(shape=(train_data.shape[1], train_data.shape[2]))

# 첫번째 입력층으로부터 분기되어 진행되는 인공 신경망을 정의
x = Masking()(ip)
#x = AttentionLSTM(384, unroll=True)(x)
x = LSTM(8)(x)
x = Dropout(0.8)(x)

# 두번째 입력층으로부터 분기되어 진행되는 인공 신경망을 정의
y = Permute((2, 1))(ip)
y = Conv1D(128, 8, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = Conv1D(256, 5, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = Conv1D(128, 3, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = GlobalAveragePooling1D()(y)

x = concatenate([x, y])

out = Dense(1, activation='sigmoid')(x)

model = Model(ip, out)
model.summary()

model.compile(loss='binary_crossentropy', optimizer='RMSprop' ,metrics=['acc'])

model.fit(train_data,
          train_label,
          epochs = 100, 
          batch_size = 128, 
          callbacks = callback_list,
          validation_split = 0.2
          )

h = model.predict(test_data)
   
a = []
for i in range(len(h)):
    
    if h[i][0] < 0.5:
        a.append(0)
    else:
        a.append(1)
        
count = 0
for i,k in zip(a,test_label):
    
    if i == k:
        count += 1
        
result0 = count/len(test_label)


callback_list = [ keras.callbacks.EarlyStopping(monitor='val_acc', patience=10,),
                 keras.callbacks.ModelCheckpoint(filepath = './basic_model.h5',monitor='val_loss', save_best_only=True)]
ip = Input(shape=(train_data_event.shape[1], train_data_event.shape[2]))

# 첫번째 입력층으로부터 분기되어 진행되는 인공 신경망을 정의
x = Masking()(ip)
#x = AttentionLSTM(384, unroll=True)(x)
x = LSTM(8)(x)
x = Dropout(0.8)(x)

# 두번째 입력층으로부터 분기되어 진행되는 인공 신경망을 정의
y = Permute((2, 1))(ip)
y = Conv1D(128, 8, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = Conv1D(256, 5, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = Conv1D(128, 3, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = GlobalAveragePooling1D()(y)

x = concatenate([x, y])

out = Dense(1, activation='sigmoid')(x)

model = Model(ip, out)
model.summary()

model.compile(loss='binary_crossentropy', optimizer='RMSprop' ,metrics=['acc'])

model.fit(train_data_event,
          train_label_event,
          epochs = 100, 
          batch_size = 128, 
          callbacks = callback_list,
          validation_split = 0.2
          )

h = model.predict(test_data1)
   
a = []
for i in range(len(h)):
    
    if h[i][0] < 0.5:
        a.append(0)
    else:
        a.append(1)
        
count = 0
for i,k in zip(a,test_label1):
    
    if i == k:
        count += 1
        
result1 = count/len(test_label1)


callback_list = [ keras.callbacks.EarlyStopping(monitor='val_acc', patience=10,),
                 keras.callbacks.ModelCheckpoint(filepath = './basic_model.h5',monitor='val_loss', save_best_only=True)]
ip = Input(shape=(train_data2.shape[1], train_data2.shape[2]))

# 첫번째 입력층으로부터 분기되어 진행되는 인공 신경망을 정의
x = Masking()(ip)
#x = AttentionLSTM(384, unroll=True)(x)
x = LSTM(8)(x)
x = Dropout(0.8)(x)

# 두번째 입력층으로부터 분기되어 진행되는 인공 신경망을 정의
y = Permute((2, 1))(ip)
y = Conv1D(128, 8, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = Conv1D(256, 5, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = Conv1D(128, 3, padding='same', kernel_initializer='he_uniform')(y)
y = BatchNormalization()(y)
y = Activation('relu')(y)

y = GlobalAveragePooling1D()(y)

x = concatenate([x, y])

out = Dense(1, activation='sigmoid')(x)

model = Model(ip, out)
model.summary()

model.compile(loss='binary_crossentropy', optimizer='RMSprop' ,metrics=['acc'])

model.fit(train_data2,
          train_label2,
          epochs = 100, 
          batch_size = 128, 
          callbacks = callback_list,
          validation_split = 0.2
          )

h = model.predict(test_data2)
   
a = []
for i in range(len(h)):
    
    if h[i][0] < 0.5:
        a.append(0)
    else:
        a.append(1)
        
count = 0
for i,k in zip(a,test_label2):
    
    if i == k:
        count += 1
        
result2 = count/len(test_label2)


print('산업 + +momentom + 종목 :', result0)
print('종목                    :',result1)
print('momentom+종목           :',result2)

