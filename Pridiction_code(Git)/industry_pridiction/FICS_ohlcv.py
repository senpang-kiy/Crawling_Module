import pandas as pd
import os



def all_info_data(FICS_file_route, dir_route):
    # 산업분류된 파일과, 1분데이터 매칭시켜 최종 데이터프레임 출력
    file_names = os.listdir(dir_route)
    file_name = {'file_name': file_names}
    FICS = pd.read_excel(FICS_file_route)[['code','업종(대분류)','업종(중분류)','업종(소분류)','시가총액']].dropna()
    code_name = {'code':list(map(lambda x : x[6:12], file_names))}
    filter_code = pd.DataFrame(code_name)
    filter_code['file_name'] = file_name['file_name']
    df = pd.merge(FICS, filter_code, left_on='code', right_on='code', how='inner')
    
    return df
#%%
    
def event_info(data,code,sector_kinds):
    # 종목 해당 섹터분류 , 섹터명을 출력
    mask = (data['code'] == code)
    result = data.loc[mask,:]
    
    sector_name = result[sector_kinds].iloc[0]
    file_name = result['file_name'].iloc[0]
    
    return sector_name , file_name


#%%
def sector_filter(data,sector_kinds,sector_name):
    # 종목에  알고싶은 섹터분류 , 섹터명을 필터처리한 데이터프레임 출력
    mask = (data[sector_kinds] == sector_name)
    result = data.loc[mask,:]
    
    return result


#%%

def Market_cap_filter(data):
    # 시총비율을 만들어 준다.
    cap_sum = data['시가총액'].sum()
    data['시가총액'] = data['시가총액'].apply(lambda x : round(x/cap_sum,4))
    data.rename(columns={'시가총액':'시총비율'} , inplace = True)
    data = data.reset_index().drop('index',axis=1)
    
    return data



#%%

def sector_ohlcv(data,dir_route):
    # 분류된 섹터 데이터프레임 과 1분데이터 파일 경로를 넣어주면 
    # 시총비율을 곱하하여 모든 값을 더한 산업ohlcv 값이 나온다.
    df_list = []
    
    for i in range(len(data)):
        
        file = data['file_name'][i]
        columns = ['open','high','low','close','volume']
        df  = pd.read_pickle(dir_route + '\\' + file)[columns]
        cap_ratio = data['시총비율'][i]
        
        for i in columns:
            df[i] = df[i].apply(int)
        df = df * cap_ratio    
        df_list.append(df) 
    
    ohlcv = sum(df_list)
    ohlcv.columns = ['sector_open','sector_high','sector_low','sector_close','sector_volume']
    return ohlcv   

#%%

def file_name_print(data,code):
    
    code_mask = (data['code'] == code)
    result = data.loc[code_mask,:]
    file_name = result['file_name'][0]
    
    return file_name
    
        
    
    














