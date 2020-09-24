from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.keys import Keys
import os
import xlrd


df = {'종목코드': [], '업종(대분류)': [],'업종(중분류)': [],'업종(소분류)': []}


file_name = '주식종목전체검색.xls'
num = ['2','3','4']
url = 'http://seibro.co.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/stock/BIP_CNTS02004V.xml&menuNo=41'
    
driver = webdriver.Chrome('./chromedriver')
driver.get(url)

for count in num: 
    
    try:    
        os.remove('C:\\Users\\john\\Downloads\\' + file_name)
    except:
        pass
    
    time.sleep(1)
    market = driver.find_element_by_xpath('//*[@id="CHECKBOX_SECN_TYPE_input_0"]')
    market.click()
    time.sleep(1)
    
    secn = driver.find_element_by_xpath('//*[@id="CHECKBOX_MART_TYPE"]/li[1]')
    secn.click()
    time.sleep(1)
    
    if count == '2':
        pass
    
    else:
        secn = driver.find_element_by_xpath('//*[@id="CHECKBOX_MART_TYPE"]/li['+count+']')
        secn.click()
        time.sleep(1)
        
    search = driver.find_element_by_xpath('//*[@id="image4"]')
    search.click()
    time.sleep(50)
    
    excel_down = driver.find_element_by_xpath('//*[@id="ExcelDownload_img"]')
    excel_down.click()
    time.sleep(50)

    df_A = pd.read_html("C:\\Users\\john\\Downloads\\주식종목전체검색.xls")[0]
    df_B = df_A[['종목코드','업종(대분류)','업종(중분류)','업종(소분류)']]
    
    for i in range(len(df_B['종목코드'])):
        
        df['종목코드'].append(df_B['종목코드'][i])
        df['업종(대분류)'].append(df_B['업종(대분류)'][i])
        df['업종(중분류)'].append(df_B['업종(중분류)'][i])
        df['업종(소분류)'].append(df_B['업종(소분류)'][i])
    

driver.close()    
#
data_df1 = pd.DataFrame(df)

for i,code in enumerate(data_df1['종목코드']):

    if type(code) == str:
        pass
    
    else:
        str_= str(code)
        
        if len(str_) == 6:
            data_df1['종목코드'][i] = str_
        else:
            str_code = '0'*(6 - len(str_)) + str_
            data_df1['종목코드'][i] = str_code
# 종목코드 업종(대분류)  ,업종(중분류), 업종(대분류)

url = 'http://marketdata.krx.co.kr/mdi#document=040602'
Market = ['STK', 'KSQ', 'KNX']
data2 = {'Market': [], 'code': [], '종목명': [], '업종_대분류': [],'업종_중분류': [],'업종_소분류': [] ,'상장주식수':[],'시가총액':[]}

driver2 = webdriver.Chrome('C:\\Users\\john\\chromedriver')
# driver.maximize_window() #화면에 꽉 채우는 옵션
driver2.get(url)
file_name = 'data.xls'

for market in Market:

    print(market)
    try:
        os.remove('C:\\Users\\john\\Downloads\\' + file_name)
    except:
        pass

    KOSPI = driver2.find_element_by_xpath('//*[@value="' + market + '"]')
    KOSPI.click()
    time.sleep(2)
    serch_button = driver2.find_element_by_xpath('//*[@id="btnidc81e728d9d4c2f636f067f89cc14862c"]/span')
    serch_button.click()
    time.sleep(4)
    serch_button = driver2.find_element_by_xpath('//*[@id="6512bd43d9caa6e02c990b0a82652dca"]/button[2]')
    serch_button.click()
    time.sleep(7)

    db = pd.read_excel('C:\\Users\\john\\Downloads\\data.xls')

    if market == 'STK':
        db['market'] = 'KOSPI'

    elif market == 'KSQ':
        db['market'] = 'KOSDAQ'

    elif market == 'KNX':
        db['market'] = 'KONEX'

    for i in range(len(db)):

        data2['Market'].append(db['market'][i])

        if len(str(db['종목코드'][i])) != 6:
            data2['code'].append('0' * (6 - len(str(db['종목코드'][i]))) + str(db['종목코드'][i]))
            data2['종목명'].append(db['종목명'][i])
            data2['상장주식수'].append(int(db['상장주식수(주)'][i].replace(',',''))) 
            data2['시가총액'].append(int(db['상장시가총액(원)'][i].replace(',',''))) 
            data2['업종_대분류'].append('결과없음') 
            data2['업종_중분류'].append('결과없음') 
            data2['업종_소분류'].append('결과없음') 

        else:
            data2['종목명'].append(db['종목명'][i])
            data2['code'].append(str(db['종목코드'][i]))
            data2['상장주식수'].append(int(db['상장주식수(주)'][i].replace(',','')))
            data2['시가총액'].append(int(db['상장시가총액(원)'][i].replace(',',''))) 
            data2['업종_대분류'].append('결과없음') 
            data2['업종_중분류'].append('결과없음') 
            data2['업종_소분류'].append('결과없음') 

driver2.close()
data_df2 = pd.DataFrame(data2)


for i,values in enumerate(data_df2['code']) :
    
    for k,name in enumerate(data_df1['종목코드']):
        
        if name in values:

            data_df2['업종_대분류'][i] = data_df1['업종(대분류)'][k]
            data_df2['업종_중분류'][i] = data_df1['업종(중분류)'][k]
            data_df2['업종_소분류'][i] = data_df1['업종(소분류)'][k]

GICS = data_df2
sector_list = ['대분류','중분류','소분류']
for list_name in sector_list:
        
    nan_drap_data = GICS.dropna(how='any')
    industry_colums = nan_drap_data['업종_'+ list_name].unique()
    industry = {'industry_name' : [] , 'industry_sum' : []} 
    
    for colums_name in industry_colums:
        
        mask1 = (nan_drap_data['업종_'+list_name] == colums_name)
        result = nan_drap_data.loc[mask1,:]
    
        industry['industry_sum'].append(result['시가총액'].sum())
        industry['industry_name'].append(colums_name)
    
    industry_df = pd.DataFrame(industry)

    
    GICS['시총반영비율_'+ list_name] = '산업분류값 없음'
    
    for i in range(len(GICS['업종_'+list_name])):
        
        for k in range(len(industry['industry_name'])):
            
            if industry['industry_name'][k] == GICS['업종_'+list_name][i]:
                
                GICS['시총반영비율_'+ list_name][i] = round(GICS['시가총액'][i]/industry['industry_sum'][k] , 6)
                

now = datetime.now()
now = str(now)  
data_df2.to_excel('C:\\Users\\john\\2Digit_GICS_sector_ratio'+ now[:4] + '_' + now[5:7]+now[8:11]+'.xlsx')
























 
    
    



