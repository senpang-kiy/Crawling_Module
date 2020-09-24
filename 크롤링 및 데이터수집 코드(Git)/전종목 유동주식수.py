

from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.keys import Keys
import os

url = 'http://marketdata.krx.co.kr/mdi#document=040602'
Market = ['STK', 'KSQ', 'KNX']
data = {'Market': [],'code': [], '종목명': [], '유동주식수': [], '유동주식비율': []}
markets_ = []
company_ = []
code_ = []

driver = webdriver.Chrome('C:\\Users\\john\\chromedriver')
# driver.maximize_window() #화면에 꽉 채우는 옵션
driver.get(url)
file_name = 'data.xls'

for market in Market:

    print(market)
    try:
        os.remove('C:\\Users\\john\\Downloads\\' + file_name)
    except:
        pass

    KOSPI = driver.find_element_by_xpath('//*[@value="' + market + '"]')
    KOSPI.click()
    time.sleep(2)
    serch_button = driver.find_element_by_xpath('//*[@id="btnidc81e728d9d4c2f636f067f89cc14862c"]/span')
    serch_button.click()
    time.sleep(4)
    serch_button = driver.find_element_by_xpath('//*[@id="6512bd43d9caa6e02c990b0a82652dca"]/button[2]')
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

        markets_.append(db['market'][i])

        if len(str(db['종목코드'][i])) != 6:
            code_.append('0' * (6 - len(str(db['종목코드'][i]))) + str(db['종목코드'][i]))
            company_.append(db['종목명'][i])

        else:
            company_.append(db['종목명'][i])
            code_.append(str(db['종목코드'][i]))

driver.close()
#%%
code_
#%%

A = []
B = []
driver = webdriver.Chrome('C:\\Users\\john\\chromedriver')
for index in range(len(code_)):
    print('전체갯수 :',len(code_), '진행갯수 :',index+1)
    url = 'https://navercomp.wisereport.co.kr/v2/company/c1070001.aspx?cmp_cd='+code_[index]+'&cn='
    driver.get(url)
    id="cTB711"
    time.sleep(0.3)
    tmp_page_source = driver.page_source
    tmp_soup = BeautifulSoup(tmp_page_source, 'html.parser')
    tr_first_last = tmp_soup.find_all('table', attrs={'id': 'cTB711'})
    A.append(tr_first_last[0].find_all('td')[8].text.replace('\n','').replace('\t',''))
    B.append(tr_first_last[0].find_all('td')[9].text.replace('\n','').replace('\t',''))
    
driver.close()
#%%
data = {'시장': [],'코드': [], '종목명': [], '유동주식수': [], '유동주식비율': []}
for i in range(len(code_)):
    data['시장'].append(markets_[i])
    data['코드'].append(company_[i])
    data['종목명'].append(code_[i])
    data['유동주식수'].append(A[i][:-1])
    data['유동주식비율'].append(B[i])
    #%%
data_df = pd.DataFrame(data) 
#%%  
now = datetime.now()
now = str(now)  
data_df.to_excel('C:\\Users\\john\\2Digit_유동주식수(통계)_'+ now[:4] + '_' + now[5:7]+now[8:11]+'.xlsx')
#%%
data_df
