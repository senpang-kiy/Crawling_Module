from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
import numpy as np
import requests
from io import BytesIO

data = {'market':[], 'code':[],'ISIN_code':[], 'listing':[],'Company_Name':[],'IPO_date':[]}
error_code = []


def stock_master(market):
    
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do'
    data = {
        'method':'download',
        'orderMode':'1',           # 정렬컬럼
        'orderStat':'D',           # 정렬 내림차순
        'searchType':'13',         # 검색유형: 상장법인
        'fiscalYearEnd':'all',     # 결산월: 전체
        'location':'all',          # 지역: 전체
        'marketType' : market
    }

    r = requests.post(url, data=data)
    f = BytesIO(r.content)
    dfs = pd.read_html(f, header=0, parse_dates=['상장일'])
    df = dfs[0].copy()

    # 숫자를 앞자리가 0인 6자리 문자열로 변환
    df['종목코드'] = df['종목코드'].astype(np.str)   
    df['종목코드'] = df['종목코드'].str.zfill(6)

    if market == 'stockMkt':

        df['market'] = 'KOSPI'
            
    elif market == 'kosdaqMkt':
        

        df['market'] = 'KOSDAP'
    
    elif market == 'konexMkt':

        df['market'] = 'KONEX'
    
    return df


KOSPI_df = stock_master('stockMkt')
KOSDAP_df = stock_master('kosdaqMkt')
KONEX_df = stock_master('konexMkt')


market_2EA = KOSPI_df.append(KOSDAP_df, ignore_index = True)
All_market = market_2EA.append(KONEX_df, ignore_index = True)

codes_ = [f for f in All_market['종목코드']]
market_name_ = [f for f in All_market['market']]

codes = codes_[:10]
market_name = market_name_[:10]



url = 'http://isin.krx.co.kr/srch/srch.do?method=srchList'
options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")

driver = webdriver.Chrome('./chromedriver', chrome_options=options)
#driver.maximize_window() #화면에 꽉 채우는 옵션
driver.get(url)
cnt=0

for i in range(len(codes)):
    
    keyword = 'KR7' + codes[i]
    cnt += 1
    search = driver.find_element_by_id('isur_nm1')
    search.clear()
    search.send_keys(keyword)
    search.send_keys(Keys.ENTER)
    

    time.sleep(0.2)
    tmp_page_source = driver.page_source
    tmp_soup = BeautifulSoup(tmp_page_source,'html.parser')

    tr_first_last = tmp_soup.find_all('tr',attrs={'class':'first last'})
        
    for results in tr_first_last:
        
        values = results.find_all('td')

        try:
            if '상장' in results.find_all('td')[6].text and '비상장' not in results.find_all('td')[6].text and '상장폐지' not in results.find_all('td')[6].text :
                
                for k,value in enumerate(values):

                    if k == 1:
                        data['ISIN_code'].append(value.text[1:-1])
                        data['code'].append(value.text[4:10])
                        data['market'].append(market_name[i])
                    
                    elif k == 2:
                        data['listing'].append(value.text)
                        print('count : ' ,cnt, ' company : ', value.text , '  market_name : ',market_name[i])
                    
                    elif k == 3:
                        data['Company_Name'].append(value.text)
                    
                    elif k == 7:
                        data['IPO_date'].append(value.text.strip())
                        
        except:
            
            error_code.append(keyword)
            
            pass
                 

driver.close()   



data_df = pd.DataFrame(data)
data_df
data_df.to_excel('stock_info.xlsx')