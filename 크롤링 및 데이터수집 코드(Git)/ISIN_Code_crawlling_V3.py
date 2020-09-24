import FinanceDataReader as fdr
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from datetime import datetime

tickers = fdr.StockListing('KRX')
tickers_A = tickers['Symbol']
market = tickers['Market']

group_A_count = 0
count = 0

for i,value in enumerate(tickers_A):

    count += 1
    if  value[5] == '5':
        tickers_A[i] = value[:5] + '1' 
        group_A_count += 1
    
    elif value[5] == '7':
        tickers_A[i] = value[:5] + '2'
        group_A_count += 1
    
    elif value[5] == 'M':
        tickers_A[i] = value[:5] + 'K'
        group_A_count += 1
    
    elif value[5] == 'L':
        tickers_A[i] = value[:5] + 'K'
        group_A_count += 1
        
    elif value[5] == 'K':
        group_A_count += 1
    
    elif value[5] == '1':
        group_A_count += 1
    
    elif value[5] == '2':
        group_A_count += 1

codes = tickers_A[:]


data = {'Market':[],'code':[],'ISIN_code':[], 'listing':[],'Company_Name':[],'IPO_date':[]}
error_code = []


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
                        data['code'].append(tickers_A[i])
                        data['Market'].append(market[i])
                    
                    elif k == 2:
                        data['listing'].append(value.text)
                        print('Market : ',market[i], 'count : ' ,cnt, ' company : ', value.text)
                    
                    elif k == 3:
                        data['Company_Name'].append(value.text)
                    
                    elif k == 7:
                        data['IPO_date'].append(value.text.strip())
                        
        except:
            
            error_code.append(keyword)
            
            pass
                 

driver.close()   

data_df = pd.DataFrame(data)

for i,value in enumerate(data_df['listing']):
 
    if  '보통주' in value:
        data_df['listing'][i]= '보통주'

    elif  '우선주' in value:
        data_df['listing'][i]= '우선주'
    
    else:
        data_df['listing'][i]= '보통주'
        

now = datetime.now()
now = str(now)
data_df.to_excel('2Digit_ISIN_Code_Crawlling_'+ now[:4] + '_' + now[5:7]+now[8:11]+'.xlsx')