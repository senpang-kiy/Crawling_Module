from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.keys import Keys
import os

url = 'http://marketdata.krx.co.kr/mdi#document=040602'
Market = ['STK','KSQ','KNX']
data = {'Market':[],'code':[],'ISIN_code':[], 'listing':[],'Company_Name':[],'IPO_date':[]}


driver = webdriver.Chrome('./chromedriver')
#driver.maximize_window() #화면에 꽉 채우는 옵션
driver.get(url)
file_name  = 'data.xls'

for market in Market:
    
    print(market)
    try:    
        os.remove('C:\\Users\\john\\Downloads\\' + file_name)
    except:
        pass
    
    KOSPI = driver.find_element_by_xpath('//*[@value="'+market+'"]')
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
        
        data['Market'].append(db['market'][i])
        
        if len(str(db['종목코드'][i])) != 6:
            data['code'].append('0'*(6 - len(str(db['종목코드'][i]))) + str(db['종목코드'][i]))
        else:
            data['code'].append(str(db['종목코드'][i]))
        
driver.close() 

for i,value in enumerate(data['code']):

    if  value[5] == '5':
        data['code'][i] = value[:5] + '1' 
  
    
    elif value[5] == '7':
        data['code'][i] = value[:5] + '2'
    
    elif value[5] == '9':
        data['code'][i] = value[:5] + '3'
    
    elif value[5] == 'M':
        data['code'][i] = value[:5] + 'K'

    
    elif value[5] == 'L':
        data['code'][i] = value[:5] + 'K'
        
    



#%%
url = 'http://isin.krx.co.kr/srch/srch.do?method=srchList'
options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")

driver2 = webdriver.Chrome('./chromedriver', chrome_options=options)
#driver.maximize_window() #화면에 꽉 채우는 옵션
driver2.get(url)

cnt = 0

error_code = []
error_index = []

for i in range(len(data['code'])):
        
    keyword = 'KR7' + data['code'][i]
    
    cnt += 1
    search = driver2.find_element_by_id('isur_nm1')
    search.clear()
    search.send_keys(keyword)
    search.send_keys(Keys.ENTER)
    

    time.sleep(0.2)
    tmp_page_source = driver2.page_source
    tmp_soup = BeautifulSoup(tmp_page_source,'html.parser')

    tr_first_last = tmp_soup.find_all('tr',attrs={'class':'first last'})
        
    for results in tr_first_last:
        time.sleep(0.2)
        values = results.find_all('td')

        try:
            if '상장' in results.find_all('td')[6].text and '비상장' not in results.find_all('td')[6].text and '상장폐지' not in results.find_all('td')[6].text :
                
                for k,value in enumerate(values):

                    if k == 1:
                        data['ISIN_code'].append(value.text[1:-1])                    
                    elif k == 2:
                        data['listing'].append(value.text)
                        print('Market : ',data['Market'][i], 'count : ' ,cnt, ' company : ', value.text)
                    
                    elif k == 3:
                        data['Company_Name'].append(value.text)
                    
                    elif k == 7:
                        data['IPO_date'].append(value.text.strip())
                        time.sleep(0.2)
        except:
            
            error_index.append(i)
            error_code.append(keyword)
            
            pass
                 

driver2.close()   

#%%
print(len(data['Market']))
print(len(data['code']))
print(len(data['ISIN_code']))
print(len(data['listing']))
print(len(data['Company_Name']))
print(len(data['IPO_date']))
print(error_code)
print(error_index)
print(len(error_code))
print(len(error_index))
print(data['code'][554])

cnts = 0
for i in data['code']:
    
    if i[0] == '9':
        cnts += 1
print(cnts)
    
    
#%%


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


#%%
