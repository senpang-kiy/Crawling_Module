from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd
from datetime import datetime
import os

#['STK','KSQ','KNX']
url = 'http://marketdata.krx.co.kr/mdi#document=040602'
Market = ['STK','KSQ','KNX']
data = {'Market':[],'code':[],'ISIN_code':[], 'listing':[],'Company_Name':[],'IPO_date':[]}


driver = webdriver.Chrome('./chromedriver')
driver.get(url)


for market in Market:
    print(market)
    try:    
        os.remove('C:\\Users\\john\\Downloads\\data.xls')
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
    time.sleep(4)
    
    db = pd.read_excel('C:\\Users\\john\\Downloads\\data.xls')
    
    if market == 'STK':
        db['market'] = 'KOSPI'       
    elif market == 'KSQ':
        db['market'] = 'KOSDAQ'      
    elif market == 'KNX':
        db['market'] = 'KONEX'         
    
    for i in range(len(db)):
        
        data['Market'].append(db['market'][i])
        data['code'].append(db['종목코드'][i])
        
#%%
        
print(len(data['code']))
#%%
for market in Market:
    
    os.remove('C:\\Users\\john\\Downloads\\data.xls')
    
    KOSPI = driver.find_element_by_xpath('//*[@value="'+ market +'"]')
    KOSPI.click()
    time.sleep(2)
    serch_button = driver.find_element_by_xpath('//*[@id="btnidc81e728d9d4c2f636f067f89cc14862c"]/span')
    serch_button.click()
    time.sleep(4)
    serch_button = driver.find_element_by_xpath('//*[@id="6512bd43d9caa6e02c990b0a82652dca"]/button[3]')
    serch_button.click()
    time.sleep(4)
    
    data = pd.read_excel('C:\\Users\\john\\Downloads\\data.xls')
#%%

for market in Market:
    
    KOSPI = driver.find_element_by_xpath('//*[@value="'+ market +'"]')
    KOSPI.click()
    time.sleep(2)
    serch_button = driver.find_element_by_xpath('//*[@id="btnidc81e728d9d4c2f636f067f89cc14862c"]/span')
    serch_button.click()
    time.sleep(4)
    
    for k in range(1,20):
        try:
            for t in range(1,11):
                page_button1 = driver.find_element_by_xpath('//*[@class="WEBPONENT-PAGING-WRAPPER"]/a['+ str(t) +']')
                page_button1.click()
                tmp_page_source = driver.page_source
                tmp_soup = BeautifulSoup(tmp_page_source,'html.parser')
                even = tmp_soup.find_all('td',attrs={'class':'CI-GRID-ALIGN-CENTER'})
                company= tmp_soup.find_all('td',attrs={'class':'CI-GRID-ALIGN-LEFT'})
                
                for i,value in enumerate(even):
                    
                    if divmod(i,2)[1] == 0:
                        time.sleep(0.2)
                        count += 1
                        data['code'].append(value.text)
                        if market == 'STK':
                            data['Market'].append('KOSPI')
                            print('market: KOSPI','count:', count,'code: ', value.text )
                        elif market == 'KSQ':
                            data['Market'].append('KOSDAQ')
                            print('market: KOSDAQ','count:', count,'code: ', value.text )
                        elif market == 'KNX':
                            data['Market'].append('KONEX')   
                            print('market: KONEX','count:', count,'code: ', value.text )
                        
                    
                
            page_button2 = driver.find_element_by_xpath('//*[@class="WEBPONENT-PAGING-WRAPPER"]/input['+ str(3) +']')
            page_button2.click()
        
        except:
            
            break   
        
        time.sleep(1)
         #%%
         
print(len(data['Market']))
print(len(data['code']))


#%%
data_df = pd.DataFrame(data)
print(data_df)

#%%
now = datetime.now()
now = str(now)

data_df.to_excel('1112Digit_GICS_Crawlling_'+ now[:4] + '_' + now[5:7]+now[8:11]+'.xlsx')


