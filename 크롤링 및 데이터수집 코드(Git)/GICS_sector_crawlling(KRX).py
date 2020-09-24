from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd
from datetime import datetime


url = 'http://marketdata.krx.co.kr/mdi#document=03030204'

driver = webdriver.Chrome('./chromedriver')


data = {'base_sector_code':[],'sector_code':[],'sector_name':[],'code':[],'company_name':[]}

# =============================================================================
# sector_code = ['1010','1510','2010','2020','2030','2510','2520','2530','2550','3010','3020','3030',
#                '3510','3520','4010','4020','4030','4510','4520','4530','5010','5510','6010']
# =============================================================================

driver.get(url)

tmp_page_source = driver.page_source
tmp_soup = BeautifulSoup(tmp_page_source,'html.parser')
secter_count = tmp_soup.find_all('option')[4:]

sector_code = []
count_code = 0

for num in secter_count:
    
    str_num = str(num)
    count_code += 1
    
    if str_num[20] == '>':
        sector_code.append(str_num[15:19])       

driver.close()

driver2 = webdriver.Chrome('./chromedriver')

for k in sector_code:

    driver2.get(url)
    KOSPI = driver2.find_element_by_xpath('//*[@value=' + k + ']')
    KOSPI.click()
    time.sleep(2)
    
    serch_button = driver2.find_element_by_xpath('//*[@id="btnidc81e728d9d4c2f636f067f89cc14862c"]/span')
    serch_button.click()
    time.sleep(3)
    
    tmp_page_source = driver2.page_source
    tmp_soup = BeautifulSoup(tmp_page_source,'html.parser')
    
    
    even = tmp_soup.find_all('td',attrs={'class':'CI-GRID-ALIGN-CENTER'})
    even_name = tmp_soup.find_all('td',attrs={'style':'text-align: center'})[0].text
    
    for i,value in enumerate(even):
        
        if divmod(i,2)[1] == 0:
            data['sector_name'].append(even_name)
            data['code'].append(value.text)
            data['sector_code'].append(k)
            data['base_sector_code'].append(k[:2])
        else:
            data['company_name'].append(value.text)

driver2.close()

data_df = pd.DataFrame(data)
now = datetime.now()
now = str(now)

data_df.to_excel('2Digit_GICS_Crawlling_'+ now[:4] + '_' + now[5:7]+now[8:11]+'.xlsx')
#%%

