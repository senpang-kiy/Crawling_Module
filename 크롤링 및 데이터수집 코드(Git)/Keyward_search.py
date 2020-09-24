from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd


search = ['3S']
data = {'검색명':[],'키워드명':[]}

for search_name in search:

    url = 'https://search.naver.com/search.naver?sm=tab_hty.top&where=nexearch&query=' + search_name + '&oquery=s3&tqi=UXr4ylprvhGssDhcSrGssssstcR-032897'

    driver = webdriver.Chrome('C:\\Users\\john\\chromedriver.exe')
    driver.get(url)

    tmp_page_source = driver.page_source
    tmp_soup = BeautifulSoup(tmp_page_source, 'html.parser')
    result = tmp_soup.find_all('a', attrs={'data-area': "*q"})

    for i in result:
        data['키워드명'].append(i.text)
        data['검색명'].append(search_name)

driver.close()
data_df = pd.DataFrame(data)

print(data_df)