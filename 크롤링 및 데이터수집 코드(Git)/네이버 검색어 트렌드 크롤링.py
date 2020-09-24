import sys
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import datetime #날짜 추출시 필요
import pandas as pd

from collections import Counter

sys.setrecursionlimit(10000)
# 크롤링할때 로봇이 아니다라는 표현장치라고 함
session = requests.session()
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    "Accept": "text/html;q=0.9,*/*;q=0.8"
}

path = 'C:\\Users\\john\\chromedriver.exe'

driver = webdriver.Chrome(path)
driver.get("https://datalab.naver.com/keyword/trendSearch.naver")
driver.find_element_by_id("item_keyword1").click() 
element = driver.find_element_by_id("item_keyword1") 
element.send_keys("우리들휴브레인") 
keyword1 = driver.find_element_by_id("item_sub_keyword1_1") 
keyword1.send_keys("비비비") 
driver.find_element_by_link_text("네이버 검색 데이터 조회").click()
time.sleep(1)
_url = driver.current_url

html = session.get(_url, headers=headers).content
soup = BeautifulSoup(html, 'html.parser')

result = soup.find_all('div',attrs={'data-timedimension':'date'}) 

#%%