import requests
from io import BytesIO
from selenium import webdriver
import time
import pandas as pd
from datetime import datetime
import os
from loguru import logger
from seibro_config import log_config
import pymongo
import pymysql
import urllib
import copy


class sector_classification:

    def __init__(self):

        self.Data_table_name = 'test_FICS_Industry_classification'
        self.start()
        self.seibro_crawling()
        self.KRX_event_code_crawling()
        self.Data_merge()
        self.mongoDB_insert()
        self.MariaDB_insert()


    def seibro_crawling(self):

        df = {'종목코드': [], '종목명': [], '업종(대분류)': [], '업종(중분류)': [], '업종(소분류)': []}

        options = webdriver.ChromeOptions()

        url = 'http://seibro.co.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/stock/BIP_CNTS02004V.xml&menuNo=41'

        driver = webdriver.Chrome('chromedriver.exe')
        driver.get(url)

        logger.info('Crawling start')
        try:
            os.remove('C:\\Users\\john\\Downloads\\' + '주식종목전체검색.xls')
        except:
            pass
        time.sleep(5)
        secn = driver.find_element_by_xpath('//*[@id="CHECKBOX_SECN_TYPE_input_0"]')
        secn.click()
        logger.info('all click')
        time.sleep(5)
        market = driver.find_element_by_xpath('//*[@id="CHECKBOX_MART_TYPE_input_0"]')
        market.click()
        logger.info('market all click')

        time.sleep(5)

        search = driver.find_element_by_xpath('//*[@id="image4"]')
        search.click()
        time.sleep(50)
        logger.success('search click success')

        excel_down = driver.find_element_by_xpath('//*[@id="ExcelDownload_img"]')
        excel_down.click()
        time.sleep(50)
        logger.success('excel_down success')

        df_A = pd.read_html('C:\\Users\\john\\Downloads\\' + '주식종목전체검색.xls')[0]  # 다운로드 폴더 경로 지정
        df_B = df_A[['종목코드', '종목명', '시장구분', '업종(대분류)', '업종(중분류)', '업종(소분류)']]

        result_df = df_B[(df_B['시장구분'] == '코스닥') | (df_B['시장구분'] == '코넥스') | (df_B['시장구분'] == '유가증권')].reset_index(
            drop=True)
        driver.close()
        logger.success('seibro Crawling success')
        self.data_df1 = result_df

    def KRX_event_code_crawling(self):

        logger.debug('KRX Crawling start')

        todate = datetime.today().strftime('%Y%m%d')
        market = ['STK', 'KSQ', 'KNX']
        market_num = ['1001', '2001', 'N001']

        data2 = {'Market': [], 'event_code': [], 'event_name': [], 'industry_Main_Category': [],
                 'industry_intermediate_classification': [], 'industry_subclass': [], 'listed_stock_issued_count': [],
                 'market_cap': []}

        for i in range(len(market)):

            cookies = {'__utmz': '139639017.1591924625.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
                       '__utma': '139639017.1059413876.1591924625.1594945066.1596796152.19',
                       '__utmc': '139639017',
                       '__utma': '70557324.1154182106.1591924667.1594945103.1596796389.41',
                       '__utmc': '70557324',
                       '__utmz': '70557324.1596796389.41.23.utmcsr=krx.co.kr|utmccn=(referral)|utmcmd=referral|utmcct=/main/main.jsp',
                       'JSESSIONID': 'F2408210CAB1F95B0BBFBC7982CA6C13.103tomcat1',
                       }

            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Cookie': '__utmz=139639017.1591924625.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=139639017.1059413876.1591924625.1594945066.1596796152.19; __utmc=139639017; __utmc=70557324; __utmz=70557324.1596796389.41.23.utmcsr=krx.co.kr|utmccn=(referral)|utmcmd=referral|utmcct=/main/main.jsp; JSESSIONID=B790EC0ED9E799A7AE938764E74CE7A5.103tomcat1; __utmt=1; __utma=70557324.1154182106.1591924667.1596796389.1596796583.42; __utmb=70557324.6.10.1596796583',
                'Host': 'marketdata.krx.co.kr',
                'Referer': 'http://marketdata.krx.co.kr/mdi',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest',
            }

            params = {
                'name': 'fileDown',
                'filetype': 'csv',
                'url': 'MKD/04/0406/04060200/mkd04060200',
                'market_gubun': market[i],
                'indx_ind_cd': market_num[i],
                'sect_tp_cd': 'ALL',
                'isu_cdnm': '전체',
                'secugrp': 'ST',
                'stock_gubun': 'on',
                'schdate': todate,
                'pagePath': '/contents/MKD/04/0406/04060200/MKD04060200.jsp'
            }

            otp = requests.get('http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx', headers=headers,
                               params=params,
                               cookies=cookies, verify=False).text

            headers = {
                'Referer': 'http://marketdata.krx.co.kr/mdi',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
            }

            data_ = {'code': otp,
                     'some': 'data'}

            r = requests.post('http://file.krx.co.kr/download.jspx', headers=headers, data=data_)
            r.encoding = "utf-8-sig"
            db = pd.read_csv(BytesIO(r.content), header=0, thousands=',')

            if market[i] == 'STK':
                db['market'] = 'KOSPI'
            elif market[i] == 'KSQ':
                db['market'] = 'KOSDAQ'
            elif market[i] == 'KNX':
                db['market'] = 'KONEX'

            for i in range(len(db)):

                data2['Market'].append(db['market'][i])

                if len(str(db['종목코드'][i])) != 6:
                    data2['event_code'].append('0' * (6 - len(str(db['종목코드'][i]))) + str(db['종목코드'][i]))
                    data2['event_name'].append(db['종목명'][i])
                    data2['listed_stock_issued_count'].append(int(db['상장주식수(주)'][i]))
                    data2['market_cap'].append(int(db['상장시가총액(원)'][i]))
                    data2['industry_Main_Category'].append('no_result')
                    data2['industry_intermediate_classification'].append('no_result')
                    data2['industry_subclass'].append('no_result')

                else:
                    data2['event_name'].append(db['종목명'][i])
                    data2['event_code'].append(str(db['종목코드'][i]))
                    data2['listed_stock_issued_count'].append(int(db['상장주식수(주)'][i]))
                    data2['market_cap'].append(int(db['상장시가총액(원)'][i]))
                    data2['industry_Main_Category'].append('no_result')
                    data2['industry_intermediate_classification'].append('no_result')
                    data2['industry_subclass'].append('no_result')

        self.data_df2 = pd.DataFrame(data2)
        self.data_df2.to_excel('KRX.xlsx')
        logger.success('KRX Crawling suceess')

    def Data_merge(self):

        logger.info('Data merge start')
        for i, values in enumerate(self.data_df2['event_code']):
            for k, name in enumerate(self.data_df1['종목코드']):
                if name in values:
                    self.data_df2['industry_Main_Category'][i] = self.data_df1['업종(대분류)'][k]
                    self.data_df2['industry_intermediate_classification'][i] = self.data_df1['업종(중분류)'][k]
                    self.data_df2['industry_subclass'][i] = self.data_df1['업종(소분류)'][k]

        from datetime import datetime
        now = datetime.now()

        self.data_df3 = copy.deepcopy(self.data_df2)
        self.data_df2['_id'] = str(now)[:10].replace('-', '') + '_' + self.data_df2['event_code']
        self.data_df3['std_dt'] = str(now)[:10].replace('-', '')
        self.dff = self.data_df2[
            ['_id', 'Market', 'event_code', 'event_name', 'industry_Main_Category',
             'industry_intermediate_classification',
             'industry_subclass', 'listed_stock_issued_count', 'market_cap']]
        self.df_dict = self.dff.to_dict(orient="record")
        logger.success('Data merge success')

    def mongoDB_insert(self):

        logger.info('mongoDB insert start')
        # DB init
        HOSTNAME = '54.180.91.199'
        # HOSTNAME = '172.31.20.198'
        PORT = 27017
        username = urllib.parse.quote_plus('test')
        password = urllib.parse.quote_plus('test12!@')
        connection = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (username, password, HOSTNAME, PORT))
        DB = connection['test_seibro']
        DB_insert = DB['FICS_data2']
        logger.success('mongoDB connection success')

        for i in self.df_dict:
            try:
                DB_insert.insert_one(i)
                logger.info(i)
            except:
                logger.info('Duplicate occurrence')
                pass

        now = datetime.now()
        now = str(now)
        self.dff.to_excel('2Digit_FICS(seibro)_Crawlling_' + now[:4] + '_' + now[5:7] + now[8:11] + '.xlsx')
        self.dff.to_excel('2Digit_FICS(seibro).xlsx')
        logger.success('mongoDB insert success')
        logger.complete()

    def MariaDB_insert(self):

        logger.info("MariaDB insert Start")
        DataFrame = self.data_df3
        DataFrame = DataFrame.fillna(0)
        column_name_list = ''

        for column in DataFrame.columns:
            column_name_list += ' ,' + column

        columns_num = len(DataFrame.columns)
        value = '%s, ' * columns_num
        # DB Connection Setting
        conn = pymysql.connect(
        host="(host(url or ip)",
        user="ID",
        passwd="PW",
        database="table_name",
        charset='utf8')
        curs = conn.cursor(pymysql.cursors.DictCursor)
        vals = tuple([tuple(x) for x in DataFrame.values])
        logger.info("MariaDB connection success")
        sql = "INSERT INTO " + self.Data_table_name + " (" + column_name_list[2:] + ") VALUES(" + value[:-2] + ");"
        try:
            logger.info("MariaDB data input Start")
            curs.executemany(sql, vals)
            conn.commit()
            logger.success("MariaDB insert Success")
        except:
            logger.debug('overlap occur')
            pass
        finally:
            conn.close()

    def start(self):

        logger.add(log_config.filename + '_{time}.log', rotation=log_config.rotation, enqueue=True, encoding="utf8")
        logger.start('seibro Crawling round start')

if __name__ == '__main__':
    sector_classification()



