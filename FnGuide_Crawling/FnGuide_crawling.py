from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import requests
from io import BytesIO
from FnGuide_config import filtering_word
from tqdm import tqdm_notebook, tnrange, tqdm
import urllib
import pymongo
from loguru import logger
from FnGuide_config import log_config
import pandas as pd
import pymysql


def KRX_code():
    todate = datetime.today().strftime('%Y%m%d')
    market = ['STK', 'KSQ', 'KNX']
    market_num = ['1001', '2001', 'N001']
    excel_list = []

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

        excel_list.append(db)

    KRX_df = pd.concat(excel_list).reset_index().drop('index', axis=1)

    return KRX_df


def html_extraction(code):
    # Snapshot page http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp
    header = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'
    }
    data_ = {
        'pGB': '1',
        'gicode': code,
        'MenuYn': 'Y',
        'NewMenuID': '101',
        'stkGb': '701'
    }
    req = requests.post('http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp', params=data_, headers=header)
    page1 = BeautifulSoup(req.content, 'html.parser')

    # 재무재표 page http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp
    header = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'
    }
    data_ = {
        'pGB': '1',
        'gicode': code,
        'MenuYn': 'Y',
        'NewMenuID': '103',
        'stkGb': '701'
    }
    req = requests.post('http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp', params=data_, headers=header)
    page2 = BeautifulSoup(req.content, 'html.parser')

    # 재무비율 page http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp
    header = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'
    }
    data_ = {
        'pGB': '1',
        'gicode': code,
        'MenuYn': 'Y',
        'NewMenuID': '104',
        'stkGb': '701'
    }
    req = requests.post('http://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp', params=data_, headers=header)
    page3 = BeautifulSoup(req.content, 'html.parser')

    # 투자지표 page http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp
    header = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'
    }
    data_ = {
        'pGB': '1',
        'gicode': code,
        'MenuYn': 'Y',
        'NewMenuID': '105',
        'stkGb': '701'
    }
    req = requests.post('http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp', params=data_, headers=header)
    page4 = BeautifulSoup(req.content, 'html.parser')

    return page1, page2, page3, page4


def field_name_filtering(field_name):
    filtering_dict = filtering_word.field_name_filtering

    if field_name in filtering_dict:
        return filtering_dict[field_name]
    else:
        return field_name


def field_name_change(field_name):
    change_dict = filtering_word.field_name_change

    if field_name in change_dict:
        return change_dict[field_name]
    else:
        return field_name


def page1_parsing(page1, result, event_code, event_name):
    logger.debug(event_code + ':' + event_name + ' page1 crawling start')

    date = page1.find_all('div', attrs={'id': 'highlight_D_Q'})[0].find_all('thead')[0]  # 분기 날짜
    data_table_list = page1.find_all('div', attrs={'id': 'highlight_D_Q'})[0].find_all('tbody')[0]  # 재무 데이터
    data_table_list = data_table_list.find_all('tr')
    data = [datas for datas in data_table_list]  # 항목들 추출 및 리스트 append

    # 분기 날짜 추출
    date = date.find_all('tr', attrs={'class': 'td_gapcolor2'})[0].find_all('th')
    quarter_date_list = [date[i].text for i in range(5)]

    # result dict 키값 삽입
    for quarter_date in quarter_date_list:
        dict_ = dict()
        dict_['_id'] = quarter_date.replace('/', '') + '_' + event_code
        dict_['std_dt'] = quarter_date.replace('/', '') + '30'
        dict_['event_code'] = event_code
        dict_['event_name'] = event_name
        result[quarter_date] = dict_

    field_name_list = filtering_word.field_name_quarter_page1  # 필요한 필드명 가져온다
    remove_word = filtering_word.remove_word  # 삭제 값 리스트 호출

    # 항목별 분기 값 추출 및 result dict 값 삽입
    for field in data:
        try:
            field_name = field.find_all('span', attrs={'class': "txt_acd"})[0].text.strip()
        except:
            field_name = field.find_all('th', attrs={'scope': "row"})[0].text.strip()

        if field_name in field_name_list:
            values = field.find_all('td', attrs={'class': 'r'})
            for value, quarter_date in zip(values[:len(quarter_date_list)], quarter_date_list):
                value = value.text
                try:
                    value = float(value.replace(',',''))             # 소수점을 만드는데 에러가 안나면 작업을 진행한다 문자열일 경우 에러발생
                    if value not in remove_word:
                        change_name = field_name_change(field_name)  # 한글명 -> 영문명 변경
                        result[quarter_date][change_name] = value    # 딕셔너리 삽입
                    else:pass
                except:pass

    logger.success(event_code + ':' + event_name + ' page1 crawling success')

    return result


def page2_parsing(page2, result, event_code, event_name):

    logger.debug(event_code + ':' + event_name + ' page2 crawling start')

    date = page2.find_all('div', attrs={'id': 'divSonikQ'})[0].find_all('thead')[0]  # 분기 날짜
    data_table_list = page2.find_all('div', attrs={'class': 'um_table'})  # 재무 데이터

    # 분기 id값  추출 후 분기 재무재표 table 리스트 append
    quarter_table_list = list()
    for ids in data_table_list:
        if ids['id'][-1] == 'Q':
            quarter_table_list.append(page2.find_all('div', attrs={'id': ids['id']})[0].find_all('tbody'))
    data_table = sum(quarter_table_list, [])  # sum table_list
    data = sum([tbody.find_all('tr') for tbody in data_table], [])  # 항목들 추출 및 리스트 append

    # 분기 날짜 추출
    date = date.find_all('th')[1:]
    quarter_date_list = [date[i].text for i in range(4)]

    field_name_list = filtering_word.field_name_quarter_page2  # 필요한 필드명 가져온다
    remove_word = filtering_word.remove_word  # 삭제 값 리스트 호출

    # 항목별 분기 값 추출 및 result dict 값 삽입
    for field in data:
        try:
            field_name = field.find_all('th', attrs={'scope': "row"})[0].find_all('div', attrs={'class': "th_b"})[
                0].text.strip()
        except:
            try:
                field_name = \
                field.find_all('th', attrs={'scope': "row"})[0].find_all('span', attrs={'class': 'txt_acd'})[
                    0].text.strip()
            except:
                field_name = field.find_all('th', attrs={'scope': "row"})[0].text.strip()

        if field_name in field_name_list:
            values = field.find_all('td', attrs={'class': 'r'})
            for value, quarter_date in zip(values[:len(quarter_date_list)], quarter_date_list):
                value = value.text
                try:
                    value = float(value.replace(',', ''))  # 소수점을 만드는데 에러가 안나면 작업을 진행한다 문자열일 경우 에러발생
                    if value not in remove_word:
                        change_name = field_name_change(field_name)  # 한글명 -> 영문명 변경
                        result[quarter_date][change_name] = value  # 값 딕셔너리 삽입
                    else:
                        pass
                except:
                    pass

    logger.success(event_code + ':' + event_name + ' page2 crawling success')

    return result


def page3_parsing(page3, result, event_code, event_name):

    logger.debug(event_code+':'+event_name +' page3 crawling start')

    table_data = page3.find_all('div', attrs={
        'class': 'ul_col2wrap pd_t25'})  # len(table_data index) = 2  [ 0: 5년 데이터,  index 1: 분기데이터 ]
    data_5year_date = table_data[0].find_all('thead')[0].find_all('th')[1:5]  # 5년 데이터 날짜
    data_5year_data = table_data[0].find_all('tbody')[0].find_all('tr')
    data_quarter_date = table_data[1].find_all('thead')[0].find_all('th')[1:]  # 분기 데이터 날짜
    data_quarter_data = table_data[1].find_all('tbody')[0].find_all('tr')  # 분기 데이터 재무

    result_key_list = list(result)
    # 분기 날짜 추출 및 딕셔너리 삽입
    data_5year_date_list = [data_5year_date[i].text for i in range(4)]
    data_quarter_date_list = [data_quarter_date[i].text for i in range(5)]

    for date_5year in data_5year_date_list:  # 딕셔너리 중복 키값 확인 새로운 키값이
        if date_5year in result_key_list:
            pass
        else:
            dict_ = dict()
            dict_['_id'] = date_5year.replace('/', '') + '_' + event_code
            dict_['std_dt'] = date_5year.replace('/', '') + '30'
            dict_['event_code'] = event_code
            dict_['event_name'] = event_name
            result[date_5year] = dict_

    for quarter_date in data_quarter_date_list:
        if quarter_date in result_key_list:
            pass
        else:
            dict_ = dict()
            dict_['_id'] = quarter_date.replace('/', '') + '_' + event_code
            dict_['std_dt'] = quarter_date.replace('/','') + '30'
            dict_['event_code'] = event_code
            dict_['event_name'] = event_name
            result[quarter_date] = dict_

    field_name_list_5year = filtering_word.field_name_page3_5year  # 필요한 필드명 가져온다
    field_name_list_quatrer = filtering_word.field_name_quarter_page3  # 필요한 필드명 가져온다
    remove_word = filtering_word.remove_word  # 삭제 값 리스트 호출

    count_5year = 0
    # 항목별 분기 값 추출 및 result dict 값 삽입 - 5year
    for field in data_5year_data:
        try:
            field_name = field.find_all('dt')[0].text.strip()
        except:
            try:
                field_name = field.find_all('th', attrs={'scope': "row"})[0].text.strip()
            except:
                field_name = None
                pass
        if field_name is None:
            pass
        else:
            if field_name in field_name_list_5year and count_5year < len(field_name_list_5year):
                count_5year += 1
                values = field.find_all('td', attrs={'class': 'r'})
                for value, quarter_date in zip(values[:len(data_5year_date_list)], data_5year_date_list):
                    value = value.text
                    try:
                        value = float(value.replace(',', ''))  # 소수점을 만드는데 에러가 안나면 작업을 진행한다 문자열일 경우 에러발생
                        if value not in remove_word:
                            filtering_name = field_name_filtering(field_name)  # 분기데이터 필드명과 중복되는 항목 필드명 변경
                            change_name = field_name_change(filtering_name)  # 한글명 -> 영문명 변경
                            result[quarter_date][change_name] = value  # 딕셔너리 삽입
                        else:
                            pass
                    except:
                        pass

    count_quarter = 0
    # 항목별 분기 값 추출 및 result dict 값 삽입 - quarter
    for field in data_quarter_data:
        try:
            field_name = field.find_all('dt')[0].text.strip()
        except:
            try:
                field_name = field.find_all('th', attrs={'scope': "row"})[0].text.strip()
            except:
                field_name = None
                pass
        if field_name is None:
            pass
        else:
            if field_name in field_name_list_quatrer and count_quarter < len(field_name_list_quatrer):
                count_quarter += 1
                values = field.find_all('td', attrs={'class': 'r'})

                for value, quarter_date in zip(values[:len(data_quarter_date_list)], data_quarter_date_list):
                    value = value.text
                    try:
                        value = float(value.replace(',', ''))  # 소수점을 만드는데 에러가 안나면 작업을 진행한다 문자열일 경우 에러발생
                        if value not in remove_word:
                            change_name = field_name_change(field_name)  # 한글명 -> 영문명 변경
                            result[quarter_date][change_name] = value  # 딕셔너리 삽입
                        else:
                            pass
                    except:
                        pass

    logger.success(event_code + ':' + event_name + ' page3 crawling success')

    return result


def mongoDB_insert(values):
    # DB init
    HOSTNAME = '54.180.91.199'
    #HOSTNAME = '172.31.20.198'
    PORT = 27017
    username = urllib.parse.quote_plus('test')
    password = urllib.parse.quote_plus('test12!@')
    connection = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (username, password, HOSTNAME, PORT))
    DB = connection['FnGuide_Crawling']
    DB_insert = DB['Fngide_Crawling_test2']

    for value in values:
        logger.info(value)
        try:
            DB_insert.insert_one(values[value])
            logger.success('mongoDB insert success')
        except:
            logger.debug('overlap occur')
            pass
        finally:
            connection.close()


def MariaDB_insert(DataFrame, MariaDB_table_name):

    logger.debug("MariaDB insert Start")
    DataFrame = pd.DataFrame(DataFrame).T
    DataFrame = DataFrame.fillna(0)
    DataFrame = DataFrame.drop('_id', axis=1)
    column_name_list = ''

    for column in DataFrame.columns:
        column_name_list += ' ,' + column

    columns_num = len(DataFrame.columns)
    value = '%s, ' * columns_num
    # DB Connection Setting
    conn = pymysql.connect(
        host="database-1.c1pvc7savwst.ap-northeast-2.rds.amazonaws.com",
        user="nsuser",
        passwd="nsuser))))",
        database="NewsSalad_dev_01",
        charset='utf8'
    )
    curs = conn.cursor(pymysql.cursors.DictCursor)
    vals = tuple([tuple(x) for x in DataFrame.values])

    if len(result) == 4:
        logger.info('None data')
        return
    try:
        sql = "INSERT INTO " + MariaDB_table_name + " (" + column_name_list[2:] + ") VALUES(" + value[:-2] + ");"
        curs.executemany(sql, vals)
        conn.commit()
        logger.success("MariaDB insert Success")
    except:
        logger.debug('overlap occur')
        pass
    finally:
        conn.close()


def start(event_code, event_name):

    code = 'A' + event_code
    result = dict()
    MariaDB_table_name = 'test_FnGuide_Crawling'
    logger.debug('FnGuide Crawling '+code+' round start')
    page1, page2, page3, page4 = html_extraction(code)
    result = page1_parsing(page1, result, event_code, event_name)
    result = page2_parsing(page2, result, event_code, event_name)
    result = page3_parsing(page3, result, event_code, event_name)

    if len(result) == 4:
        logger.info('None data')
        return
    else:
        mongoDB_insert(result)
        MariaDB_insert(result,MariaDB_table_name)
        return result


def write_txt(list, fname, sep):
    file = open(fname, 'w')
    vstr = ''
    for a in list:
        vstr = vstr + str(a) + sep
    vstr = vstr.rstrip(sep)  # 마지막에도 추가되는  sep을 삭제
    file.writelines(vstr)  # 한 라인씩 저장
    file.close()
    logger.success('error code file save success')

if __name__ == '__main__':

    logger.add(log_config.filename + '_{time}.log', rotation=log_config.rotation, enqueue=True, encoding="utf8")
    try:
        KRX = KRX_code()
        code_list = list(KRX['종목코드'])
        code_name_list = list(KRX['종목명'])
        pool_list = list()
        for code,name in zip(code_list,code_name_list):
            pool_list.append([code,name])
        logger.success('KRX Crawling success')
    except:
        logger.error('KRX Crawling error')

    logger.info('event all count:' + str(len(code_list)))
    error_code = []

    for codes in tqdm(pool_list,desc='iterate list'):
        try:
            result = start(str(codes[0]),codes[1])
            result = pd.DataFrame(result)
            result.to_csv('FnGuide_crawling_result\\' + str(codes[0]) +'.csv' ,encoding='utf-8')
        except:
            logger.error('error occur event code:'+ str(codes[0]))
            error_code.append( str(codes[0]))

    write_txt(error_code, 'erorr_code.txt', sep='\n')

