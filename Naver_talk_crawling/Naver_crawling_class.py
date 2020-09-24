from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
from multiprocessing import Pool
import pymongo
import urllib
from loguru import logger
from config import log_config

class Naver_crawling:
    def __init__(self):

        self.pool_list_maker()
        self.start()

    def pool_list_maker(self):
        KRX_df = pd.read_excel('/home/2digit/tmp/seibro_FICS_datafile/2Digit_GICS(seibro).xlsx') # seibro 산업분류 파일 경로
        code_list = KRX_df['code']
        code_name_list = KRX_df['event_name']
        counts = 0
        all_count = len(KRX_df)
        self.pool_list = []

        for code, code_name in zip(code_list, code_name_list):
            counts += 1
            self.pool_list.append([code, code_name, counts, all_count])


    def talk_search(self,pools):

        code = pools[0]
        code_name = pools[1]
        countss = pools[2]
        all_count = pools[3]

        # DB init
        HOSTNAME = '172.31.20.198'
        PORT = 27017
        username = urllib.parse.quote_plus('test')
        password = urllib.parse.quote_plus('test12!@')
        connection = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (username, password, HOSTNAME, PORT))
        DB = connection['test_talk_03']
        DB_insert = DB['naver_talks']

        count = 0
        page = 0

        while True:

            time.sleep(0.1)
            page += 1
            retry = 0
            while 10 > retry:
                try:
                    header = {
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
                    }
                    data_ = {
                        'code': code,
                        'page': page
                    }
                    req = requests.post('https://finance.naver.com/item/board.nhn', params=data_, headers=header)

                    if str(req.status_code) == '200':
                        logger.info('event code:' + str(code) +'/Communication status:' + str(req))
                        break
                    else:
                        logger.info('event code:' + str(code) +'/Communication status:' + str(req))
                        time.sleep(30)
                        retry += 1
                        pass
                except:
                    logger.info('event code:' + str(code) +'/Communication status:' + str(req))
                    time.sleep(30)
                    retry += 1
                    pass


            soup = BeautifulSoup(req.content, 'html.parser')
            table = soup.find_all('table', attrs={'class': 'type2'})[0].find_all('tr', attrs={'align': 'center'})
            cc = 0

            if len(table) == 0 or len(table) == None:
                return
            else:
                pass

            for i in range(len(table)):

                talk_data = {'code': ' ', 'code_name': ' ', 'url': ' ', 'title': ' ', 'user': ' ', '_id': ' ',
                             'day': ' ', 'time': ' ', 'body': ' ', 'good_count': ' ', 'bad_count': ' '}

                URL = 'finance.naver.com' + str(table[i].find_all('a')[0]['href'])
                href = table[i].find_all('a')[0]['href']
                title = table[i].find_all('a')[0]['title']
                nid_start_index = href.index('&nid=')
                nid_last_index = href.index('&st')
                nid = href[nid_start_index + 5: nid_last_index]  # nid
                date = table[i].find_all('span')[0].text
                time_ = date[11:]
                day = date[:10]
                user = table[i].find_all('td', attrs={'class': 'p11'})[0].text.strip()
                good_count = table[i].find_all('strong', attrs={'class': "tah"})[0].text.strip()
                bad_count = table[i].find_all('strong', attrs={'class': "tah"})[1].text.strip()

                count += 1
                talk_data['_id'] = nid
                talk_data['url'] = 'https://' + URL
                talk_data['title'] = title
                talk_data['user'] = user
                talk_data['day'] = day
                talk_data['time'] = time_
                talk_data['code'] = str(code)
                talk_data['code_name'] = code_name
                talk_data['good_count'] = good_count
                talk_data['bad_count'] = bad_count

                while 10 > retry:
                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36'}
                        data_ = {'nid': nid}
                        r = requests.get('https://finance.naver.com/item/board_read.nhn', headers=headers, params=data_)
                        if str(r.status_code) == '200':
                            break
                        else:
                            time.sleep(30)
                            retry += 1
                            pass
                    except:
                        time.sleep(30)
                        retry += 1
                        pass

                r = requests.get('https://finance.naver.com/item/board_read.nhn', headers=headers, params=data_)
                soup = BeautifulSoup(r.content, 'html.parser')
                res = soup.find_all('div', attrs={'id': 'body'})

                if len(res) == 0:
                    try:
                        DB_insert.insert_one(talk_data)
                        cc += 1
                        pass
                    except:
                        logger.success('Now crawling page:'+str(page)+'/event code:'+ str(code)+'/company name:'+str(code_name)+'/list count:'+str(countss)+'/progress:'+str(round(countss / all_count, 2) * 100) + '%'+'/crawlling_count:'+ str(cc)+'/status:done')
                        return
                else:
                    try:
                        ress = res[0].text.replace('\r', ' ').replace('\n', ' ')
                        talk_data['body'] = ress
                        DB_insert.insert_one(talk_data)
                        cc += 1
                        pass
                    except:
                        logger.success('Now crawling page:'+str(page)+'/event code:'+ str(code)+'/company name:'+str(code_name)+'/list count:'+str(countss)+'/progress:'+str(round(countss / all_count, 2) * 100) + '%'+'/crawlling_count:'+ str(cc)+'/status:done')
                        return


    def start(self):

        import datetime
        logger.add(log_config.filename + '_{time}.log', rotation=log_config.rotation, enqueue=True, encoding="utf8")
        logger.debug('Naver talk Crawling round start')

        now = datetime.datetime.now()
        start = time.time()  # 시작 시간 저장

        HOSTNAME = 'Ip'
        PORT = 27017
        username = urllib.parse.quote_plus('ID')
        password = urllib.parse.quote_plus('PW')
        connection = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (username, password, HOSTNAME, PORT))
        DB = connection['test_talk_03']
        DB_insert = DB['naver_talks']

        start_count = DB_insert.count()
        p = Pool(8)
        p.map(self.talk_search,self.pool_list)
        p.close()

        time.sleep(2)
        end_count = DB_insert.count()

        work_time = time.time() - start
        talk_count = end_count - start_count
        logger.debug("%s/%s/%s %s:%s" % (now.year, now.month, now.day, now.hour, now.minute) + '/crawling count:'+str(talk_count), "/time:"+str(round(work_time / 60)))
        logger.debug('round end done')
        logger.complete()

        with open("output.txt", "a", encoding='utf-8') as f:
            print("%s/%s/%s     %s:%s   " % (now.year, now.month, now.day, now.hour, now.minute), 'crawling count:', talk_count, "  time:", round(work_time / 60), file=f)


if __name__ == '__main__':

    import datetime

    while True:
        Naver_crawling()






