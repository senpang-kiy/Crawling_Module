import pymysql

dbconn = pymysql.connect(
    host="database-1.c1pvc7savwst.ap-northeast-2.rds.amazonaws.com",
    user="nsuser",
    passwd="nsuser))))",
    database="NewsSalad_dev_01",
    charset='utf8')

try:
    with dbconn.cursor(pymysql.cursors.DictCursor) as cursor:
        sql = '''
            CREATE TABLE test_FICS_Industry_classification (
                   std_dt char(8) NOT NULL,
                   event_code VARCHAR(10) NOT NULL,
                   event_name VARCHAR(25) NOT NULL,
                   Market varchar(8) NULL,
                   industry_Main_Category varchar(30) NULL,
                   industry_intermediate_classification varchar(30) NULL,
                   industry_subclass varchar(30) NULL,
                   listed_stock_issued_count bigint NULL,
                   market_cap bigint NULL,
                   insert_dttm datetime default sysdate() NULL,
                   PRIMARY KEY(std_dt,event_code)
                   );
        '''
        cursor.execute(sql)
        dbconn.commit()
finally:
    dbconn.close()