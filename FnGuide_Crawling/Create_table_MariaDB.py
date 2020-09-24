import mysql.connector;
import pymysql

dbconn = pymysql.connect(
        host="(host(url or ip)",
        user="ID",
        passwd="PW",
        database="table_name",
        charset='utf8')

try:
    with dbconn.cursor(pymysql.cursors.DictCursor) as cursor:
        sql = '''
            CREATE TABLE test_FnGuide_Crawling (
                   std_dt char(8) NOT NULL,
                   event_code VARCHAR(10) NOT NULL,
                   event_name VARCHAR(25) NOT NULL,
                   ROA FLOAT NULL,
                   ROE FLOAT NULL,
                   BPS FLOAT NULL,
                   DPS FLOAT NULL,
                   PBR FLOAT NULL,
                   NOSI FLOAT NULL,	
                   touch FLOAT NULL,	
                   touch_1Y FLOAT NULL,	
                   touch_IR FLOAT NULL,
                   touch_AML FLOAT NULL,	
                   EBITDA FLOAT NULL,
                   EBITDA_1Y FLOAT NULL,	
                   EBITDA_IR FLOAT NULL,
                   EBITDA_AML FLOAT NULL,	
                   EPS FLOAT NULL,
                   Net_Income FLOAT NULL,
                   EPS_1Y FLOAT NULL,	
                   EPS_IR FLOAT NULL,
                   EPS_AML FLOAT NULL,	
                   COS FLOAT NULL,
                   Gross_margin FLOAT NULL,	
                   SAAE FLOAT NULL,
                   NPOCS FLOAT NULL,	
                   NIONS FLOAT NULL,	
                   AOTAIA FLOAT NULL,	
                   Assets FLOAT NULL,
                   Current_assets FLOAT NULL,	
                   Inventory FLOAT NULL,
                   TRAOCR FLOAT NULL,
                   liabilities FLOAT NULL,	
                   Current_liabilities FLOAT NULL,	
                   capital FLOAT NULL,
                   SOTCC FLOAT NULL,
                   Depreciation FLOAT NULL,	
                   AOIS FLOAT NULL,
                   insert_dttm datetime default sysdate() NULL, 
                   PRIMARY KEY(std_dt,event_code)
            );
        '''
        cursor.execute(sql)
        dbconn.commit()
finally:
    dbconn.close()
