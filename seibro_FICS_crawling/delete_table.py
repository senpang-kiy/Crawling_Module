import pymysql

def drop_table(table_name):
    dbconn = pymysql.connect(
        host="database-1.c1pvc7savwst.ap-northeast-2.rds.amazonaws.com",
        user="nsuser",
        passwd="nsuser))))",
        database="NewsSalad_dev_01",
        charset='utf8')

    try:
        with dbconn.cursor() as cursor:
            sql = 'DROP TABLE  ' + table_name
            cursor.execute(sql)
        dbconn.commit()
    finally:
        dbconn.close()


drop_table('test_FICS_Industry_classification')