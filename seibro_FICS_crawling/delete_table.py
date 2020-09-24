import pymysql

def drop_table(table_name):
    dbconn = pymysql.connect(
        host="(host(url or ip",
        user="ID",
        passwd="PW",
        database="table_name",
        charset='utf8')

    try:
        with dbconn.cursor() as cursor:
            sql = 'DROP TABLE  ' + table_name
            cursor.execute(sql)
        dbconn.commit()
    finally:
        dbconn.close()


drop_table('test_FICS_Industry_classification')