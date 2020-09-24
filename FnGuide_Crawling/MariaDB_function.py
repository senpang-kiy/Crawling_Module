import pandas as pd
import pymysql


def drop_table(table_name):
    dbconn = pymysql.connect(
        host="(host(url or ip)",
        user="ID",
        passwd="PW",
        database="table_name",
        charset='utf8')

    try:
        with dbconn.cursor() as cursor:
            sql = 'DROP TABLE  '+ table_name
            cursor.execute(sql)
        dbconn.commit()
    finally:
        dbconn.close()

drop_table('test_FnGuide_Crawling')


def MariaDB_insert(DataFrame, MariaDB_table_name):

    DataFrame = DataFrame.drop('_id', axis=1)
    print(DataFrame)
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

    try:
        sql = "INSERT INTO " + MariaDB_table_name + " (" + column_name_list[2:] + ") VALUES(" + value[:-2] + ");"
        print(sql)
        curs.executemany(sql, vals)
        conn.commit()
        print(curs.rowcount, "records were inserted.")

    finally:
        conn.close()