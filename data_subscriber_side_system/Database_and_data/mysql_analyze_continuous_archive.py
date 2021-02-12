#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import sys
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
#from DBConnectionPool import MySQL_config
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
print(os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
from main_config import mySQL_conf as MySQL_config

'''
    sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
    sys.path.insert(0, '/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
    将'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/'加入系统目录，这样在本文件中就可以引用该目录下的子模块中导入相应的模块
'''


def get_average_speed():

    #query_sql = '''SELECT ingestion_date,ingestion_rate FROM ngas_files_test'''
    query_sql = '''SELECT ingestion_date FROM received_10_6_7_48_1024'''
    #query_sql = '''SELECT ingestion_date FROM ngas_files_test'''
    query_sql_flag = False
    while not query_sql_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name,
                           user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                           charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_sql)
            query_results = cursor_syncfile.fetchall()
            row_count = cursor_syncfile.rowcount
            cursor_syncfile.close()
            conn_syncfile.close()
            query_sql_flag = True
        except MySQLdb.OperationalError:
            if conn_syncfile:
                conn_syncfile.rollback()
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            continue
        except MySQLdb.InvalidConnection:
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            continue
        except Exception as _:
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            continue
    ingestion_date_list = []
    for ingestion_date in query_results:
        ingestion_date_list.append(ingestion_date[0])

    ingestion_date_list.sort()

    #sum_size = 66240*row_count/1024.0/1024
    sum_size = 66240*1999200/1024.0/1024
    print('sum_size:%f (GB)'%(201600*200000/1024.0/1024/1024))
    print(len(ingestion_date_list),ingestion_date_list[1])
    elapsed_time = ingestion_date_list[-251] - ingestion_date_list[253]
    print('The size:%f (MB),elapsed time:%s (s),average speed:%f (MB),files/s:%f'%(sum_size,elapsed_time,sum_size/elapsed_time,row_count/elapsed_time))

if __name__ == '__main__':
    get_average_speed()
