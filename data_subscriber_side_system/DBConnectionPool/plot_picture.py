#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import matplotlib as mpl

mpl.use('Agg')
import matplotlib.pyplot as plt
import sys
import os

sys.path.insert(0, os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep)
from DBConnectionPool import MySQL_config
from commonUtils import iso8601Conversion

'''
    sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
    sys.path.insert(0, '/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
    将'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/'加入系统目录，这样在本文件中就可以引用该目录下的子模块中导入相应的模块
'''


def create_and_init_table_for_ngas_files_test():
    query_received_files_sql = '''select ingestion_date from received_10_10_10_101_1024 order by ingestion_date'''
    query_ngas_files_sql = '''select ingestion_date from ngas_files_test order by ingestion_date'''
    query_received_files_sql_flag = False
    while not query_received_files_sql_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name,
                                            user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                            charset=MySQL_config.db_charset)
            conn_syncfile.ping(True)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_received_files_sql)
            row_count_received = cursor_syncfile.rowcount
            results_received = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            query_received_files_sql_flag = True
        except MySQLdb.OperationalError:
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            continue
        except Exception as e:
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            continue
    '''ngas'''
    query_ngas_files_sql_flag = False
    while not query_ngas_files_sql_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name,
                                            user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                            charset=MySQL_config.db_charset)
            conn_syncfile.ping(True)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_ngas_files_sql)
            row_count_ngas = cursor_syncfile.rowcount
            results_ngas = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            query_ngas_files_sql_flag = True
        except MySQLdb.OperationalError:
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            continue
        except Exception as e:
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            continue
    interval_adjacent_received = []
    ingestion_rate_received = []
    for i in range(row_count_received):
        interval_value = float(results_received[i][0]) - float(results_received[0][0])
        interval_adjacent_received.append(interval_value)
        if interval_value == 0.0:
            if i == 0:
                ingestion_rate_received.append(0.0)
            else:
                ingestion_rate_received.append(ingestion_rate_received[i - 1])
        else:
            ingestion_rate_received.append(i * 201600 / 1024.0 / 1024 / interval_value)
    ingestion_rate_received[0] = ingestion_rate_received[1]
    '''ngas'''
    interval_adjacent_ngas = []
    ingestion_rate_ngas = []
    for i in range(row_count_ngas):
        interval_value = float(results_ngas[i][0]) - float(results_ngas[0][0])
        interval_adjacent_ngas.append(interval_value)
        if interval_value == 0.0:
            if i == 0:
                ingestion_rate_ngas.append(0.0)
            else:
                ingestion_rate_ngas.append(ingestion_rate_ngas[i - 1])
        else:
            ingestion_rate_ngas.append(i * 201600 / 1024.0 / 1024 / interval_value)
    ingestion_rate_ngas[0] = ingestion_rate_ngas[1]

    received_plot = plt.subplot(122)
    received_plot.set_title('sync_method based on ZeroMQ', fontsize=10)
    received_plot.set_xlabel('Elapsed time (second)', fontsize=10)
    received_plot.set_ylabel('Average ingestion rate (MB/second)', fontsize=10)
    received_plot.plot(interval_adjacent_received, ingestion_rate_received, 'k')
    ngas_plot = plt.subplot(121)
    ngas_plot.set_title('sync_method of NGAS', fontsize=10)
    ngas_plot.set_xlabel('Elapsed time (second)', fontsize=10)
    ngas_plot.set_ylabel('Average ingestion rate (MB/second)', fontsize=10)
    ngas_plot.plot(interval_adjacent_ngas, ingestion_rate_ngas, 'k')
    plt.savefig('received_r_8.pdf')
    plt.show()

    # for results_item in results:


if __name__ == '__main__':
    create_and_init_table_for_ngas_files_test()

