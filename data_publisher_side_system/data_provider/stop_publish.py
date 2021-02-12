#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import sys
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
# from DBConnectionPool import MySQL_config
from main_config import mySQL_conf as MySQL_config
from main_config import localhost_conf

# def stop_server(server_ip='10.10.10.101', server_port=65535):
def stop_server(server_ip=localhost_conf.engas_server_ip, server_port=localhost_conf.engas_server_port):

    replace_sql = '''REPLACE INTO server_running_status(running_status, server_ip, server_port) VALUES(%s, %s, %s)'''
    replace_sql_flag = False
    try_count = 0
    while (not replace_sql_flag) and (try_count < 5):
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name,
                                   user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                   charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(replace_sql,(0, server_ip, server_port))
            conn_syncfile.commit()
            row_count = cursor_syncfile.rowcount
            cursor_syncfile.close()
            conn_syncfile.close()
            replace_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def stop_server() operational_error', operational_error)
            try:
                if conn_syncfile:
                    conn_syncfile.rollback()
            except Exception as exception_error:
                print('def stop_server() operational_error -> exception_error', exception_error)
            continue
        except Exception as exception_error:
            print('def stop_server() exception_error', exception_error)
            continue
    if try_count < 5:
        if row_count == 1:
            print('There is no server(%s:%d) running!' % (server_ip,server_port))
        elif row_count == 2:
            print('Stopping the server!')
    elif try_count == 5:
        print('execute the function of stop_server() failure!!!')


if __name__ == '__main__':
    stop_server()
