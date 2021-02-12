#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import sys
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import IPy
# from DBConnectionPool import MySQL_config
from main_config import mySQL_conf as MySQL_config
from main_config import localhost_conf

def stop_client(client_ip):
    '''
        Multiple sets of publish and subscribe systems can be deployed on a single machine with different names for the database
    '''
    replace_sql = '''REPLACE INTO client_running_status(running_status,client_ip) VALUES(%s, %s)'''
    replace_sql_flag = False
    while not replace_sql_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name,
                                   user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                   charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(replace_sql,(0,client_ip))
            row_count = cursor_syncfile.rowcount
            conn_syncfile.commit()
            cursor_syncfile.close()
            conn_syncfile.close()
            replace_sql_flag = True
        except MySQLdb.OperationalError as error:
            try:
            	if conn_syncfile:
                    conn_syncfile.rollback()
            except UnboundLocalError as error_conn_syncfile:
                print(error_conn_syncfile)
            try:
            	if conn_syncfile:
                    conn_syncfile.close()
            except UnboundLocalError as error_conn_syncfile1:
                print(error_conn_syncfile1)
            #continue
            try:
                if cursor_syncfile:
                    cursor_syncfile.close()
            except UnboundLocalError as error_conn_syncfile2:
                print(error_conn_syncfile2)
            break
        except Exception as _:
            if cursor_syncfile:
                cursor_syncfile.close()
            if conn_syncfile:
                conn_syncfile.close()
            #continue

    if row_count == 1:
        print('There is no server(%s) running in client side!' % (client_ip))
    elif row_count == 2:
        print('Stopping the server running in client side!')

def is_ip(address):
    try:
        IPy.IP(address)
        return True
    except Exception as  e:
        return False


if __name__ == '__main__':
    # client_ip = '10.10.10.102'
    client_ip = localhost_conf.local_ip
    if len(sys.argv) == 1:
        stop_client(client_ip)
    elif len(sys.argv) == 2 and is_ip(sys.argv[1]):
        stop_client(sys.argv[1])
    else:
        print('specify the ip which needs to stop receiving data from the publisher!')
