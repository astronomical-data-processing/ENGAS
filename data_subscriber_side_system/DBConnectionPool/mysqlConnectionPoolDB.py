#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from MySQLdb.cursors import DictCursor
# pip install DBUtils==1.4
from DBUtils.PooledDB import PooledDB
import sys
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
# import MySQL_config
from main_config import mySQL_conf as MySQL_config


class Mysql(object):
    __pool = None

    def __init__(self):
        if not Mysql.__pool:
            '''
                Mysql.__pool = PooledDB(creator=MySQLdb, mincached=1, maxcached=20, host=MySQL_config.db_host, port=MySQL_config.db_port, user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd, db=MySQL_config.db_name, use_unicode=False, charset=MySQL_config.db_charset, cursorclass=DictCursor)
                Using cursorclass=DictCursor, query_result is dictionary: {'running_status': 1L}
                Without using cursorclass=DictCursor, query_result is tuple: (1L,)

            '''
            Mysql.__pool = PooledDB(creator=MySQLdb, mincached=1, maxcached=100000, host=MySQL_config.db_host, port=MySQL_config.db_port, user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd, db=MySQL_config.db_name, use_unicode=False, charset=MySQL_config.db_charset)
            #Mysql.__pool = PooledDB(creator=MySQLdb, host=MySQL_config.db_host, port=MySQL_config.db_port, user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd, db=MySQL_config.db_name, use_unicode=False, charset=MySQL_config.db_charset)

    def pool(self):
        return Mysql.__pool
