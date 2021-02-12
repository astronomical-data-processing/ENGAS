#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import sys
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
# from DBConnectionPool import MySQL_config
from main_config import mySQL_conf as MySQL_config
from commonUtils import iso8601Conversion
import sql


'''
    sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
    sys.path.insert(0, '/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
    将'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/'加入系统目录，这样在本文件中就可以引用该目录下的子模块中导入相应的模块
'''


def create_ngas_files_test_table_for_syncfile_system():
    print('creating the ngas files test table for data store and synchronize system')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.ngas_db_name, user=MySQL_config.db_user,
                                            passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(sql.syncfile_create_ngas_files_test_table_for_syncfile)
            conn_syncfile.commit()
            cursor_syncfile.close()
            conn_syncfile.close()
            try_flag = True
        except MySQLdb.OperationalError:
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
    print('the ngas files test table has been created')


def create_and_init_table_for_ngas_files_test():

    '''
        retrieve all archived file records from ngas_files
    '''
    query_ngas_files_sql = '''select * from ngas_files'''
    query_ngas_files_sql_flag = False
    while not query_ngas_files_sql_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.ngas_db_name,
                                            user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                            charset=MySQL_config.db_charset)
            conn_syncfile.ping(True)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_ngas_files_sql)
            results = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            query_ngas_files_sql_flag = True
        except MySQLdb.OperationalError:
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
    '''
        convert all archived file records retrieved from ngas_files
        insert the converted archived file records into the ngas_files_test with the MySQL
    '''
    def convert_string_to_float(x):
        x[8] = float(iso8601Conversion.fromiso8601(x[8]))
        return x

    results_convert = map(tuple, map(convert_string_to_float, map(list, results)))
    '''
        The format string is not really a normal Python format string.You must always use % s for all fields.
        也就是MySQLdb的字符串格式化不是标准的python的字符串格式化, 应当一直使用 % s用于字符串格式化
        
        遇到问题: TypeError: % d format: a number is required, not str
        解决办法: 传给sql的变量写对格式就行了.sql里不需要对对应的变量写 % d, 只写 % s就可以了
        
        _mysql_exceptions.ProgrammingError: (1064, "You havean error in your SQ L syntax; check the manual that corresponds 
    to your MySQLserver version for the right syntax to use near ')' at line 1")
    '''

    insert_sql = '''INSERT INTO ngas_files_test (disk_id,file_name,file_id,file_version,format,file_size,uncompressed_file_size,compression,ingestion_date,file_ignore,checksum,checksum_plugin,file_status,creation_date,container_id,ingestion_rate,io_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    insert_sql_flag = False
    while not insert_sql_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.ngas_db_name,
                                            user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                            charset=MySQL_config.db_charset)
            conn_syncfile.ping(True)
            print('test stoxxx')
            cursor_syncfile = conn_syncfile.cursor()
            print('cursor')
            cursor_syncfile.executemany(insert_sql, results_convert)
            print('executemay')
            conn_syncfile.commit()
            print('commit')
            cursor_syncfile.close()
            conn_syncfile.close()
            insert_sql_flag = True
        except MySQLdb.OperationalError:
            if conn_syncfile:
                conn_syncfile.rollback()
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

    print('Finished!')


if __name__ == '__main__':
    create_ngas_files_test_table_for_syncfile_system()
    create_and_init_table_for_ngas_files_test()
