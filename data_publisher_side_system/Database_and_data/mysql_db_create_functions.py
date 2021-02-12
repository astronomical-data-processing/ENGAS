#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import sys
import os
import time
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
from commonUtils import iso8601Conversion
#from DBConnectionPool import MySQL_config
from main_config import mySQL_conf as MySQL_config
from Database_and_data import sql


def create_database():
    print('creating the database (syncfile)')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host,port=MySQL_config.db_port,user=MySQL_config.db_user,passwd=MySQL_config.db_user_passwd,charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(sql.syncfile_db_create_sql)
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
    print('The database (syncfile) has been created')


def create_subscriber_table_for_data_provider():
    print('creating the subscriber table(sync_subscribers) for data provider')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name, user=MySQL_config.db_user,passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(sql.syncfile_create_subscriber_table_for_data_provider)
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
    print('the subscriber table (sync_subscribers) has been created')


def create_backlog_file_table_for_data_provider(backlog_file_table_name):
    print('creating the backlog file table for data provider')
    create_backlog_file_table_sql = sql.syncfile_create_backlog_file_table_for_provider % (backlog_file_table_name)
    try_flag = False
    try_count = 0
    while (not try_flag) and (try_count < 5):
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name, user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(create_backlog_file_table_sql)
            conn_syncfile.commit()
            cursor_syncfile.close()
            conn_syncfile.close()
            try_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def create_backlog_file_table_for_data_provider() -> operational_error:',
                  operational_error)
            try_count = try_count + 1
            try:
                if conn_syncfile:
                    conn_syncfile.rollback()
            except Exception as exception_error:
                print('def create_backlog_file_table_for_data_provider() -> operational_error -> exception_error',
                    exception_error)
            continue
        except Exception as exception_error:
            try_count = try_count + 1
            print('def create_backlog_file_table_for_data_provider() -> exception_error', exception_error)
            continue
    if try_count < 5:
        print('the backlog file table has been created')
        return True
    else:
        print('the backlog file table has been created unsuccessfully')
        return False


def drop_if_exist_backlog_file_table_for_data_provider(backlog_file_table_name):
    print('If exists, drop the backlog file table for data provider')
    drop_if_exist_backlog_file_table_sql = sql.syncfile_drop_exist_backlog_file_table_for_provider % (backlog_file_table_name)
    print('drop_if_exist_backlog_file_table_sql',drop_if_exist_backlog_file_table_sql)
    # create_backlog_file_table_sql = sql.syncfile_drop_exist_backlog_file_table_for_provider
    try_flag = False
    try_count = 0
    while (not try_flag) and (try_count < 5):
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name, user=MySQL_config.db_user,
                                            passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(drop_if_exist_backlog_file_table_sql)
            conn_syncfile.commit()
            cursor_syncfile.close()
            conn_syncfile.close()
            try_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def drop_exist_create_backlog_file_table_for_data_provider() -> operational_error:', operational_error)
            try_count = try_count + 1
            try:
                if conn_syncfile:
                    conn_syncfile.rollback()
            except Exception as exception_error:
                print('def drop_exist_create_backlog_file_table_for_data_provider() -> operational_error -> exception_error', exception_error)
            continue
        except Exception as exception_error:
            try_count = try_count + 1
            print('def drop_exist_create_backlog_file_table_for_data_provider() -> exception_error', exception_error)
            continue
    if try_count < 5:
        return True
    else:
        return False


def create_publisher_table_for_data_consumer():
    print('creating the subscriber table(sync_publishers) for data consumer')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name, user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(sql.syncfile_create_publisher_table_for_consumer)
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
    print('the subscriber table (sync_publishers) has been created')


def create_received_file_table_for_data_consumer(received_file_table_name):
    print('creating the received file table for data consumer')
    create_received_file_table_sql = sql.syncfile_create_recieved_file_table_for_consumer % (received_file_table_name)
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name, user=MySQL_config.db_user,
                                            passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(create_received_file_table_sql)
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
    print('the received file table has been created')


def create_server_running_status_table_in_data_provider_side():
    print('creating the server running status table in data provider side')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name, user=MySQL_config.db_user,
                                            passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(sql.syncfile_create_server_running_status_table_in_data_provider_side)
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
    print('the server running status table in data provider side has been created')


def create_allow_suicide_repair_flag_for_subscriber_table_in_data_consumer_side():
    print('creating the allow_suicide_repair_flag_for_subscriber table in data consumer side')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name, user=MySQL_config.db_user,
                                            passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(sql.syncfile_create_allow_suicide_repair_flag_for_subscriber_table_in_data_consumer_side)
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
    print('the allow_suicide_repair_flag_for_subscriber table in data consumer side has been created')


def create_server_running_status_table_in_data_consumer_side():
    print('creating the server running status table in data consumer side')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name, user=MySQL_config.db_user,
                                            passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(sql.syncfile_create_server_running_status_table_in_data_consumer_side)
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
    print('the server running status table in data consumer side has been created')


def create_ngas_files_test_table_for_syncfile_system():
    print('creating the ngas files test table for data store and synchronize system')
    try_flag = False
    while not try_flag:
        try:
            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                            db=MySQL_config.db_name, user=MySQL_config.db_user,
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


def main():
    create_database()
    create_subscriber_table_for_data_provider()
    create_publisher_table_for_data_consumer()
    create_server_running_status_table_in_data_provider_side()
    create_server_running_status_table_in_data_consumer_side()
    create_allow_suicide_repair_flag_for_subscriber_table_in_data_consumer_side()
    create_ngas_files_test_table_for_syncfile_system()

if __name__ == '__main__':
    main()



