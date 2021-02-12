#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import sys
# sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os

sys.path.insert(0, os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep)
import time
import copy
import random
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import multiprocessing
import threading
import queue as Queue
from DBConnectionPool import mysqlConnectionPoolDB
# from DBConnectionPool import MySQL_config
from main_config import localhost_conf
from server_for_subscribe_service import ServerTask
import publish_functions


def main():
    pool_syncfile = mysqlConnectionPoolDB.Mysql().pool()

    constant_dict = dict()
    # constant_dict['volume1'] = '/dev/shm/'
    # constant_dict['volume2'] = '/dev/shm/'
    # constant_dict['server_ip'] = '10.10.10.101'
    # constant_dict['server_port'] = 65535
    # constant_dict['process_feedback_info_worker_number'] = 3

    constant_dict['volume1'] = localhost_conf.ngas_volume1
    constant_dict['volume2'] = localhost_conf.ngas_volume2
    constant_dict['server_ip'] = localhost_conf.engas_server_ip
    constant_dict['server_port'] = localhost_conf.engas_server_port
    constant_dict['process_feedback_info_worker_number'] = localhost_conf.engas_process_feedbck_info_worker_number
    constant_dict['engas_timeout_republish'] = localhost_conf.engas_timeout_republish

    stop_publish_service_event = multiprocessing.Event()
    random_wait_event = multiprocessing.Event()
    new_comer_unsubscribe_event = multiprocessing.Event()

    assign_server_running_status(pool_syncfile)
    new_comer_unsubscribe_event.set()
    clear_unsubscribe_record_and_backlog_file(pool_syncfile)

    start_data_publish_server_for_subscribers(pool_syncfile, constant_dict, stop_publish_service_event,
                                              new_comer_unsubscribe_event, random_wait_event)

    start_subscriber_request_server_thread(pool_syncfile, constant_dict, stop_publish_service_event)

    start_new_comer_thread(pool_syncfile, constant_dict, stop_publish_service_event,
                           new_comer_unsubscribe_event, random_wait_event)

    start_stop_server_running_thread(pool_syncfile, constant_dict, random_wait_event, stop_publish_service_event)


# def assign_server_running_status(pool_syncfile, server_ip='10.10.10.101', server_port=65535):
def assign_server_running_status(pool_syncfile, server_ip=localhost_conf.engas_server_ip, server_port=localhost_conf.engas_server_port):
    query_sql = '''SELECT * FROM server_running_status WHERE running_status=1 AND server_ip=%s AND server_port=%s'''
    replace_sql = '''REPLACE INTO server_running_status(running_status, server_ip, server_port) VALUES(%s, %s, %s)'''

    query_sql_flag = False
    while not query_sql_flag:
        try:
            conn_syncfile = pool_syncfile.connection()
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_sql, (server_ip, server_port))
            query_rowcount = cursor_syncfile.rowcount
            cursor_syncfile.close()
            conn_syncfile.close()
            query_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def assign_server_running_status operational_error', operational_error)
            continue
        except Exception as exception_error:
            print('def assign_server_running_status exception_error', exception_error)
            continue
    if query_rowcount == 1:
        print('The server is running and cannot start a new server.')
        sys.exit()
    else:
        replace_sql_flag = False
        while not replace_sql_flag:
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(replace_sql, (1, server_ip, server_port))
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                replace_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def assign_server_running_status operational_error', operational_error)
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('def assign_server_running_status operational_error -> exception_error', exception_error)
                continue
            except Exception as exception_error:
                print('def assign_server_running_status exception_error', exception_error)
                continue
    '''
        server running status in data provider side:
            1: running
            0: stopped
    '''


def clear_unsubscribe_record_and_backlog_file(pool_syncfile):
    """
        The subscriber has two subscribed status: 0, 1 and 2.
            0 indicates unsubscribed.
            1 indicates running.
            2 indicates new_comer.
    """
    retrieval_unsubscriber_sql = '''SELECT subscriber_ip, subscriber_port FROM sync_subscribers WHERE subscribe_status=0'''
    delete_unsubscriber_record_sql = '''DELETE FROM sync_subscribers WHERE subscriber_ip=%s AND subscriber_port=%s'''
    drop_backlog_sql = '''DROP TABLE IF EXISTS %s'''
    '''
        retrieve and obtain the subscriber_ip and subscriber_port of the unsubscribers.
        drop the backlog file table corresponding to the unsubscriber with the subscriber_ip and subscriber_port.
        delete the record corresponding to the unsubscriber with the subscriber_ip and subscriber_port from the table (sync_subscriber).
    '''
    retrieval_unsubscriber_sql_flag = False
    while not retrieval_unsubscriber_sql_flag:
        try:
            conn_syncfile = pool_syncfile.connection()
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(retrieval_unsubscriber_sql)
            retrieval_unsubscriber_results = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            retrieval_unsubscriber_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def clear_unsubscribe_record_and_backlog_file operational_error', operational_error)
            continue
        except Exception as exception_error:
            print('def clear_unsubscribe_record_and_backlog_file exception_error', exception_error)
            continue

    for unsubscriber_record in retrieval_unsubscriber_results:
        if type(unsubscriber_record[0]) is bytes:
            unsubscriber_ip = str(unsubscriber_record[0],encoding='utf-8')
        if unsubscriber_ip.startswith('b'):
            unsubscriber_ip = unsubscriber_ip[2:-1]
        unsubscriber_port = unsubscriber_record[1]
        unsubscriber_IpPort = unsubscriber_ip + '_' + str(unsubscriber_port)
        backlog_file_table_name = 'backlog_' + unsubscriber_IpPort.replace('.', '_')
        drop_backlog_file_sql = drop_backlog_sql % (backlog_file_table_name)
        drop_backlog_file_sql_flag = False

        while not drop_backlog_file_sql_flag:
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(drop_backlog_file_sql)
                cursor_syncfile.execute(delete_unsubscriber_record_sql,(unsubscriber_ip, int(unsubscriber_port)))
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                drop_backlog_file_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def clear_unsubscribe_record_and_backlog_file operational_error', operational_error)
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('def clear_unsubscribe_record_and_backlog_file operational_error -> exception_error', exception_error)
                continue
            except Exception as exception_error:
                print('def clear_unsubscribe_record_and_backlog_file exception_error', exception_error)
                continue


def start_data_publish_server_for_subscribers(pool_syncfile, constant_dict, stop_publish_service_event,
                                              new_comer_unsubscribe_event, random_wait_event):
    retrieval_subscriber_sql = '''SELECT subscriber_ip, subscriber_port, publisher_ip, publisher_port, subscriber_start_date, last_file_sync_date,subscribe_status FROM sync_subscribers WHERE subscribe_status=1'''
    ngas_retrieval_sql = '''SELECT file_name, file_id, file_version, format FROM ngas_files_test WHERE ingestion_date >= %s'''
    update_sql = '''UPDATE sync_subscribers SET last_file_sync_date=%s WHERE subscriber_ip=%s AND subscriber_port=%s'''
    engas_timeout_republish = constant_dict['engas_timeout_republish']
    '''
        retrieve and obtain subscribers (if exist) from sync_subscribers.
    '''
    retrieval_subscriber_sql_flag = False
    while not retrieval_subscriber_sql_flag:
        try:
            conn_syncfile = pool_syncfile.connection()
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(retrieval_subscriber_sql)
            row_count = cursor_syncfile.rowcount
            retrieval_results = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            retrieval_subscriber_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def start_publish_threads operational_error', operational_error)
            continue
        except Exception as exception_error:
            print('def start_publish_threads exception_error', exception_error)
            continue

    if row_count == 0:
        '''
            there is no subscriber in the table (sync_subscribers).
        '''
        print('there is no subscriber!')
    else:
        '''
            There exist subscribers in the table (sync_subscribers).
        '''
        for publisher_record in retrieval_results:
            subscriber_ip = str(publisher_record[0],encoding='utf-8')
            subscriber_port = int(publisher_record[1])
            publisher_ip = str(publisher_record[2],encoding='utf-8')
            publisher_port_string = str(publisher_record[3],encoding='utf-8')
            last_file_sync_date = float(publisher_record[5])
            subscribe_status = int(publisher_record[6])
            subscriber_IpPort = subscriber_ip + '_' + str(subscriber_port)

            filename_queue = multiprocessing.Queue()  # item in queue:(file_name, file_id, file_version, format)

            pub_event = multiprocessing.Event()
            unsubscribe_event = multiprocessing.Event()
            receive_event = threading.Event()
            receive_event.set()

            feedback_info_queue = Queue.Queue()

            """
                retrieve the records from backlog_file_table. That is to say, the publisher does not receive the feedback information from subscriber over 1 hour.
                republish these records.
                update the time(change_status_timestamp) in the backlog_files_table.
            """
            backlog_table_name = 'backlog_' + subscriber_IpPort.replace('.', '_')
            retrieval_sql = '''SELECT file_name, file_id, file_version, format FROM %s''' % (backlog_table_name)
            retrieval_sql_syncfile = retrieval_sql + ''' ''' + '''WHERE file_status=1 AND change_status_timestamp<%s'''

            """
                When publisher does not receive the feedback information from subscriber over 1 hour, re-send the file to the subscriber
            """
            republish_time = time.time() - random.randint(600, 900)  # 86400
            retrieval_sql_syncfile_flag = False
            while not retrieval_sql_syncfile_flag:
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(retrieval_sql_syncfile, (republish_time,))
                    republish_files_results = cursor_syncfile.fetchall()
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    retrieval_sql_syncfile_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('def start_publish_threads operational_error', operational_error)
                    continue
                except Exception as exception_error:
                    print('def start_publish_threads exception_error', exception_error)
                    continue

            update_sql_arguments = []
            for queue_element in republish_files_results:
                filename_queue.put(queue_element)
                queue_element_deepcopy = copy.deepcopy(queue_element)
                queue_element_deepcopy_list = list(queue_element_deepcopy)
                queue_element_deepcopy_list.insert(0, time.time())
                update_sql_arguments.append(tuple(queue_element_deepcopy_list))

            update_republish_origin_sql = "UPDATE %s" % (backlog_table_name)
            update_republish_sql = update_republish_origin_sql + " " + "SET change_status_timestamp=%s WHERE file_name=%s AND file_id=%s AND file_version=%s AND format=%s"
            update_republish_sql_flag = False
            while not update_republish_sql_flag:
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.executemany(update_republish_sql, update_sql_arguments)
                    conn_syncfile.commit()
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    update_republish_sql_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('def start_publish_threads operational_error', operational_error)
                    try:
                        if conn_syncfile:
                            conn_syncfile.rollback()
                    except Exception as exception_error:
                        print('def start_publish_threads operational_error -> exception_error', exception_error)
                    continue
                except Exception as exception_error:
                    print('def start_publish_threads exception_error', exception_error)
                    continue

            '''
                retrieve ngas_files from ngas_files_test where ingestion_date >= last_file_sync_date.
                put item in the ngas_files into the queue
            '''
            ngas_retrieval_sql_flag = False
            while not ngas_retrieval_sql_flag:
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(ngas_retrieval_sql, (last_file_sync_date,))
                    ngas_files = cursor_syncfile.fetchall()
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    ngas_retrieval_sql_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('def start_publish_threads operational_error', operational_error)
                    continue
                except Exception as exception_error:
                    print('def start_publish_threads exception_error', exception_error)
                    continue

            temp_last_file_sync_date = time.time()

            for queue_element in ngas_files:
                filename_queue.put(queue_element)

            '''
                insert ngas_files (backlog file) retrieved from ngas_files_test into the backlog_file_table with the query condition:last_file_sync_date.  
            '''
            newly_added_backlog_file_list = map(list, ngas_files)
            for newly_added_backlog_file_list_item in newly_added_backlog_file_list:
                newly_added_backlog_file_list_item.append(1)
                newly_added_backlog_file_list_item.append(time.time())
            newly_added_backlog_file_tuple = list(map(tuple, newly_added_backlog_file_list))
            syncfiles_backlog_sql = '''INSERT INTO %s (file_name, file_id, file_version, format, file_status, change_status_timestamp)''' % (
                backlog_table_name)
            insert_backlog_files_sql_syncfile = syncfiles_backlog_sql + ''' ''' + '''VALUES(%s, %s, %s, %s, %s, %s)'''
            insert_backlog_files_sql_syncfile_flag = False
            while not insert_backlog_files_sql_syncfile_flag:
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.executemany(insert_backlog_files_sql_syncfile, newly_added_backlog_file_tuple)
                    conn_syncfile.commit()
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    insert_backlog_files_sql_syncfile_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('def start_publish_threads operational_error', operational_error)
                    try:
                        if conn_syncfile:
                            conn_syncfile.rollback()
                    except Exception as exception_error:
                        print('def start_publish_threads operational_error -> exception_error', exception_error)
                    continue
                except Exception as exception_error:
                    print('def start_publish_threads exception_error', exception_error)
                    continue

            '''
                #UPDATE sync_subscribers SET last_file_sync_date = temp_last_file_sync_date WHERE subscriber_ip=str(publisher_record[0]) AND subscriber_port=int(publisher_record[1])
            '''
            update_sql_flag = False
            while not update_sql_flag:
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(update_sql, (temp_last_file_sync_date, subscriber_ip, subscriber_port))
                    conn_syncfile.commit()
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    update_sql_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('def start_publish_threads operational_error', operational_error)
                    try:
                        if conn_syncfile:
                            conn_syncfile.rollback()
                    except Exception as exception_error:
                        print('def start_publish_threads operational_error -> exception_error', exception_error)
                    continue
                except Exception as exception_error:
                    print('def start_publish_threads exception_error', exception_error)
                    continue

            # start update_backlog_and_queue_thread
            """
                def update_backlog_and_queue_thread(pool_syncfile,publishers_dict,random_wait_event,stop_event):
                create the update_backlog_and_queue_thread
                start the update_backlog_and_queue_thread
            """
            update_backlog_and_queue_thread = threading.Thread(
                target=publish_functions.update_backlog_and_queue_function,
                args=(pool_syncfile, subscriber_ip, subscriber_port, engas_timeout_republish, filename_queue, unsubscribe_event, pub_event,
                    stop_publish_service_event, random_wait_event, receive_event))
            update_backlog_and_queue_thread.start()
            publish_port_list = list(map(int, publisher_port_string.split(',')))

            # start poll_and_handle_unsubscribe_thread
            poll_and_handle_unsubscribe_thread = threading.Thread(
                target=publish_functions.poll_and_handle_unsubscribe,
                args=(pool_syncfile, stop_publish_service_event, new_comer_unsubscribe_event, random_wait_event,
                      subscriber_ip, subscriber_port, publisher_ip, unsubscribe_event, receive_event))
            poll_and_handle_unsubscribe_thread.start()

            # start receive_feedback_info_thread
            receive_feedback_info_thread = threading.Thread(
                target=publish_functions.receive_feedback_info,
                args=(subscriber_ip, subscriber_port, feedback_info_queue, receive_event, stop_publish_service_event))
            receive_feedback_info_thread.start()

            # start process_feedback_info_thread
            for _ in range(int(constant_dict['process_feedback_info_worker_number'])):
                process_feedback_info_thread = threading.Thread(
                    target=publish_functions.process_feedback_info,
                    args=(receive_event, feedback_info_queue, pool_syncfile, stop_publish_service_event, subscriber_ip,
                          subscriber_port))
                process_feedback_info_thread.start()

            for publish_port in publish_port_list:

                # create and start fetch_and_publish_data_process
                fetch_and_publish_data_process = multiprocessing.Process(
                    target=publish_functions.fetch_and_publish_data,
                    args=(filename_queue, pool_syncfile, publisher_ip, publish_port, subscriber_ip, subscriber_port,
                          pub_event, constant_dict, stop_publish_service_event, unsubscribe_event))
                fetch_and_publish_data_process.start()

                # create and start poll_pub_event_thread
                poll_pub_event_thread = threading.Thread(
                    target= publish_functions.poll_pub_event,
                    args=(publish_port, subscriber_ip, pub_event, stop_publish_service_event, unsubscribe_event,
                          random_wait_event))
                poll_pub_event_thread.start()


def start_stop_server_running_thread(pool_syncfile, constant_dict, random_wait_event, stop_publish_service_event):
    stop_server_running_thread = threading.Thread(
        target=publish_functions.stop_server_running,
        args=(pool_syncfile, constant_dict, random_wait_event, stop_publish_service_event))
    stop_server_running_thread.start()


def start_subscriber_request_server_thread(pool_syncfile, constant_dict, stop_publish_service_event):
    server_reply = ServerTask(pool_syncfile, constant_dict, stop_publish_service_event)
    server_reply.start()


def start_new_comer_thread(pool_syncfile, constant_dict, stop_publish_service_event,
                           new_comer_unsubscribe_event, random_wait_event):

    new_comer_thread_process = threading.Thread(
        target=publish_functions.new_comer_publish_and_feedback_thread,
        args=(pool_syncfile, constant_dict, stop_publish_service_event, new_comer_unsubscribe_event,
              random_wait_event))
    new_comer_thread_process.start()


if __name__ == '__main__':
    '''
    未实现的功能：
    1、启动服务时，检查配置的publisher_ip与sync_subscribers（订阅者表）中的publisher_ip是否一致，
        不一致则不启动对应记录的服务，并将该异常记录写入异常日志文件（exception_log）中

    2、启动服务时，检查sync_subscribers（订阅者表）中的subscriber_ip(订阅者IP)是否合法，
        非法则不启动相应的服务，并将该异常记录写入异常日志文件（exception_log）中. 
        这样做的目的是阻止运行之前将sync_subscribers（订阅者表）中的合法的IP修改为hostname这样的非法IP

    3、插入新增的积压文件记录到积压文件表时，插入不成功，则说明积压文件表已经被恶意删除或者恶意修改，
        重新创建积压文件，并将该异常记录写入异常日志文件（exception_log）中. 
        这样做的目的是保持服务器能够正常运行且可以记录下异常信息
    '''
    main()