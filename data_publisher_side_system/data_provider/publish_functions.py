#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import zmq
import multiprocessing
import time
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import random
import copy
import threading
import sys
import queue as Queue
# import msgpack
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
from commonUtils import network_port_functions
from Database_and_data import mysql_db_create_functions


def fetch_and_publish_data(filename_queue, pool_syncfile, publisher_ip, publish_port, subscriber_ip,
                           subscriber_port, pub_event, constant_dict, stop_publish_service_event, unsubscribe_event):

    subscriber_IpPort = str(subscriber_ip) + '_' + str(subscriber_port)
    publisher_IpPort = str(publisher_ip) + '_' + str(publish_port)
    message_head = subscriber_IpPort + ':' + publisher_IpPort
    backlog_file_table_name = 'backlog_' + subscriber_IpPort.replace('.', '_')
    delete_sql = "DELETE FROM" + " " + str(backlog_file_table_name) + " " + "WHERE file_name=%s"

    context = zmq.Context()
    publish_data_socket = context.socket(zmq.PUB)

    bind_flag = False
    try_count = 0
    while (not bind_flag) and (try_count < 5):
        try:
            if type(publisher_ip) is bytes:
                publisher_ip = str(publisher_ip,encoding='utf-8')
            if publisher_ip.startswith('b'):
                publisher_ip = publisher_ip[2:-1]
            publish_data_socket.bind('tcp://%s:%d' % (str(publisher_ip), int(publish_port)))
            bind_flag = True
        except zmq.ZMQError as zmq_error:
            print('def fetch_and_publish_data() -> zmq.ZMQError: %s' %zmq_error)
            continue

    if try_count == 5:
        return

    """
        obtain the network connection status.
    """
    connected_flag = network_port_functions.localHost_connected_ip_success(publish_port, subscriber_ip)
    if connected_flag:
        # The publisher and subscriber have established a connection.
        # Trigger publish event.
        pub_event.set()

    volume1 = constant_dict['volume1']
    volume2 = constant_dict['volume2']
    # filename_shm = multiprocessing.current_process().name.split('-')[1] + '.fits'
    while (not stop_publish_service_event.is_set()) and (not unsubscribe_event.is_set()):

        if pub_event.is_set():
            try:
                backlog_file = filename_queue.get(timeout=3)
            except Exception as exception_error:
                # print('def fetch_data() -> filename_queue is empty -> %s' % exception_error)
                continue
            if type(backlog_file[0]) is bytes:
                backlog_filename = str(backlog_file[0],encoding='utf-8')
            if backlog_filename.startswith('b'):
                backlog_filename = backlog_filename[2:-1]
            file_path1 = volume1 + backlog_filename
            file_path2 = volume2 + backlog_filename
            # file_path1 = volume1 + str(backlog_file[0])
            # file_path2 = volume2 + str(backlog_file[0])
            # file_path1 = volume1 + filename_shm
            # file_path2 = volume2 + filename_shm

            if os.path.isfile(file_path1):
                '''
                    If the file corresponding to the backlog file record exists, read data from the file and send it to the subscriber.
                    send publish message format: {message_head:message_data}
                    send publish message_head format: subscriber_IpPort + ':' + publisher_IpPort
                                subscriber_IpPort: str(subscriber_ip) + '_' + str(subscriber_port)
                                publisher_IpPort: str(publisher_ip) + '_' + str(publisher_port)
                    send publish message_data format: {backlog_file: send_data}
                                backlog_file: (file_name, file_id, file_version, format)
                                send_data: read data from the file corresponding to the backlog_file.
                '''
                print('file_path1',file_path1)
                with open(file_path1, 'rb') as file_obj:
                    send_data = file_obj.read()
                    message_data = {backlog_file: send_data}
                    publish_data_socket.send_pyobj({message_head: message_data})
            elif os.path.isfile(file_path2):
                '''
                    If the file corresponding to the backlog file record exists, read data from the file and send it to the subscriber.
                    send publish message format: {message_head:message_data}
                    send publish message_head format: subscriber_IpPort + ':' + publisher_IpPort
                                subscriber_IpPort: str(subscriber_ip) + '_' + str(subscriber_port)
                                publisher_IpPort: str(publisher_ip) + '_' + str(publisher_port)
                    send publish message_data format: {backlog_file: send_data}
                                backlog_file: (file_name, file_id, file_version, format)
                                send_data: read data from the file corresponding to the backlog_file.
                '''
                print('file_path2', file_path2)
                with open(file_path2, 'rb') as file_obj:
                    send_data = file_obj.read()
                    message_data = {backlog_file: send_data}
                    publish_data_socket.send_pyobj({message_head: message_data})

            else:
                '''
                    The file corresponding to the backlog file record does not exist in the specified directory.
                '''
                delete_sql_execute_flag = False
                delete_sql_execute_count = 0
                while (not delete_sql_execute_flag) and (delete_sql_execute_count < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.execute(delete_sql, (backlog_filename,))
                        conn_syncfile.commit()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        delete_sql_execute_flag = True
                    except MySQLdb.OperationalError as operational_error:
                        print('operational_error', operational_error)
                        delete_sql_execute_count = delete_sql_execute_count + 1
                        try:
                            if conn_syncfile:
                                conn_syncfile.rollback()
                        except Exception as exception_error:
                            print('operational_error->exception_error', operational_error)
                        continue
                    except Exception as exception_error:
                        print('exception_error1', exception_error,delete_sql)
                        delete_sql_execute_count = delete_sql_execute_count + 1
                        continue

        elif (not pub_event.is_set()):
            time.sleep(1)


def poll_pub_event(publish_port, subscriber_ip, pub_event, stop_publish_service_event, unsubscribe_event, random_wait_event):

    while (not stop_publish_service_event.is_set()) and (not unsubscribe_event.is_set()):

        # obtain the network connection status.
        connected_flag = network_port_functions.localHost_connected_ip_success(publish_port, subscriber_ip)
        if connected_flag:
            # The publisher and subscriber have established a connection.
            # Trigger publish event.
            if pub_event.is_set():
                random_wait_event.wait(random.randint(10, 60))
            else:
                pub_event.set()
                random_wait_event.wait(random.randint(10, 60))

        else:
            # The publisher and subscriber have not established a connection.
            # clear the publish event.

            if not pub_event.is_set():
                random_wait_event.wait(random.randint(1, 3))
            else:
                pub_event.clear()
                random_wait_event.wait(random.randint(1, 3))

def receive_feedback_info(subscriber_ip, subscriber_port, feedback_info_queue, receive_event, stop_publish_service_event):

    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    #sub_socket.connect('tcp://%s:%d' % (str(subscriber_ip), int(subscriber_port)))
    connection_flag = False
    while not connection_flag:
        try:
            sub_socket.connect('tcp://%s:%d' % (str(subscriber_ip), int(subscriber_port)))
            #sub_socket.connect('tcp://%s:%d' % (str(subscriber_ip), int(subscriber_port)))
            connection_flag = True
            print('can connect to client', subscriber_port)
        except zmq.ZMQError:
            print('can not connect to client', subscriber_port)
            continue

    sub_socket.setsockopt(zmq.SUBSCRIBE, b'')
    poll = zmq.Poller()
    poll.register(sub_socket, zmq.POLLIN)

    while receive_event.is_set() and (not stop_publish_service_event.is_set()):
        socks = dict(poll.poll(50))
        if socks.get(sub_socket) == zmq.POLLIN:
            receive_feedback_info = sub_socket.recv_pyobj()
            '''
                feedback_dict[subscriber_IpPort] = [feedback_info_queue, receive_event]
                feedback_queue = feedback_dict[subscriber_IpPort][0]
                feedback_queue.put(receive_feedback_info)
            '''
            feedback_info_queue.put(receive_feedback_info)
        else:
            continue


def process_feedback_info(receive_event, feedback_info_queue, pool_syncfile, stop_publish_service_event, subscriber_ip,
                          subscriber_port):

    """
        receive feedback message format: {message_head:message_data}
        receive feedback message_head format: subscriber_IpPort + ':' + publisher_IpPort
                subscriber_IpPort: str(subscriber_ip) + '_' + str(subscriber_port)
                publisher_IpPort: str(publisher_ip) + '_' + str(publisher_port)
       receive feedback  message_data format: backlog_file
                backlog_file: (file_name, file_id, file_version, format)
    """
    subscriber_IpPort = subscriber_ip + '_' + str(subscriber_port)
    backlog_file_table_name = 'backlog_' + subscriber_IpPort.replace('.', '_')
    update_origin_sql = '''UPDATE %s SET file_status=0''' % (backlog_file_table_name)
    update_sql = update_origin_sql + ''' ''' + '''WHERE file_name=%s AND file_id=%s AND file_version=%s AND format=%s'''
    while receive_event.is_set() and (not stop_publish_service_event.is_set()):

        try:
            feedback_info = feedback_info_queue.get(timeout=1)
        except Exception as _:
            continue
        # feedback_message_head = feedback_info.keys()[0]
        # feedback_message_data = feedback_info[feedback_message_head]
        feedback_message_head, feedback_message_data = feedback_info.popitem()
        commit_flag = False
        try_count = 0
        while (not commit_flag) and (try_count < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                # cursor_syncfile.execute(update_sql, (str(feedback_message_data[0]), str(feedback_message_data[1]),int(feedback_message_data[2]), str(feedback_message_data[3])))
                cursor_syncfile.execute(update_sql, (feedback_message_data[0], feedback_message_data[1], feedback_message_data[2],feedback_message_data[3]))
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                commit_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('operational_error2',operational_error,update_sql)
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('operational_error -> exception_error', exception_error)
                try_count = try_count + 1
                continue
            except Exception as exception_error:
                print('exception_error2',exception_error,update_sql)
                continue


def update_backlog_and_queue_function(pool_syncfile, subscriber_ip, subscriber_port, engas_timeout_republish, filename_queue, unsubscribe_event,
                                      pub_event, stop_publish_service_event, random_wait_event, receive_event):

    query_newly_added_backlog_file_sql = '''SELECT file_name, file_id, file_version, format FROM ngas_files_test WHERE ingestion_date >= %s'''
    '''
        The subscriber has three status: 0 or 1 or 2.
            0 indicates unsubscribed.
            1 indicates running.
            2 indicates new_comer.
    '''
    query_subscriber_in_subscription_status_sql = '''SELECT last_file_sync_date FROM sync_subscribers WHERE subscriber_ip=%s AND subscriber_port=%s and subscribe_status=1'''
    update_last_file_sync_date_sql = '''UPDATE sync_subscribers SET last_file_sync_date=%s WHERE subscriber_ip=%s AND subscriber_port=%s'''

    '''
        construct the backlog file table name
        construct the sql: insert_newly_added_backlog_file_sql
    '''
    subscriber_IpPort = subscriber_ip + '_' + str(subscriber_port)
    backlog_table_name = 'backlog_' + subscriber_IpPort.replace('.', '_')
    insert_newly_added_backlog_file_origin_sql = '''INSERT INTO %s (file_name,file_id,file_version,format,file_status,change_status_timestamp)''' % (backlog_table_name)
    insert_newly_added_backlog_file_sql = insert_newly_added_backlog_file_origin_sql + ''' ''' + '''VALUES (%s, %s, %s, %s, %s, %s)'''

    while (not stop_publish_service_event.is_set()) and (not unsubscribe_event.is_set()):

        '''
            query and obtain the subscriber with subscribe_status=2 from the table (sync_subscribers).
            SELECT subscriber_ip,subscriber_port,last_file_sync_date FROM sync_subscribers WHERE subscribe_status=2
        '''
        query_subscribers_in_subscription_status_sql_execute_flag = False
        try_count_query_subscribers = 0
        while (not query_subscribers_in_subscription_status_sql_execute_flag) and (try_count_query_subscribers < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(query_subscriber_in_subscription_status_sql, (subscriber_ip, subscriber_port))
                row_count = cursor_syncfile.rowcount
                query_subscriber_in_subscription_status_result = cursor_syncfile.fetchall()
                cursor_syncfile.close()
                conn_syncfile.close()
                query_subscribers_in_subscription_status_sql_execute_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('operational_error3',operational_error)
                try_count_query_subscribers = try_count_query_subscribers + 1
                continue
            except Exception as exception_error:
                print('exception_error3',exception_error,query_subscriber_in_subscription_status_sql)
                try_count_query_subscribers = try_count_query_subscribers + 1
                continue
        if try_count_query_subscribers == 5:
            continue

        if row_count == 0:
            unsubscribe_event.set()
            receive_event.clear()
            continue
        elif row_count == 1:
            """
                If this subscriber exists in the table (sync_subscribers), do the following:
                    1、obtain republish records, update backlog_file_table, and append the republish records to the filename_queue
                    2、obtain newly added records, insert newly added records into the backlog_file_table, and append the newly records to the filename_queue
            """
            last_file_sync_date = float(query_subscriber_in_subscription_status_result[0][0])

            '''
                query and obtain the backlog files which meet ingestion_date >= last_file_sync_date from the ngas_files_test.
                That is to say: SELECT file_name, file_id, file_version, format FROM ngas_files_test WHERE ingestion_date >= last_file_sync_date
            '''
            #obtain newly added records
            query_newly_added_backlog_file_sql_execute_flag = False
            query_newly_added_backlog_file_sql_execute_num = 0
            while (not query_newly_added_backlog_file_sql_execute_flag) and (query_newly_added_backlog_file_sql_execute_num < 5):
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(query_newly_added_backlog_file_sql, (last_file_sync_date,))
                    query_newly_added_backlog_file_results = cursor_syncfile.fetchall()
                    print('query_newly_added_backlog_file_results',len(query_newly_added_backlog_file_results))
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    query_newly_added_backlog_file_sql_execute_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('operational_error4',operational_error)
                    query_newly_added_backlog_file_sql_execute_num = query_newly_added_backlog_file_sql_execute_num + 1
                    continue
                except Exception as exception_error:
                    print('exception_error4',exception_error,query_newly_added_backlog_file_sql)
                    query_newly_added_backlog_file_sql_execute_num = query_newly_added_backlog_file_sql_execute_num + 1
                    continue
            if query_newly_added_backlog_file_sql_execute_num == 5:
                continue

            '''
                get time.time() as the value of temp_last_file_sync_date which is as the new last_file_sync_date.
            '''
            temp_last_file_sync_date = time.time()
            if pub_event.is_set():

                '''
                    query and obtain the republish backlog files from the backlog file table meeting the publisher not receiving the feedback information from subscriber over 1 hour. 
                    put the republish backlog files into the queue with the format:(file_name,file_id,file_version,format)
                    queue is publishers_dict[subscriber_IpPort][2]. 
                '''
                # obtain republish records
                # republish_backlog_file_date = time.time() - random.randint(600, 900)  # 86400
                republish_backlog_file_date = time.time() - engas_timeout_republish * 60
                query_republish_backlog_file_origin_sql = '''SELECT file_name,file_id,file_version,format FROM %s''' % (
                    backlog_table_name)
                query_republish_backlog_file_sql = query_republish_backlog_file_origin_sql + ''' ''' + '''WHERE file_status=%s AND change_status_timestamp<%s'''
                query_republish_backlog_file_sql_execute_flag = False
                query_republish_backlog_file_sql_execute_num = 0
                republish_count = 0
                while (not query_republish_backlog_file_sql_execute_flag) and (
                        query_republish_backlog_file_sql_execute_num < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.execute(query_republish_backlog_file_sql, (1, republish_backlog_file_date))
                        query_republish_backlog_file_results = cursor_syncfile.fetchall()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        query_republish_backlog_file_sql_execute_flag = True
                    except MySQLdb.OperationalError as operational_error:
                        print('operational_error5', operational_error)
                        query_republish_backlog_file_sql_execute_num = query_republish_backlog_file_sql_execute_num + 1
                        continue
                    except Exception as exception_error:
                        print('exception_error5', exception_error,query_republish_backlog_file_sql)
                        query_republish_backlog_file_sql_execute_num = query_republish_backlog_file_sql_execute_num + 1
                        continue
                if query_republish_backlog_file_sql_execute_num == 5:
                    continue

                query_republish_backlog_file_results_deepcopy = copy.deepcopy(query_republish_backlog_file_results)

                '''
                    construct the arguments of the republish backlog file records for the backlog file table.
                    update the change_status_timestamp for the republish backlog file records.
                '''
                query_republish_backlog_file_results_list = list(map(list, query_republish_backlog_file_results))
                for query_republish_backlog_file_results_list_item in query_republish_backlog_file_results_list:
                    query_republish_backlog_file_results_list_item.insert(0, time.time())
                query_republish_backlog_file_results_tuple = list(map(tuple, query_republish_backlog_file_results_list))
                update_republish_backlog_file_change_status_timestamp_origin_sql = '''UPDATE %s''' % (
                    backlog_table_name)
                update_republish_backlog_file_change_status_timestamp_sql = update_republish_backlog_file_change_status_timestamp_origin_sql + ''' ''' + '''SET change_status_timestamp=%s WHERE file_name=%s AND file_id=%s AND file_version=%s AND format=%s'''

                # update backlog_file_table
                update_republish_backlog_file_change_status_timestamp_sql_execute_sql = False
                update_republish_backlog_file_change_status_timestamp_sql_execute_num = 0
                while (not update_republish_backlog_file_change_status_timestamp_sql_execute_sql) and (
                        update_republish_backlog_file_change_status_timestamp_sql_execute_num < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.executemany(update_republish_backlog_file_change_status_timestamp_sql,
                                                    query_republish_backlog_file_results_tuple)
                        conn_syncfile.commit()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        update_republish_backlog_file_change_status_timestamp_sql_execute_sql = True
                    except MySQLdb.OperationalError as operational_error:
                        print('operational_error6', operational_error)
                        try:
                            if conn_syncfile:
                                conn_syncfile.rollback()
                        except Exception as exception_error:
                            print('operational_error -> exception_error', exception_error)
                        update_republish_backlog_file_change_status_timestamp_sql_execute_num = update_republish_backlog_file_change_status_timestamp_sql_execute_num + 1
                        continue
                    except Exception as exception_error:
                        print('exception_error6', exception_error,update_republish_backlog_file_change_status_timestamp_sql)
                        update_republish_backlog_file_change_status_timestamp_sql_execute_num = update_republish_backlog_file_change_status_timestamp_sql_execute_num + 1
                        continue

                if update_republish_backlog_file_change_status_timestamp_sql_execute_num == 5:
                    continue

                # append the republish records to the filename_queue
                for query_republish_backlog_file_results_deepcopy_item in query_republish_backlog_file_results_deepcopy:
                    filename_queue.put(query_republish_backlog_file_results_deepcopy_item)

                '''
                    construct the newly added backlog file records for the backlog file table.
                    insert the newly added backlog file records into the backlog file table.
                '''
                query_newly_added_backlog_file_results_list = list(map(list, query_newly_added_backlog_file_results))
                for query_newly_added_backlog_file_results_list_item in query_newly_added_backlog_file_results_list:
                    query_newly_added_backlog_file_results_list_item.append(1)
                    query_newly_added_backlog_file_results_list_item.append(time.time())
                query_newly_added_backlog_file_results_tuple = list(map(tuple, query_newly_added_backlog_file_results_list))

                # insert newly added records into the backlog_file_table
                insert_newly_added_backlog_file_sql_execute_flag = False
                insert_newly_added_backlog_file_sql_execute_num = 0
                while (not insert_newly_added_backlog_file_sql_execute_flag) and (
                        insert_newly_added_backlog_file_sql_execute_num < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.executemany(insert_newly_added_backlog_file_sql,
                                                    query_newly_added_backlog_file_results_tuple)
                        conn_syncfile.commit()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        insert_newly_added_backlog_file_sql_execute_flag = True
                    except MySQLdb.OperationalError as operational_error:
                        print('operational_error7', operational_error)
                        try:
                            if conn_syncfile:
                                conn_syncfile.rollback()
                        except Exception as exception_error:
                            print('operational_error -> exception_error', exception_error)
                        insert_newly_added_backlog_file_sql_execute_num = insert_newly_added_backlog_file_sql_execute_num + 1
                        continue
                    except Exception as exception_error:
                        
                        #error_code, error_msg = exception_error.orig.args
                        #print('exception_error',type(exception_error))
                        #print('exception_error',type(exception_error.args))
                        #print('exception_error',exception_error.args)
                        error_code,error_msg = exception_error.args
                        if error_code == 1062:
                            insert_newly_added_backlog_file_sql_execute_flag = True
                            continue
                        else:
                        
                            print('exception_error7',error_code,error_msg)
                            insert_newly_added_backlog_file_sql_execute_num = insert_newly_added_backlog_file_sql_execute_num + 1
                            continue
                if insert_newly_added_backlog_file_sql_execute_num == 5:
                    continue

                #append the newly records to the filename_queue
                '''
                    put the newly added backlog file into the queue with the format: (file_name,file_id,file_version,format)
                    queue is publishers_dict[subscriber_IpPort][2].    
                '''
                query_newly_added_backlog_file_results_deepcopy = copy.deepcopy(query_newly_added_backlog_file_results)
                for query_newly_added_backlog_file_results_deepcopy_item in query_newly_added_backlog_file_results_deepcopy:
                    filename_queue.put(query_newly_added_backlog_file_results_deepcopy_item)

                '''
                    #update the last_file_sync_date with temp_last_file_sync_date for the subscriber in the table sync_subscribers
                    #UPDATE sync_subscribers SET last_file_sync_date = temp_last_file_sync_date WHERE subscriber_ip=str(publisher_record[0]) AND subscriber_port=int(publisher_record[1])
                '''
                update_last_file_sync_date_sql_execute_flag = False
                update_last_file_sync_date_sql_execute_num = 0
                while (not update_last_file_sync_date_sql_execute_flag) and (update_last_file_sync_date_sql_execute_num < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.execute(update_last_file_sync_date_sql, (temp_last_file_sync_date, subscriber_ip, subscriber_port))
                        print('update_last_file_sync_date_sql',update_last_file_sync_date_sql%(temp_last_file_sync_date, subscriber_ip, subscriber_port))
                        conn_syncfile.commit()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        update_last_file_sync_date_sql_execute_flag = True
                        #time.sleep(2)
                        #print('update_last_file_sync_date_sql Finished')
                    except MySQLdb.OperationalError as operational_error:
                        print('operational_error8', operational_error)
                        try:
                            if conn_syncfile:
                                conn_syncfile.rollback()
                        except Exception as exception_error:
                            print('operational_error -> exception_error', exception_error)
                        update_last_file_sync_date_sql_execute_num = update_last_file_sync_date_sql_execute_num + 1
                        continue
                    except Exception as exception_error:
                        print('exception_error8', exception_error,update_last_file_sync_date_sql)
                        update_last_file_sync_date_sql_execute_num = update_last_file_sync_date_sql_execute_num + 1
                        continue
                if update_last_file_sync_date_sql_execute_num == 5:
                    continue
        random_wait_event.wait(random.randint(10,60))


def poll_and_update_pub_event(pool_syncfile, pub_event, stop_publish_service_event, random_wait_event, subscriber_ip,
                              subscriber_port, publisher_ip, publisher_port, unsubscribe_event, receive_event):

    query_subscriber_in_the_subscription_status_sql = '''SELECT subscriber_ip,subscriber_port,publisher_port FROM sync_subscribers WHERE subscribe_status=%s AND subscriber_ip=%s AND subscriber_port=%s AND publisher_ip=%s AND publisher_port=%s'''

    while (not stop_publish_service_event.is_set()) and (not unsubscribe_event.is_set()):
        '''
            query and obtain the subscribers in the subscription status from the table (sync_subscribers).
        '''

        query_subscriber_in_the_subscription_status_sql_execute_flag = False
        query_subscriber_in_the_subscription_status_sql_execute_num = 0
        while (not query_subscriber_in_the_subscription_status_sql_execute_flag) and (query_subscriber_in_the_subscription_status_sql_execute_num < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(query_subscriber_in_the_subscription_status_sql, (1, subscriber_ip, subscriber_port, publisher_ip, publisher_port))
                row_count = cursor_syncfile.rowcount
                cursor_syncfile.close()
                conn_syncfile.close()
                query_subscriber_in_the_subscription_status_sql_execute_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('operational_error9',operational_error)
                query_subscriber_in_the_subscription_status_sql_execute_num = query_subscriber_in_the_subscription_status_sql_execute_num + 1
                continue
            except Exception as exception_error:
                print('exception_error9',exception_error,query_subscriber_in_the_subscription_status_sql)
                query_subscriber_in_the_subscription_status_sql_execute_num = query_subscriber_in_the_subscription_status_sql_execute_num + 1
                continue
        if query_subscriber_in_the_subscription_status_sql_execute_num == 5:
            continue

        '''
            update the publish event basing on whether the corresponding subscriber is connected to the data publisher or not.
        '''
        if row_count == 0:
            unsubscribe_event.set()
            receive_event.clear()
            continue
        elif row_count == 1:
            '''
                obtain the network connection status.
            '''
            connected_flag = network_port_functions.localHost_connected_ip_success(publisher_port, subscriber_ip)
            if connected_flag:
                '''
                    The publisher and subscriber have established a connection.
                    Trigger publish event.
                '''
                if pub_event.is_set():
                    continue
                else:
                    pub_event.set()

            else:
                '''
                    The publisher and subscriber have not established a connection.
                    clear the publish event.
                '''
                if not pub_event.is_set():
                    continue
                else:
                    pub_event.clear()
        random_wait_event.wait(random.randint(10,60))


def new_comer_publish_and_feedback_thread(pool_syncfile, constant_dict, stop_publish_service_event,
                                          new_comer_unsubscribe_event, random_wait_event):

    retrieval_subscriber_sql = '''SELECT subscriber_ip, subscriber_port, publisher_ip, publisher_port, subscriber_start_date, last_file_sync_date FROM sync_subscribers WHERE subscribe_status=2'''
    ngas_retrieval_sql = '''SELECT file_name, file_id, file_version, format FROM ngas_files_test WHERE ingestion_date >= %s'''
    update_sql = '''UPDATE sync_subscribers SET subscribe_status=1,last_file_sync_date=%s WHERE subscriber_ip=%s AND subscriber_port=%s'''

    while not stop_publish_service_event.is_set():

        '''
            retrieve and obtain newly added subscribers (if exist) from sync_subscribers.
        '''
        retrieval_subscriber_sql_execute_flag = False
        retrieval_subscriber_sql_execute_num = 0
        while (not retrieval_subscriber_sql_execute_flag) and (retrieval_subscriber_sql_execute_num < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(retrieval_subscriber_sql)
                row_count = cursor_syncfile.rowcount
                retrieval_results = cursor_syncfile.fetchall()
                cursor_syncfile.close()
                conn_syncfile.close()
                retrieval_subscriber_sql_execute_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('operational_error10',operational_error)
                retrieval_subscriber_sql_execute_num = retrieval_subscriber_sql_execute_num + 1
                continue
            except Exception as exception_error:
                print('exception_error10',exception_error,retrieval_subscriber_sql)
                retrieval_subscriber_sql_execute_num = retrieval_subscriber_sql_execute_num + 1
                continue
        if retrieval_subscriber_sql_execute_num == 5:
            continue

        if row_count > 0:
            '''
                There exist subscribers in the table (sync_subscribers).
            '''
            for publisher_record in retrieval_results:

                subscriber_ip = str(publisher_record[0],encoding='utf-8')
                subscriber_port = int(publisher_record[1])
                publisher_ip = str(publisher_record[2])
                publisher_port_string = str(publisher_record[3],encoding='utf-8')
                # subscriber_start_date = float(publisher_record[4])
                last_file_sync_date = float(publisher_record[5])
                subscriber_IpPort = str(subscriber_ip) + '_' + str(subscriber_port)

                """
                    insert ngas_files (backlog file) retrieved from ngas_files_test into the backlog_file_table with the query condition:last_file_sync_date.  
                """
                backlog_file_table_name = 'backlog_' + subscriber_IpPort.replace('.', '_')

                """
                     retrieve ngas_files from ngas_files_test where ingestion_date >= last_file_sync_date.
                     put item in the ngas_files into the queue
                """
                """
                     drop_exist_create_backlog_file_table_for_data_provider()这个函数的主要用途是解决刚创建完数据积压表，
                     突然断电导致的无法更新数据发布者表中相应订阅者对应的订阅状态值导致系统重新加电后因为订阅者的状态值为2，
                     创建数据积压表时，创建失败的错误
                """
                # drop_exist_backlog_file_table_flag = mysql_db_create_functions.drop_exist_create_backlog_file_table_for_data_provider(backlog_file_table_name)
                drop_exist_backlog_file_table_flag = mysql_db_create_functions.drop_if_exist_backlog_file_table_for_data_provider(backlog_file_table_name)
                if not drop_exist_backlog_file_table_flag:
                    continue

                # create backlog_file_table_name
                create_backlog_file_table_flag = mysql_db_create_functions.create_backlog_file_table_for_data_provider(backlog_file_table_name)
                if create_backlog_file_table_flag:
                    ###################################################################
                    ngas_retrieval_sql_execute_flag = False
                    ngas_retrieval_sql_execute_num = 0
                    execute_ngas_retrieval_sql = ngas_retrieval_sql%last_file_sync_date
                    while (not ngas_retrieval_sql_execute_flag) and (ngas_retrieval_sql_execute_num < 5):
                        try:
                            conn_syncfile = pool_syncfile.connection()
                            cursor_syncfile = conn_syncfile.cursor()
                            # cursor_syncfile.execute(ngas_retrieval_sql, (last_file_sync_date,))
                            cursor_syncfile.execute(execute_ngas_retrieval_sql)
                            ngas_files = cursor_syncfile.fetchall()
                            cursor_syncfile.close()
                            conn_syncfile.close()
                            ngas_retrieval_sql_execute_flag = True
                        except MySQLdb.OperationalError as operational_error:
                            print('operational_error11', operational_error)
                            ngas_retrieval_sql_execute_num = ngas_retrieval_sql_execute_num + 1
                            continue
                        except Exception as exception_error:
                            print('exception_error11', exception_error,execute_ngas_retrieval_sql)
                            ngas_retrieval_sql_execute_num = ngas_retrieval_sql_execute_num + 1
                            continue
                    if ngas_retrieval_sql_execute_num == 5:
                        # 一旦没有执行成功，应该杀掉已经创建的取数据和发布数据进程(fetch_and_publish_data)，这里未做回滚
                        continue
                    temp_last_file_sync_date = time.time()

                    """
                        construct the backlog file records
                    """
                    newly_added_backlog_file_list = list(map(list, ngas_files))
                    for newly_added_backlog_file_list_item in newly_added_backlog_file_list:
                        newly_added_backlog_file_list_item.append(1)
                        newly_added_backlog_file_list_item.append(time.time())
                    newly_added_backlog_file_tuple = list(map(tuple, newly_added_backlog_file_list))

                    """
                        insert the backlog file record into the backlog_file_table_name
                    """
                    syncfiles_backlog_sql = '''INSERT INTO %s (file_name, file_id, file_version, format, file_status, change_status_timestamp)''' % (
                        backlog_file_table_name)
                    insert_backlog_files_sql_syncfile = syncfiles_backlog_sql + ''' ''' + '''VALUES(%s, %s, %s, %s, %s, %s)'''

                    insert_backlog_files_sql_syncfile_execute_flag = False
                    insert_backlog_files_sql_syncfile_execute_num = 0
                    while (not insert_backlog_files_sql_syncfile_execute_flag) and (
                            insert_backlog_files_sql_syncfile_execute_num < 5):
                        try:
                            conn_syncfile = pool_syncfile.connection()
                            cursor_syncfile = conn_syncfile.cursor()
                            cursor_syncfile.executemany(insert_backlog_files_sql_syncfile,
                                                        newly_added_backlog_file_tuple)
                            conn_syncfile.commit()
                            cursor_syncfile.close()
                            conn_syncfile.close()
                            insert_backlog_files_sql_syncfile_execute_flag = True
                        except MySQLdb.OperationalError as operational_error:
                            print('operational_error12', operational_error)
                            try:
                                if conn_syncfile:
                                    conn_syncfile.rollback()
                            except Exception as exception_error:
                                print('exception_error12', exception_error,insert_backlog_files_sql_syncfile)
                            insert_backlog_files_sql_syncfile_execute_num = insert_backlog_files_sql_syncfile_execute_num + 1
                            continue
                        except Exception as exception_error:
                            print('exception_error13', exception_error,insert_backlog_files_sql_syncfile)
                            insert_backlog_files_sql_syncfile_execute_num = insert_backlog_files_sql_syncfile_execute_num + 1
                            continue
                    if insert_backlog_files_sql_syncfile_execute_num == 5:
                        continue

                    """
                        UPDATE sync_subscribers SET last_file_sync_date = temp_last_file_sync_date AND subscribe_status=1 WHERE subscriber_ip=str(publisher_record[0]) AND subscriber_port=int(publisher_record[1])
                    """
                    update_sql_execute_flag = False
                    update_sql_execute_num = 0
                    execute_update_sql = update_sql%(temp_last_file_sync_date, subscriber_ip, subscriber_port)
                    while (not update_sql_execute_flag) and (update_sql_execute_num < 5):
                        try:
                            conn_syncfile = pool_syncfile.connection()
                            cursor_syncfile = conn_syncfile.cursor()
                            # cursor_syncfile.execute(execute_update_sql)
                            cursor_syncfile.execute(update_sql,(temp_last_file_sync_date, subscriber_ip, subscriber_port))
                            conn_syncfile.commit()
                            cursor_syncfile.close()
                            conn_syncfile.close()
                            update_sql_execute_flag = True
                        except MySQLdb.OperationalError as operational_error:
                            print('operational_error14', operational_error)
                            try:
                                if conn_syncfile:
                                    conn_syncfile.rollback()
                            except Exception as exception_error:
                                print('exception_error14', exception_error,update_sql)
                            update_sql_execute_num = update_sql_execute_num + 1
                            continue
                        except Exception as exception_error:
                            print('exception_error15', exception_error,update_sql)
                            update_sql_execute_num = update_sql_execute_num + 1
                            continue
                    if update_sql_execute_num == 5:
                        continue

                    filename_queue = multiprocessing.Queue()  # item in queue:(file_name, file_id, file_version, format)

                    pub_event = multiprocessing.Event()
                    unsubscribe_event = multiprocessing.Event()
                    receive_event = threading.Event()
                    receive_event.set()

                    feedback_info_queue = Queue.Queue()

                    for queue_element in ngas_files:
                        filename_queue.put(queue_element)
                    # ###
                    # start update_backlog_and_queue_thread
                    """
                        def update_backlog_and_queue_thread(pool_syncfile,publishers_dict,random_wait_event,stop_event):
                        create the update_backlog_and_queue_thread
                        start the update_backlog_and_queue_thread
                    """
                    engas_timeout_republish = constant_dict['engas_timeout_republish']
                    update_backlog_and_queue_thread = threading.Thread(target=update_backlog_and_queue_function,
                                                                       args=(
                                                                           pool_syncfile, subscriber_ip,
                                                                           subscriber_port, engas_timeout_republish, filename_queue,
                                                                           unsubscribe_event, pub_event,
                                                                           stop_publish_service_event,
                                                                           random_wait_event, receive_event))
                    update_backlog_and_queue_thread.start()

                    publish_port_list = list(map(int, publisher_port_string.split(',')))

                    # start poll_and_handle_unsubscribe_thread
                    poll_and_handle_unsubscribe_thread = threading.Thread(
                        target=poll_and_handle_unsubscribe,
                        args=(pool_syncfile, stop_publish_service_event, new_comer_unsubscribe_event, random_wait_event,
                              subscriber_ip, subscriber_port, publisher_ip, unsubscribe_event, receive_event))
                    poll_and_handle_unsubscribe_thread.start()

                    # start receive_feedback_info_thread
                    receive_feedback_info_thread = threading.Thread(
                        target=receive_feedback_info,
                        args=(subscriber_ip, subscriber_port, feedback_info_queue, receive_event,
                              stop_publish_service_event))
                    receive_feedback_info_thread.start()

                    # start process_feedback_info_thread
                    for _ in range(int(constant_dict['process_feedback_info_worker_number'])):
                        process_feedback_info_thread = threading.Thread(
                            target=process_feedback_info,
                            args=(receive_event, feedback_info_queue, pool_syncfile, stop_publish_service_event,
                                  subscriber_ip, subscriber_port))
                        process_feedback_info_thread.start()

                    for publish_port in publish_port_list:
                        # create and start fetch_and_publish_data_process
                        fetch_and_publish_data_process = multiprocessing.Process(
                            target=fetch_and_publish_data,
                            args=(
                            filename_queue, pool_syncfile, publisher_ip, publish_port, subscriber_ip, subscriber_port,
                            pub_event, constant_dict, stop_publish_service_event, unsubscribe_event))
                        fetch_and_publish_data_process.start()

                        # create and start poll_pub_event_thread
                        poll_pub_event_thread = threading.Thread(
                            target=poll_pub_event,
                            args=(publish_port, subscriber_ip, pub_event, stop_publish_service_event, unsubscribe_event,
                                  random_wait_event))
                        poll_pub_event_thread.start()
                    ###################################################
                else:
                    continue

        random_wait_event.wait(random.randint(10, 60))


def poll_and_handle_unsubscribe(pool_syncfile, stop_publish_service_event, new_comer_unsubscribe_event,
                                random_wait_event, subscriber_ip, subscriber_port, publisher_ip, unsubscribe_event,
                                receive_event):

    select_sql = "SELECT * FROM sync_subscribers WHERE subscribe_status=%s AND subscriber_ip=%s AND subscriber_port=%s AND publisher_ip=%s"
    update_sql = "UPDATE sync_subscribers SET subscribe_status=-1 WHERE subscriber_ip=%s AND subscriber_port=%s AND publisher_ip=%s"

    while (not stop_publish_service_event.is_set()) and (not unsubscribe_event.is_set()):

        if new_comer_unsubscribe_event.is_set():

            select_sql_execute_flag = False
            select_sql_execute_num = 0
            while (not select_sql_execute_flag) and (select_sql_execute_num < 5):
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(select_sql, (0, subscriber_ip, subscriber_port, publisher_ip))
                    row_count = cursor_syncfile.rowcount
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    select_sql_execute_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('operational_error16',operational_error)
                    select_sql_execute_num = select_sql_execute_num + 1
                    continue
                except Exception as exception_error:
                    print('exception_error16',exception_error,select_sql)
                    select_sql_execute_num = select_sql_execute_num + 1
                    continue
            if select_sql_execute_num == 5:
                continue

            if row_count == 0:
                random_wait_event.wait(random.randint(1, 10))
            elif row_count == 1:

                unsubscribe_event.set()
                receive_event.clear()

                """
                    feedback_dict[subscriber_IpPort] = [feedback_info_queue, receive_event]
                    the item of feedback_dict including: feedback_key and feedback_value
                    feedback_key: subscriber_IpPort
                    feedback_value: [feedback_info_queue, receive_event]
                    clear the event (receive_event)
                """

                '''
                    update the subscribe_status with -1 for the unsubscriber with the subscriber_ip and subscriber_port
                '''
                update_sql_execute_flag = False
                update_sql_execute_num = 0
                while (not update_sql_execute_flag) and (update_sql_execute_num < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.execute(update_sql, (subscriber_ip, subscriber_port, publisher_ip))
                        conn_syncfile.commit()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        update_sql_execute_flag = True
                    except MySQLdb.OperationalError as operational_error:
                        print('operational_error17', operational_error)
                        try:
                            if conn_syncfile:
                                conn_syncfile.rollback()
                        except Exception as exception_error:
                            print('exception_error18', exception_error,update_sql)
                        update_sql_execute_num = update_sql_execute_num + 1
                        continue
                    except Exception as exception_error:
                        print('exception_error19', exception_error,update_sql)
                        update_sql_execute_num = update_sql_execute_num + 1
                        continue
                if update_sql_execute_num == 5:
                    continue
        else:
            random_wait_event.wait(random.randint(1, 60))


def stop_server_running(pool_syncfile, constant_dict, random_wait_event, stop_publish_service_event):
    retrieval_server_running_status_sql = '''SELECT running_status FROM server_running_status WHERE server_ip=%s AND server_port=%s'''
    detection_flag = True
    '''
        Every 2 seconds, poll the running status of the server in the data provider side.
        If the running status of the server is 1, sleep 2 seconds.
        If the running status of the server is 0, trigger the stop event and delete the record from the table (server_running_status)
    '''

    '''
        Suppose that you are running in the default REPEATABLE READ isolation level. When you issue a consistent read (that is, 
        an ordinary SELECT statement), InnoDB gives your transaction a timepoint according to which your query sees the database. 
        If another transaction deletes a row and commits after your timepoint was assigned, you do not see the row as having been 
        deleted. Inserts and updates are treated similarly.

        为了避免读出的数据永远不变（即使其他会话已经修改了该值），最好是读取的时候建立连接和游标，读完之后立刻关闭游标和连接对象。
        To avoid reading data that will never change (even if the value has been modified by other sessions), 
        it is best to create a connection and cursor when you read it and close the cursor and connection objects 
        as soon as you read it.
        或者将事务隔离级别（transaction isolation level）设置成可串行化(serializable),但是设置成可串行化会是并发处理能力有所降低，且拥有更高的系统开销
    '''
    server_ip = constant_dict['server_ip']
    server_port = constant_dict['server_port']
    while detection_flag:
        retrieval_server_running_status_sql_flag = False
        try_count = 0
        while (not retrieval_server_running_status_sql_flag) and (try_count < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(retrieval_server_running_status_sql, (server_ip, server_port))
                query_result = cursor_syncfile.fetchall()[0]
                cursor_syncfile.close()
                conn_syncfile.close()
                retrieval_server_running_status_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def stop_server_running operational_error', operational_error)
                try_count = try_count + 1
                continue
            except Exception as exception_error:
                print('def stop_server_running exception_error', exception_error)
                try_count = try_count + 1
                continue
        if try_count < 5:
            server_running_status = query_result[0]
            if server_running_status == 1:
                time.sleep(2)
            elif server_running_status == 0:
                random_wait_event.set()
                stop_publish_service_event.set()
                detection_flag = False
        else:
            random_wait_event.set()
            stop_publish_service_event.set()
            detection_flag = False
