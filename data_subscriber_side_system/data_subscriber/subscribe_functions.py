#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import zmq
import sys
# import msgpack
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
import time
import random
import threading
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import multiprocessing
from commonUtils import network_port_functions
from Database_and_data import mysql_db_create_functions


def subscribe_receive_data(publisher_ip, publish_port, subscriber_port, terminate_receive_data_service_event, unsubscribe_event, receive_data_event, constant_dict, pool_syncfile, feedback_info_queue):

    """
        create subscribe_socket
        bind subscribe_socket to tcp://publisher_ip:publisher_port
    """
    publisherIp_subscriberPort = str(publisher_ip) + '_' + str(subscriber_port)
    time.sleep(4)
    received_file_directory = constant_dict['store_file_directory']
    received_files_table_name = 'received_' + publisherIp_subscriberPort.replace('.', '_')
    replace_received_table_origin_sql = '''REPLACE INTO %s (file_name,file_id,file_version,format,ingestion_date)''' % (
        received_files_table_name)
    replace_received_table_sql = replace_received_table_origin_sql + ''' ''' + '''VALUES (%s, %s, %s, %s, %s)'''

    context = zmq.Context()
    subscribe_socket = context.socket(zmq.SUB)
    subscribe_socket.connect('tcp://%s:%d' % (str(publisher_ip), int(publish_port)))
    subscribe_socket.setsockopt(zmq.SUBSCRIBE, b'')
    poll = zmq.Poller()
    poll.register(subscribe_socket, zmq.POLLIN)
    print('subscribe_receive_data',multiprocessing.current_process())
    # filename_shm = multiprocessing.current_process().name.split('-')[1] + '.fits'
    while not terminate_receive_data_service_event.is_set():
        if (not unsubscribe_event.is_set()) and receive_data_event.is_set():
            """
                received_pyobj format: {message_head:message_data}
                received_pyobj message_head format: subscriber_IpPort + ':' + publisher_IpPort
                                subscriber_IpPort: str(subscriber_ip) + '_' + str(subscriber_port)
                                publisher_IpPort: str(publisher_ip) + '_' + str(publisher_port)
                received_pyobj message_data format: {backlog_file: send_data}
                                backlog_file: (file_name, file_id, file_version, format)
                                send_data: read data from the file corresponding to the backlog_file.                
            """
            socks = dict(poll.poll(3000))
            if socks.get(subscribe_socket) == zmq.POLLIN:
                received_pyobj = subscribe_socket.recv_pyobj()
                message_head, message_data = received_pyobj.popitem()
                file_record, file_data = message_data.popitem()
                """
                    The MySQL REPLACE statement works as follows:
                        If the new row already does not exist, the MySQL REPLACE  statement inserts a new row.
                        If the new row already exist, the REPLACE  statement deletes the old row first and then inserts a 
                        new row. In some cases, the REPLACE statement updates the existing row only.
                    if affected_row ==1:
                        commit()
                    elif affected_row > 1:
                        rollback()
                """
                replace_received_table_sql_flag = False
                try_count_replace_received_table_sql_flag = 0

                while (not replace_received_table_sql_flag) and (try_count_replace_received_table_sql_flag < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        affected_row = cursor_syncfile.execute(replace_received_table_sql, (
                        file_record[0], file_record[1], int(file_record[2]), file_record[3], time.time()))
                        if affected_row == 1:
                            conn_syncfile.commit()
                            if type(file_record[0]) is bytes:
                                file_record_filename = str(file_record[0],encoding='utf-8')
                            if file_record_filename.startswith('b'):
                                file_record_filename = file_record_filename[2:-1]
                            filename = file_record_filename.replace('/', '_')
                            #print('filename',filename)
                            filepath = os.path.join(received_file_directory,filename)
                            #print('filepath',filepath)
                            #with open(filepath, 'w') as fileobj:
                            # filepath = received_file_directory + filename_shm
                            with open(filepath, 'wb+') as fileobj:
                                fileobj.write(file_data)
                        elif affected_row > 1:
                            conn_syncfile.rollback()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        replace_received_table_sql_flag = True
                        print('file_record1', file_record)
                    except MySQLdb.OperationalError as operational_error:
                        print('def process_received_data_and_generate_feedback_info() -> operational_error:', operational_error)
                        try_count_replace_received_table_sql_flag = try_count_replace_received_table_sql_flag + 1
                        try:
                            if conn_syncfile:
                                conn_syncfile.rollback()
                        except Exception as exception_error:
                            print('def process_received_data_and_generate_feedback_info() -> operational_error -> exception_error', exception_error)
                        continue
                    except Exception as exception_error:
                        try_count_replace_received_table_sql_flag = try_count_replace_received_table_sql_flag + 1
                        print('def process_received_data_and_generate_feedback_info() -> exception_error', exception_error)
                        continue

                if try_count_replace_received_table_sql_flag < 5:
                    feedback_info_queue.put({message_head: file_record})

            else:
                continue
        elif unsubscribe_event.is_set():
            subscribe_socket.close()
            context.term()
            break
        elif not receive_data_event.is_set():
            poll_receive_data_event(publisher_ip, publish_port, receive_data_event)


def poll_receive_data_event(publisher_ip, publish_port, receive_data_event):

    """
         obtain the network connection status.
         trigger the subscribe_event basing on the network connection status
    """
    connect_flag = network_port_functions.localhost_connect_remoteHost_success(publisher_ip, publish_port)
    if connect_flag:
        receive_data_event.set()


def publish_feedback_info(terminate_receive_data_service_event, constant_dict, subscriber_port, feedback_info_queue, unsubscribe_event, publish_feedback_info_event):
    context = zmq.Context()
    feedback_info_socket = context.socket(zmq.PUB)
    bind_flag = False
    subscriber_ip = constant_dict['local_ip']

    while (not bind_flag) and (not terminate_receive_data_service_event.is_set()) and (not unsubscribe_event.is_set()):
        try:
            feedback_info_socket.bind('tcp://%s:%d' % (str(subscriber_ip), int(subscriber_port)))
            bind_flag = True
        except zmq.ZMQError:
            continue

    while not terminate_receive_data_service_event.is_set():
        if (not unsubscribe_event.is_set()) and publish_feedback_info_event.is_set():
            try:
                feedback_info_element = feedback_info_queue.get(timeout=2)
            except Exception as _:
                continue
            feedback_info_socket.send_pyobj(feedback_info_element)
        elif unsubscribe_event.is_set():
            feedback_info_socket.close()
            context.term()
            break
        elif not publish_feedback_info_event.is_set():
            time.sleep(1)


def poll_publish_feedback_info_event(subscriber_port, publisher_ip, publish_feedback_info_event, terminate_receive_data_service_event, unsubscribe_event, random_wait_event):
    """
        obtain the network connection status.
        trigger the feedback_event basing on the network connection status
    """
    while (not terminate_receive_data_service_event.is_set()) and (not unsubscribe_event.is_set()):

        # obtain the network connection status.
        connected_flag = network_port_functions.localHost_connected_ip_success(subscriber_port, publisher_ip)
        if connected_flag:
            # The publisher and subscriber have established a connection.
            # Trigger publish event.
            if publish_feedback_info_event.is_set():
                random_wait_event.wait(random.randint(10, 60))
            else:
                publish_feedback_info_event.set()
                random_wait_event.wait(random.randint(10, 60))

        else:
            # The publisher and subscriber have not established a connection.
            # clear the publish event.

            if not publish_feedback_info_event.is_set():
                random_wait_event.wait(random.randint(1, 3))
            else:
                publish_feedback_info_event.clear()
                random_wait_event.wait(random.randint(1, 3))

def poll_unsubscribe(terminate_receive_data_service_event, pool_syncfile, random_wait_event, unsubscribe_event, constant_dict, subscriber_port, publisher_ip):
    retrieval_sql = '''SELECT subscriber_ip,subscriber_port,publisher_ip,publisher_port FROM sync_publishers WHERE publish_status=%s AND subscriber_ip=%s AND subscriber_port=%s AND publisher_ip=%s'''
    update_sql = '''UPDATE sync_publishers SET publish_status=0 WHERE subscriber_ip=%s AND subscriber_port=%s AND publisher_ip=%s'''
    subscriber_ip = constant_dict['local_ip']
    exit_flag = False
    while (not terminate_receive_data_service_event.is_set()) and (not exit_flag):
        try_count_retrieval_sql_flag = 0
        retrieval_sql_flag = False
        while (not retrieval_sql_flag) and (try_count_retrieval_sql_flag < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(retrieval_sql,(0, subscriber_ip, subscriber_port, publisher_ip))
                row_count = cursor_syncfile.rowcount
                cursor_syncfile.close()
                conn_syncfile.close()
                retrieval_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def poll_unsubscribe() -> operational_error:', operational_error)
                try_count_retrieval_sql_flag = try_count_retrieval_sql_flag + 1
                continue
            except Exception as exception_error:
                print('def poll_unsubscribe() -> exception_error:', exception_error)
                try_count_retrieval_sql_flag = try_count_retrieval_sql_flag + 1
                continue

        if try_count_retrieval_sql_flag == 5:
            print('poll_unsubscribe Failure: retrieval_sql_flag!!!')
            continue
        if try_count_retrieval_sql_flag < 5:
            if row_count == 1:
                unsubscribe_event.is_set()
                exit_flag = True
                update_sql_flag = False
                try_count_update_sql_flag = 0
                while (not update_sql_flag) and (try_count_update_sql_flag < 5):
                    try:
                        conn_syncfile = pool_syncfile.connection()
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.execute(update_sql, (subscriber_ip, subscriber_port, publisher_ip))
                        conn_syncfile.commit()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        update_sql_flag = True
                    except MySQLdb.OperationalError as operational_error:
                        print('def poll_unsubscribe() -> operational_error:', operational_error)
                        try_count_update_sql_flag = try_count_update_sql_flag + 1
                        try:
                            if conn_syncfile:
                                conn_syncfile.rollback()
                        except Exception as exception_error:
                            print('def poll_unsubscribe() -> operational_error -> exception_error', exception_error)
                        continue
                    except Exception as exception_error:
                        try_count_update_sql_flag = try_count_update_sql_flag + 1
                        print('def poll_unsubscribe() -> exception_error', exception_error)
                        continue
            elif row_count == 0:
                random_wait_event.wait(random.randint(1, 10))

        else:
            random_wait_event.wait(random.randint(1, 10))


def deal_with_newly_added_subscriber(terminate_receive_data_service_event, random_wait_event, pool_syncfile, constant_dict):

    select_sql = '''SELECT subscriber_ip,subscriber_port,publisher_ip,publisher_port FROM sync_publishers WHERE publish_status=2'''
    update_sql = '''UPDATE sync_publishers SET publish_status=1 WHERE publish_status=2 and subscriber_ip=%s AND subscriber_port=%s AND publisher_ip=%s'''
    #subscriber_ip = constant_dict['local_ip']

    while (not terminate_receive_data_service_event.is_set()):
        select_sql_flag = False
        try_count_select_sql_flag = 0
        while (not select_sql_flag) and (try_count_select_sql_flag < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(select_sql)
                row_count = cursor_syncfile.rowcount
                select_results = cursor_syncfile.fetchall()
                cursor_syncfile.close()
                conn_syncfile.close()
                select_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def deal_with_newly_added_subscriber() -> operational_error:', operational_error)
                select_sql_flag = select_sql_flag + 1
                continue
            except Exception as exception_error:
                print('def deal_with_newly_added_subscriber() -> exception_error:', exception_error)
                select_sql_flag = select_sql_flag + 1
                continue
        if try_count_select_sql_flag < 5:
            if row_count == 0:
                random_wait_event.wait(random.randint(10, 60))
            else:
                for newly_added_subscriber in select_results:
                    # subscriber_ip = str(newly_added_subscriber[0])
                    if type(newly_added_subscriber[0]) is bytes:
                        subscriber_ip = str(newly_added_subscriber[0],encoding='utf-8')
                    if subscriber_ip.startswith('b'):
                        subscriber_ip = subscriber_ip[2:-1]
                    subscriber_port = int(newly_added_subscriber[1])
                    # publisher_ip = str(newly_added_subscriber[2])
                    if type(newly_added_subscriber[2]) is bytes:
                        publisher_ip = str(newly_added_subscriber[2],encoding='utf-8')
                    if publisher_ip.startswith('b'):
                        publisher_ip = publisher_ip[2:-1]
                    publisher_port_string = str(newly_added_subscriber[3],encoding='utf-8')
                    publisher_port_list = list(map(int, publisher_port_string.split(',')))
                    publisherIp_subscriberPort = str(publisher_ip) + "_" + str(subscriber_port)

                    '''
                        construct the received file table name 
                        create the table with the constructed received file table name
                        update the publish_status with 1 for the publish_status of the new_comer
                    '''
                    received_file_table_name = 'received_' + publisherIp_subscriberPort.replace('.', '_')
                    """
                        drop_exist_received_file_table_for_data_consumer()这个函数的主要用途是解决刚创建完数据接收到表，
                        突然断电导致的无法更新数据订阅者表中相应订阅者对应的接收状态值导致系统重新加电后因为订阅者的状态值为2，
                        创建接收表时，创建失败的错误
                    """
                    drop_exist_received_file_table_flag = mysql_db_create_functions.drop_exist_received_file_table_for_data_consumer(received_file_table_name)
                    if not drop_exist_received_file_table_flag:
                        continue

                    create_received_file_table_flag = mysql_db_create_functions.create_received_file_table_for_data_consumer(received_file_table_name)
                    if create_received_file_table_flag:
                        update_sql_flag = False
                        try_count_update_sql_flag = 0
                        while (not update_sql_flag) and (try_count_update_sql_flag < 5):
                            try:
                                conn_syncfile = pool_syncfile.connection()
                                cursor_syncfile = conn_syncfile.cursor()
                                cursor_syncfile.execute(update_sql, (subscriber_ip, subscriber_port, publisher_ip))
                                conn_syncfile.commit()
                                cursor_syncfile.close()
                                conn_syncfile.close()
                                update_sql_flag = True
                            except MySQLdb.OperationalError as operational_error:
                                print('def deal_with_newly_added_subscriber() -> operational_error:', operational_error)
                                try_count_update_sql_flag = try_count_update_sql_flag + 1
                                try:
                                    if conn_syncfile:
                                        conn_syncfile.rollback()
                                except Exception as exception_error:
                                    print('def deal_with_newly_added_subscriber() -> operational_error -> exception_error', exception_error)
                                continue
                            except Exception as exception_error:
                                try_count_update_sql_flag = try_count_update_sql_flag + 1
                                print('def deal_with_newly_added_subscriber() -> exception_error', exception_error)
                                continue

                        if try_count_update_sql_flag < 5:
                            feedback_info_queue = multiprocessing.Queue()
                            unsubscribe_event = multiprocessing.Event()
                            publish_feedback_info_event = multiprocessing.Event()
                            for publish_port in publisher_port_list:
                                receive_data_event = multiprocessing.Event()

                                # start the corresponding subscribe receive and deal with data process
                                subscribe_receive_process = multiprocessing.Process(
                                    target=subscribe_receive_data,
                                    args=(publisher_ip, publish_port, subscriber_port,
                                          terminate_receive_data_service_event, unsubscribe_event, receive_data_event,
                                          constant_dict, pool_syncfile, feedback_info_queue))
                                subscribe_receive_process.start()

                            # start the corresponding publish feedback infomation thread
                            publish_feedback_info_thread = threading.Thread(
                                target=publish_feedback_info,
                                args=(terminate_receive_data_service_event, constant_dict, subscriber_port,
                                      feedback_info_queue, unsubscribe_event, publish_feedback_info_event))
                            publish_feedback_info_thread.start()

                            # args=(terminate_receive_data_service_event, subscriber_ip, subscriber_port, feedback_info_queue, unsubscribe_event, publish_feedback_info_event)
                            poll_publish_feedback_info_event_thread = threading.Thread(
                                target=poll_publish_feedback_info_event,
                                args=(subscriber_port, publisher_ip, publish_feedback_info_event,
                                      terminate_receive_data_service_event, unsubscribe_event, random_wait_event))
                            poll_publish_feedback_info_event_thread.start()

                            # inspect unsubscribe and trigger the corresponding event
                            poll_unsubscribe_thread = threading.Thread(
                                target=poll_unsubscribe,
                                args=(terminate_receive_data_service_event, pool_syncfile, random_wait_event,
                                      unsubscribe_event, constant_dict, subscriber_port, publisher_ip))
                            poll_unsubscribe_thread.start()

                        elif try_count_update_sql_flag == 5:
                            print('deal_with_newly_added_subscriber execute update_sql:%s Failure!!'%update_sql)
                            continue

                    else:
                        continue

        elif try_count_select_sql_flag == 5:
            print('deal_with_newly_added_subscriber Failure: select_sql_flag!!!')
            random_wait_event.wait(random.randint(1,10))
            continue


def poll_terminate_receive_data_service_event(pool_syncfile, random_wait_event, terminate_receive_data_service_event):
    retrieval_client_running_status_sql = '''SELECT * FROM client_running_status'''
    '''
        Every 2 seconds, poll the running status of the client in the data consumer side.
        If the running status of the client is 1, sleep 2 seconds.
        If the running status of the client is 0, trigger the stop event and delete the record from the table (client_running_status)
    '''
    detection_flag = True
    while detection_flag:
        retrieval_client_running_status_sql_flag = False
        try_count = 0
        while (not retrieval_client_running_status_sql_flag) and (try_count < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(retrieval_client_running_status_sql)
                client_running_status = cursor_syncfile.fetchall()[0][0]
                cursor_syncfile.close()
                conn_syncfile.close()
                retrieval_client_running_status_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def poll_terminate_receive_data_service_event() -> operational_error:', operational_error)
                try_count = try_count + 1
                continue
            except Exception as exception_error:
                print('def poll_terminate_receive_data_service_event() -> exception_error:', exception_error)
                try_count = try_count + 1
                continue

        if try_count < 5:
            if client_running_status == 1:
                time.sleep(2)
            elif client_running_status == 0:
                random_wait_event.set()
                terminate_receive_data_service_event.set()
                detection_flag = False
        else:
            time.time(2)

    time.sleep(5)
    print('The client has been stopped!')
    pool_syncfile.close()
