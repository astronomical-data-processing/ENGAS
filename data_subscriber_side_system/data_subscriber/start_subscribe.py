#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import sys
# sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os
sys.path.insert(0, os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep)
import time
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import multiprocessing
import threading
import subscribe_functions
from DBConnectionPool import mysqlConnectionPoolDB
from main_config import localhost_conf

def main():
    pool_syncfile = mysqlConnectionPoolDB.Mysql().pool()
    constant_dict = dict()
    # constant_dict['local_ip'] = '10.10.10.102'
    # constant_dict['process_data_worker_number_for_one_receiver'] = 2
    # constant_dict['store_file_directory'] = '/dev/shm/'
    constant_dict['local_ip'] = localhost_conf.local_ip
    constant_dict['process_data_worker_number_for_one_receiver'] = localhost_conf.process_data_worker_number_for_one_receiver
    constant_dict['store_file_directory'] =  localhost_conf.received_data_files_dir
    terminate_receive_data_service_event = multiprocessing.Event()
    random_wait_event = multiprocessing.Event()

    assign_client_running_status(pool_syncfile, constant_dict)

    clear_unsubscribe_publisher_record_and_received_file_table(pool_syncfile)

    start_subscribe_thread(pool_syncfile, constant_dict, terminate_receive_data_service_event, random_wait_event)

    start_new_comer_subscribe_and_feedback_thread(terminate_receive_data_service_event, random_wait_event,
                                                  pool_syncfile, constant_dict)

    start_poll_terminate_receive_data_service_event(pool_syncfile, random_wait_event, terminate_receive_data_service_event)


def get_store_file_directory(received_file_directory):

    if received_file_directory == '':
        store_file_directory = os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep + 'received_data_files_dir' + os.sep
    elif os.path.isdir(received_file_directory):
        if received_file_directory[-1] == '/':
            store_file_directory = received_file_directory
        else:
            store_file_directory = received_file_directory + os.sep
    else:
        store_file_directory = os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep + 'received_data_files_dir' + os.sep
    return store_file_directory


def assign_client_running_status(pool_syncfile, constant_dict):

    query_client_running_status_sql = '''SELECT * FROM client_running_status'''
    update_client_running_status_sql = '''UPDATE client_running_status SET running_status = 1'''
    insert_client_running_status_sql = '''INSERT INTO client_running_status(running_status, client_ip) VALUES(%s,%s)'''
    '''
        query the running_status from the table (client_running_status).
        If there exists running_status record in the table(client_running_status) and running_status is 0, reset the running_status with 1.
        If there exists running_status record in the table(client_running_status) and running_status is 1, do nothing.
        If there does not exist running_status record in the table (client_running_status), insert the running_status with the value (1) into the table.
    '''
    query_sql_flag = False
    try_count_query_sql_flag = 0
    while (not query_sql_flag) and (try_count_query_sql_flag < 5):
        try:
            conn_syncfile = pool_syncfile.connection()
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_client_running_status_sql)
            row_count = cursor_syncfile.rowcount
            query_running_status_result = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            query_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def assign_client_running_status() -> operational_error:', operational_error)
            try_count_query_sql_flag = try_count_query_sql_flag + 1
            continue
        except Exception as exception_error:
            print('def assign_client_running_status() -> exception_error:', exception_error)
            try_count_query_sql_flag = try_count_query_sql_flag + 1
            continue

    if try_count_query_sql_flag == 5:
        print('assign_client_running_status Failure: query_sql_flag!!!')
        sys.exit()

    if row_count == 1:
        if query_running_status_result[0][0] == 0:
            update_client_running_status_sql_flag = False
            try_count_update_client_running_status_sql_flag = 0
            while (not update_client_running_status_sql_flag) and (try_count_update_client_running_status_sql_flag < 5):
                try:
                    conn_syncfile = pool_syncfile.connection()
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(update_client_running_status_sql)
                    conn_syncfile.commit()
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    update_client_running_status_sql_flag = True
                except MySQLdb.OperationalError as operational_error:
                    print('def assign_client_running_status() -> operational_error:', operational_error)
                    try_count_update_client_running_status_sql_flag = try_count_update_client_running_status_sql_flag + 1
                    try:
                        if conn_syncfile:
                            conn_syncfile.rollback()
                    except Exception as exception_error:
                        print('def assign_client_running_status() -> operational_error -> exception_error', exception_error)
                    continue
                except Exception as exception_error:
                    try_count_update_client_running_status_sql_flag = try_count_update_client_running_status_sql_flag + 1
                    print('def assign_client_running_status() -> exception_error', exception_error)
                    continue
            if try_count_update_client_running_status_sql_flag == 5:
                print('assign_client_running_status Failure: update_client_running_status_sql_flag!!!')
                sys.exit()

        elif query_running_status_result[0][0] == 1:
            print('The server in client side is running and cannot start a new server in client side.')
            sys.exit()

    elif row_count == 0:
        insert_client_running_status_sql_flag = False
        try_count_insert_client_running_status_sql_flag = 0
        while (not insert_client_running_status_sql_flag) and (try_count_insert_client_running_status_sql_flag< 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(insert_client_running_status_sql, (1, constant_dict['local_ip']))
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                insert_client_running_status_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def assign_client_running_status() -> operational_error:', operational_error)
                try_count_insert_client_running_status_sql_flag = try_count_insert_client_running_status_sql_flag + 1
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('def assign_client_running_status() -> operational_error -> exception_error', exception_error)
                continue
            except Exception as exception_error:
                try_count_insert_client_running_status_sql_flag = try_count_insert_client_running_status_sql_flag + 1
                print('def assign_client_running_status() -> exception_error', exception_error)
                continue
        if try_count_insert_client_running_status_sql_flag == 5:
            print('assign_client_running_status Failure: insert_client_running_status_sql_flag!!!')
            sys.exit()


def clear_unsubscribe_publisher_record_and_received_file_table(pool_syncfile):

    delete_unsubscribe_publisher_record_sql = '''DELETE FROM sync_publishers WHERE publisher_ip=%s AND subscriber_port=%s'''
    drop_received_file_table_origin_sql = '''DROP TABLE IF EXISTS %s'''

    '''
        clear subscribers who have just finished unsubscribing before the server shuts down. 
        That is to say, clear the subscribers whose publish_status is 0
        The publish_status has three possible values: 0, 1, 2
            2:new-comer
            1:running
            0:unsubscribed
    '''
    # subscriber_port is a feedback information port for the publisher
    query_unsubscribe_publishers_with_publish_status_0_sql = '''SELECT publisher_ip,subscriber_port FROM sync_publishers WHERE publish_status=0'''
    query_unsubscribe_publishers_with_publish_status_0_sql_flag = False
    try_count_query_0_sql_flag = 0
    # print('query_unsubscribe_publishers_with_publish_status_0_sql',query_unsubscribe_publishers_with_publish_status_0_sql)

    while (not query_unsubscribe_publishers_with_publish_status_0_sql_flag) and (try_count_query_0_sql_flag < 5):
        try:
            conn_syncfile = pool_syncfile.connection()
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_unsubscribe_publishers_with_publish_status_0_sql)
            query_unsubscribe_publishers_with_publish_status_0_results = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            query_unsubscribe_publishers_with_publish_status_0_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error:', operational_error)
            try_count_query_0_sql_flag = try_count_query_0_sql_flag + 1
            continue
        except Exception as exception_error:
            print('def clear_unsubscribe_publisher_record_and_received_file_table() -> exception_error:', exception_error)
            try_count_query_0_sql_flag = try_count_query_0_sql_flag + 1
            continue

    if try_count_query_0_sql_flag == 5:
        print('clear_unsubscribe_publisher_record_and_received_file_table Failure: query_unsubscribe_publishers_with_publish_status_0_sql_flag!!!')
        sys.exit()

    for unsubscribe_publisher_record in query_unsubscribe_publishers_with_publish_status_0_results:
        '''
            construct the received file table name which will be dropped from the database.
            construct the drop table sql which is used to drop the received file table which will be dropped from the database.
            drop the received file table meeting the drop condition.
        '''
        publisherIp_subscriberPort = str(unsubscribe_publisher_record[0],encoding='utf-8') + '_' + str(unsubscribe_publisher_record[1])
        received_file_table_name = 'received_' + publisherIp_subscriberPort.replace('.', '_')
        drop_received_file_table_sql = drop_received_file_table_origin_sql % (received_file_table_name)
        drop_received_file_table_sql_flag = False
        try_count_drop_received_file_table_sql_flag = 0
        while (not drop_received_file_table_sql_flag) and (try_count_drop_received_file_table_sql_flag < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(drop_received_file_table_sql)
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                drop_received_file_table_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error:', operational_error)
                try_count_drop_received_file_table_sql_flag = try_count_drop_received_file_table_sql_flag + 1
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error -> exception_error', exception_error)
                continue
            except Exception as exception_error:
                try_count_drop_received_file_table_sql_flag = try_count_drop_received_file_table_sql_flag + 1
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> exception_error', exception_error)
                continue

        if try_count_drop_received_file_table_sql_flag == 5:
            print('clear_unsubscribe_publisher_record_and_received_file_table Failure: drop_received_file_table_sql_flag!!!')
            sys.exit()

        '''
           delete the record corresponding to the unsubscriber with publish_status (0) from the table (sync_pubishers). 
        '''
        # publisher_ip = str(unsubscribe_publisher_record[0])
        if type(unsubscribe_publisher_record[0]) is bytes:
            publisher_ip = str(unsubscribe_publisher_record[0],encoding='utf-8')
        if publisher_ip.startswith('b'):
            publisher_ip = publisher_ip[2:-1]
        subscriber_port = int(unsubscribe_publisher_record[1])
        delete_unsubscribe_publisher_record_sql_flag = False
        try_count_delete_unsubscribe_publisher_record_sql_flag = 0
        while (not delete_unsubscribe_publisher_record_sql_flag) and (try_count_delete_unsubscribe_publisher_record_sql_flag < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(delete_unsubscribe_publisher_record_sql, (publisher_ip, subscriber_port))
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                delete_unsubscribe_publisher_record_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error:', operational_error)
                try_count_delete_unsubscribe_publisher_record_sql_flag = try_count_delete_unsubscribe_publisher_record_sql_flag + 1
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error -> exception_error', exception_error)
                continue
            except Exception as exception_error:
                try_count_delete_unsubscribe_publisher_record_sql_flag = try_count_delete_unsubscribe_publisher_record_sql_flag + 1
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> exception_error', exception_error)
                continue

        if try_count_delete_unsubscribe_publisher_record_sql_flag == 5:
            print('clear_unsubscribe_publisher_record_and_received_file_table Failure: delete_unsubscribe_publisher_record_sql_flag!!!')
            sys.exit()

    '''
        clear the subscribers whose publish_status is 0.
    '''
    query_unsubscribed_publishers_sql = '''SELECT publisher_ip,subscriber_port FROM sync_publishers WHERE publish_status=0'''
    try_count_query_unsubscribed_publishers_sql_flag = 0
    query_unsubscribed_publishers_sql_flag = False
    while (not query_unsubscribed_publishers_sql_flag) and (try_count_query_unsubscribed_publishers_sql_flag < 5):
        try:
            conn_syncfile = pool_syncfile.connection()
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(query_unsubscribed_publishers_sql)
            query_unsubscribed_publishers_results = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            query_unsubscribed_publishers_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error:', operational_error)
            try_count_query_unsubscribed_publishers_sql_flag = try_count_query_unsubscribed_publishers_sql_flag + 1
            continue
        except Exception as exception_error:
            print('def clear_unsubscribe_publisher_record_and_received_file_table() -> exception_error:', exception_error)
            try_count_query_unsubscribed_publishers_sql_flag = try_count_query_unsubscribed_publishers_sql_flag + 1
            continue

    if try_count_query_unsubscribed_publishers_sql_flag == 5:
        print('clear_unsubscribe_publisher_record_and_received_file_table Failure: try_count_query_unsubscribed_publishers_sql_flag!!!')
        sys.exit()

    for unsubscribe_publisher_record in query_unsubscribed_publishers_results:
        '''
            construct the received file table name which will be dropped from the database.
            construct the drop table sql which is used to drop the received file table which will be dropped from the database.
            drop the received file table meeting the drop condition.
        '''
        publisherIp_subscriberPort = str(unsubscribe_publisher_record[0]) + '_' + str(unsubscribe_publisher_record[1])
        received_file_table_name = 'received_' + publisherIp_subscriberPort.replace('.', '_')
        drop_received_file_table_sql = drop_received_file_table_origin_sql % (received_file_table_name)
        drop_received_file_table_sql_flag = False
        try_count_drop_received_file_table_sql_flag = 0
        while (not drop_received_file_table_sql_flag) and (try_count_drop_received_file_table_sql_flag < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(drop_received_file_table_sql)
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                drop_received_file_table_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error:', operational_error)
                try_count_drop_received_file_table_sql_flag = try_count_drop_received_file_table_sql_flag + 1
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error -> exception_error', exception_error)
                continue
            except Exception as exception_error:
                try_count_drop_received_file_table_sql_flag = try_count_drop_received_file_table_sql_flag + 1
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> exception_error', exception_error)
                continue

        if try_count_drop_received_file_table_sql_flag == 5:
            print('clear_unsubscribe_publisher_record_and_received_file_table Failure: drop_received_file_table_sql_flag!!!')
            sys.exit()

        '''
           delete the record corresponding to the unsubscriber with publish_status (0) from the table (sync_pubishers). 
        '''
        publisher_ip = str(unsubscribe_publisher_record[0])
        subscriber_port = int(unsubscribe_publisher_record[1])
        try_count_delete_unsubscribe_publisher_record_sql_flag = 0
        delete_unsubscribe_publisher_record_sql_flag = False
        while (not delete_unsubscribe_publisher_record_sql_flag) and (try_count_delete_unsubscribe_publisher_record_sql_flag < 5):
            try:
                conn_syncfile = pool_syncfile.connection()
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(delete_unsubscribe_publisher_record_sql, (publisher_ip, subscriber_port))
                conn_syncfile.commit()
                cursor_syncfile.close()
                conn_syncfile.close()
                delete_unsubscribe_publisher_record_sql_flag = True
            except MySQLdb.OperationalError as operational_error:
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error:', operational_error)
                try_count_delete_unsubscribe_publisher_record_sql_flag = try_count_delete_unsubscribe_publisher_record_sql_flag + 1
                try:
                    if conn_syncfile:
                        conn_syncfile.rollback()
                except Exception as exception_error:
                    print('def clear_unsubscribe_publisher_record_and_received_file_table() -> operational_error -> exception_error', exception_error)
                continue
            except Exception as exception_error:
                try_count_delete_unsubscribe_publisher_record_sql_flag = try_count_delete_unsubscribe_publisher_record_sql_flag + 1
                print('def clear_unsubscribe_publisher_record_and_received_file_table() -> exception_error', exception_error)
                continue

        if try_count_delete_unsubscribe_publisher_record_sql_flag == 5:
            print('clear_unsubscribe_publisher_record_and_received_file_table Failure: delete_unsubscribe_publisher_record_sql_flag!!!')
            sys.exit()


def start_subscribe_thread(pool_syncfile, constant_dict, terminate_receive_data_service_event, random_wait_event):
    """Run only when the server in client side is started"""
    #subscriber_ip = constant_dict['local_ip']
    # global publisher_dict, subscribe_info_queue, pool_syncfile,publisher_dict_lock
    #global publisher_dict, pool_syncfile, publisher_dict_lock
    '''
        retrieve and obtain the subscriber with publish_status equaling to 1 from the table (sync_publishers)
        create subscribe_thread for the subscirber with publish_status (1)
        start subscribe_thread for the subscriber with publish_status (1)
        construct item for publisher_dict.
            the item of publisher_dict : publisher_IpPort:[subscribe_event,unsubscribe_event]
        add the constructed item into the publisher_dict.
    '''
    retrieval_publishers_status_1_sql = '''SELECT publisher_ip,publisher_port,subscriber_port FROM sync_publishers WHERE publish_status=1'''
    retrieval_publishers_status_1_sql_flag = False
    try_count_retrieval_publishers_status_1_sql_flag = 0

    while (not retrieval_publishers_status_1_sql_flag) and (try_count_retrieval_publishers_status_1_sql_flag < 5):
        try:
            conn_syncfile = pool_syncfile.connection()
            cursor_syncfile = conn_syncfile.cursor()
            cursor_syncfile.execute(retrieval_publishers_status_1_sql)
            retrieval_publishers_status_1_results = cursor_syncfile.fetchall()
            cursor_syncfile.close()
            conn_syncfile.close()
            retrieval_publishers_status_1_sql_flag = True
        except MySQLdb.OperationalError as operational_error:
            print('def start_subscribe_thread() -> operational_error:', operational_error)
            try_count_retrieval_publishers_status_1_sql_flag = try_count_retrieval_publishers_status_1_sql_flag + 1
            continue
        except Exception as exception_error:
            print('def start_subscribe_thread() -> exception_error:', exception_error)
            try_count_retrieval_publishers_status_1_sql_flag = try_count_retrieval_publishers_status_1_sql_flag + 1
            continue

    if try_count_retrieval_publishers_status_1_sql_flag == 5:
        print('start_subscribe_thread Failure: retrieval_publishers_status_1_sql_flag!!!')
        sys.exit()

    for retrieval_publishers_status_1_record in retrieval_publishers_status_1_results:
        '''
            def subscribe_thread(publisher_ip, publisher_port, publisher_dict, stop_event, subscribe_event, unsubscribe_event)
            create subscribe_thread for the subscriber with publish_status (1)
            start subscribe_thread for the subscriber with publish_status (1)
        '''
        feedback_info_queue = multiprocessing.Queue()
        unsubscribe_event = multiprocessing.Event()
        publish_feedback_info_event = multiprocessing.Event()
        # publisher_ip = str(retrieval_publishers_status_1_record[0])
        # publisher_port_string = retrieval_publishers_status_1_record[1]
        if type(retrieval_publishers_status_1_record[0]) is bytes:
            publisher_ip = str(retrieval_publishers_status_1_record[0],encoding='utf-8')
        if publisher_ip.startswith('b'):
            publisher_ip = publisher_ip[2:-1]
        if type(retrieval_publishers_status_1_record[1]) is bytes:
            publisher_port_string = str(retrieval_publishers_status_1_record[1],encoding='utf-8')
        if publisher_port_string.startswith('b'):
            publisher_port_string = publisher_port_string[2:-1]
        subscriber_port = retrieval_publishers_status_1_record[2]
        publisher_port_list = list(map(int, publisher_port_string.split(',')))

        for publish_port in publisher_port_list:
            receive_data_event = multiprocessing.Event()

            # start the corresponding subscribe receive data process
            subscribe_receive_process = multiprocessing.Process(
                target=subscribe_functions.subscribe_receive_data,
                args=(publisher_ip, publish_port, subscriber_port, terminate_receive_data_service_event,
                      unsubscribe_event, receive_data_event, constant_dict, pool_syncfile, feedback_info_queue))
            subscribe_receive_process.start()

        # start the corresponding publish feedback infomation thread
        publish_feedback_info_thread = threading.Thread(
            target=subscribe_functions.publish_feedback_info,
            args=(terminate_receive_data_service_event, constant_dict, subscriber_port, feedback_info_queue, unsubscribe_event, publish_feedback_info_event))
        publish_feedback_info_thread.start()
        # args=(terminate_receive_data_service_event, subscriber_ip, subscriber_port, feedback_info_queue, unsubscribe_event, publish_feedback_info_event)
        poll_publish_feedback_info_event_thread = threading.Thread(
            target=subscribe_functions.poll_publish_feedback_info_event,
            args=(subscriber_port, publisher_ip, publish_feedback_info_event, terminate_receive_data_service_event,
                  unsubscribe_event, random_wait_event))
        poll_publish_feedback_info_event_thread.start()

        # inspect unsubscribe and trigger the corresponding event
        poll_unsubscribe_thread = threading.Thread(
            target=subscribe_functions.poll_unsubscribe,
            args=(terminate_receive_data_service_event, pool_syncfile, random_wait_event, unsubscribe_event, constant_dict, subscriber_port, publisher_ip))
        # args=(terminate_receive_data_service_event, pool_syncfile, random_wait_event, unsubscribe_event, subscriber_ip, subscriber_port, publisher_ip)
        poll_unsubscribe_thread.start()


def start_new_comer_subscribe_and_feedback_thread(terminate_receive_data_service_event, random_wait_event, pool_syncfile, constant_dict):

    """This function starts the threads which are daemon thread"""

    deal_with_newly_added_subscriber_thread = threading.Thread(
        target=subscribe_functions.deal_with_newly_added_subscriber,
        args=(terminate_receive_data_service_event, random_wait_event, pool_syncfile, constant_dict))
    deal_with_newly_added_subscriber_thread.start()


def start_poll_terminate_receive_data_service_event(pool_syncfile, random_wait_event, terminate_receive_data_service_event):
    """This function starts the threads which are daemon thread"""
    poll_terminate_receive_data_service_event_thread = threading.Thread(
        target=subscribe_functions.poll_terminate_receive_data_service_event,
        args=(pool_syncfile, random_wait_event, terminate_receive_data_service_event))
    poll_terminate_receive_data_service_event_thread.start()


if __name__ == '__main__':
    main()
