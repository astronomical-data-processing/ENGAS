#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import zmq
import time
import sys
import threading
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
from commonUtils import network_port_functions


def tprint(msg):
    """like print, but won't get newlines confused with multiple threads"""

    sys.stdout.write(msg + '\n')
    sys.stdout.flush()


def wait_for_shut_down_proxy(stop_event, workers, frontend, backend, context):
    while True:
        if (not stop_event.is_set()):
            time.sleep(1)
        else:
            break
    worker_number = len(workers)
    while True:
        worker_thread_stoped_number = 0
        for worker_thread in workers:
            if (not worker_thread.isAlive()):
                worker_thread_stoped_number = worker_thread_stoped_number + 1
        if worker_thread_stoped_number == worker_number:
            break
    frontend.close()
    backend.close()
    context.term()


class ServerTask(threading.Thread):
    """ServerTask"""

    def __init__(self, pool_syncfile, constant_dict, stop_publish_service_event):
        self.server_ip = constant_dict['server_ip']
        self.server_port = constant_dict['server_port']
        self.pool_syncfile = pool_syncfile
        self.stop_publish_service_event = stop_publish_service_event
        threading.Thread.__init__(self)
        self.context = zmq.Context()
        self.get_available_port_lock = threading.Lock()

    def run(self):
        self.frontend = self.context.socket(zmq.ROUTER)
        frontend_bind_flag = False
        while not frontend_bind_flag:
            try:
                self.frontend.bind('tcp://%s:%d'%(self.server_ip,self.server_port))
                frontend_bind_flag = True
            except zmq.ZMQError:
                continue

        self.backend = self.context.socket(zmq.DEALER)

        backend_bind_flag = False
        while not backend_bind_flag:
            try:
                self.backend.bind('inproc://backend')
                backend_bind_flag = True
            except zmq.ZMQError:
                continue

        self.workers = []
        for i in range(4):
            worker = ServerWorker(self.context, self.server_ip, self.pool_syncfile, self.stop_publish_service_event, self.get_available_port_lock)
            worker.start()
            self.workers.append(worker)
        wait_for_shut_down_proxy_thread = threading.Thread(target=wait_for_shut_down_proxy, args=(self.stop_publish_service_event, self.workers, self.frontend, self.backend, self.context))
        wait_for_shut_down_proxy_thread.start()
        try:
            zmq.proxy(self.frontend, self.backend)
        except Exception as _:
            pass
        finally:
            print('The server has been stopped!')


class ServerWorker(threading.Thread):
    """ServerWorker"""

    def __init__(self, context, server_ip, pool_syncfile, stop_publish_service_event, get_available_port_lock):
        threading.Thread.__init__(self)
        self.context = context
        self.server_ip = server_ip
        self.pool_syncfile = pool_syncfile
        self.stop_publish_service_event = stop_publish_service_event
        self.get_available_port_lock = get_available_port_lock

    def run(self):
        worker = self.context.socket(zmq.DEALER)
        worker.connect('inproc://backend')
        tprint('Worker started')
        poll = zmq.Poller()
        poll.register(worker, zmq.POLLIN)
        while (not self.stop_publish_service_event.is_set()):
            '''
                The server in data provider side can deal three message types (subscribe_message_type, unsubscribe_message_type and unknown_message_type).
                The format of subscribe_message_type is {SUBSCRIBE:(subscriberIp_subscriberPort,subscriber_start_date)}.
                The format of unsubscribe_message_type is {UNSUBSCRIBE: subscriberIp_subscriberPort}.
                The unknown_message_type is the received message which neither subscribe_message_type nor unsubscribe_message_type.
            '''

            socks = dict(poll.poll(4000))
            if socks.get(worker) == zmq.POLLIN:
                received_msg_list = list(worker.recv_multipart())
                ident = received_msg_list[0]
                msg = str(received_msg_list[1], encoding='utf-8')
                msg_split = msg.split(':')
                if len(msg_split) == 2:
                    '''
                        The format of received message (msg) is legal.
                        The format of legal message (msg) is Command:subscriberIp_subscriberPort_subscriberStartDate_portNumber.
                        The legal message (msg) such as: SUBSCRIBE:127.0.0.1_1024_1440633600.0_4
                        default subscriber_start_date=1440633600.0   '2015-08-27T00:00:00.000'
                        default port_number=4
                    '''
                    received_command = msg_split[0]
                    received_msg = msg_split[1]
                    # recieved_command = str(pyobj.keys()[0])
                    if received_command == 'SUBSCRIBE':
                        '''
                            The received command is SUBSCRIBE.
                            Do something.
                        '''
                        received_msg_split = received_msg.split('_')
                        subscriber_ip = received_msg_split[0]
                        '''
                        服务器端认为客户端发送过来的订阅者IP和端口都是合法的数值字符串，例如192.168.168.168_6666
                        '''
                        subscriber_port = received_msg_split[1]
                        subscriber_start_date = float(received_msg_split[2])
                        port_number = int(received_msg_split[3])
                        subscriber_IpPort = str(subscriber_ip) + '_' + str(subscriber_port)
                        query_sql_syncfile = """SELECT publisher_port FROM sync_subscribers WHERE subscribe_status>0 AND subscriber_ip=%s AND subscriber_port=%s"""
                        query_sql_syncfile_flag = False
                        while not query_sql_syncfile_flag:
                            try:
                                conn_syncfile = self.pool_syncfile.connection()
                                cursor_syncfile = conn_syncfile.cursor()
                                cursor_syncfile.execute(query_sql_syncfile, (str(subscriber_ip), int(subscriber_port)))
                                query_sql_syncfile_results = cursor_syncfile.fetchall()
                                row_count = cursor_syncfile.rowcount
                                cursor_syncfile.close()
                                conn_syncfile.close()
                                query_sql_syncfile_flag = True
                            except MySQLdb.OperationalError as operational_error:
                                print('class ServerWorker operational_error', operational_error)
                                continue
                            except Exception as exception_error:
                                print('class ServerWorker exception_error', exception_error)
                                continue

                        if row_count == 0:
                            try:
                                # obtain_port_lock.acquire()
                                # query_max_publisher_port_sql = '''SELECT publisher_port FROM sync_subscribers ORDER BY publisher_port DESC LIMIT 1'''

                                query_max_publisher_port_sql = '''SELECT publisher_port FROM sync_subscribers'''
                                query_max_publisher_port_sql_flag = False
                                while not query_max_publisher_port_sql_flag:
                                    try:
                                        conn_syncfile = self.pool_syncfile.connection()
                                        cursor_syncfile = conn_syncfile.cursor()
                                        cursor_syncfile.execute(query_max_publisher_port_sql)
                                        query_publisher_port_results = cursor_syncfile.fetchall()
                                        query_publisher_port_row_count = cursor_syncfile.rowcount
                                        cursor_syncfile.close()
                                        conn_syncfile.close()
                                        query_max_publisher_port_sql_flag = True
                                    except MySQLdb.OperationalError as operational_error:
                                        print('class ServerWorker operational_error', operational_error)
                                        continue
                                    except Exception as exception_error:
                                        print('class ServerWorker exception_error', exception_error)
                                        continue

                                self.get_available_port_lock.acquire()
                                input_port = 0
                                for one_publisher_ports in query_publisher_port_results:
                                    ports_string = str(one_publisher_ports[0],encoding='utf-8')
                                    ports_list = list(map(int, ports_string.split(',')))
                                    if ports_list[-1] > input_port:
                                        input_port = ports_list[-1]
                                if str(subscriber_ip) == str(self.server_ip):
                                    if query_publisher_port_row_count == 0:
                                        publisher_port_list = network_port_functions.find_n_available_port_for_Root(port_number, input_port)
                                    elif int(subscriber_port) > input_port:
                                        publisher_port_list = network_port_functions.find_n_available_port_for_Root(port_number, int(subscriber_port))
                                    else:
                                        publisher_port_list = network_port_functions.find_n_available_port_for_Root(port_number, input_port)
                                else:
                                    if query_publisher_port_row_count == 0:
                                        publisher_port_list = network_port_functions.find_n_available_port_for_Root(port_number, input_port)
                                    else:
                                        publisher_port_list = network_port_functions.find_n_available_port_for_Root(port_number, input_port)
                            except Exception as e:
                                print('publisher_port = network_port_functions.find_one_available_port_for_Root()', e)
                            finally:
                                self.get_available_port_lock.release()
                                # obtain_port_lock.release()
                            if len(publisher_port_list) == 0:
                                send_key = 'SUBSCRIBE_FAILURE'
                                send_value = 'NOT AVAILABLE PORT IN PUBLISHER SIDE'
                                send_msg = send_key + ':' + send_value
                                worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])
                                # worker.send_pyobj({send_key: send_value})
                            else:
                                # 即使发布端提供的可用的端口数小于订阅端请求的端口数，也认为订阅成功；
                                '''
                                    prepare replying information for the requestor
                                    insert the subscription record into the table (sync_subscribers)
                                    send the replying information to the requestor
                                '''
                                publisher_port_string = ''
                                for i in range(len(publisher_port_list)):
                                    if i == 0:
                                        publisher_port_string = publisher_port_string + str(publisher_port_list[i])
                                    else:
                                        publisher_port_string = publisher_port_string + ',' + str(publisher_port_list[i])

                                send_key = 'SUBSCRIBE_SUCCESS'
                                send_value = str(subscriber_IpPort) + '_' + str(self.server_ip) + '_' + publisher_port_string

                                replace_sql_syncfile = '''REPLACE INTO sync_subscribers(subscriber_ip,subscriber_port,publisher_ip,publisher_port,subscriber_start_date,last_file_sync_date,subscribe_status) VALUES(%s, %s, %s, %s, %s, %s, %s)'''
                                replace_sql_syncfile_flag = False
                                while not replace_sql_syncfile_flag:
                                    try:
                                        conn_syncfile = self.pool_syncfile.connection()
                                        cursor_syncfile = conn_syncfile.cursor()
                                        affected_row = cursor_syncfile.execute(replace_sql_syncfile, (
                                            subscriber_ip, int(subscriber_port), self.server_ip, publisher_port_string,
                                            subscriber_start_date, subscriber_start_date, 2))
                                        if affected_row == 1:
                                            conn_syncfile.commit()
                                        elif affected_row > 1:
                                            conn_syncfile.rollback()
                                        cursor_syncfile.close()
                                        conn_syncfile.close()
                                        replace_sql_syncfile_flag = True
                                    except MySQLdb.OperationalError as operational_error:
                                        print('class ServerWorker operational_error', operational_error)
                                        try:
                                            if conn_syncfile:
                                                conn_syncfile.rollback()
                                        except Exception as exception_error:
                                            print('class ServerWorker operational_error -> exception_error', exception_error)
                                        continue
                                    except Exception as exception_error:
                                        print('class ServerWorker exception_error', exception_error)
                                        continue

                                send_msg = send_key + ':' + send_value
                                worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])

                        else:
                            '''
                                Assume that only if the table (sync_subscribers) exists the corresponding record for the same subscriber_ip and subscriber_port, which indicate the requestor has been subscribed successfully.
                                The server directly construct replying information for the requestor and send it to the requestor.
                            '''
                            publisher_port_string = query_sql_syncfile_results[0][0]
                            send_key = 'SUBSCRIBE_SUCCESS'
                            send_value = str(subscriber_IpPort) + '_' + str(self.server_ip) + '_' + str(
                                publisher_port_string)
                            send_msg = send_key + ':' + send_value
                            worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])

                    elif received_command == 'UNSUBSCRIBE':
                        '''
                            The received command is UNSUBSCRIBE.
                            Do something.
                        '''
                        received_msg_split = received_msg.split('_')
                        subscriber_ip = received_msg_split[0]
                        subscriber_port = received_msg_split[1]

                        '''
                            retrieve the record coressponding to the subscriber_ip and subscriber_port
                        '''
                        query_exist_sql_syncfile = '''SELECT subscribe_status FROM sync_subscribers WHERE subscriber_ip=%s AND subscriber_port=%s'''
                        query_exist_sql_syncfile_flag = False
                        while not query_exist_sql_syncfile_flag:
                            try:
                                conn_syncfile = self.pool_syncfile.connection()
                                cursor_syncfile = conn_syncfile.cursor()
                                cursor_syncfile.execute(query_exist_sql_syncfile,(str(subscriber_ip), int(subscriber_port)))
                                row_count = cursor_syncfile.rowcount
                                query_exist_result_syncfile = cursor_syncfile.fetchall()
                                cursor_syncfile.close()
                                conn_syncfile.close()
                                query_exist_sql_syncfile_flag = True
                            except MySQLdb.OperationalError as operational_error:
                                print('class ServerWorker operational_error', operational_error)
                                continue
                            except Exception as exception_error:
                                print('class ServerWorker exception_error', exception_error)
                                continue

                        if row_count >= 1:
                            '''
                                There exists record(s) corresponding to the subscriber_ip and subscriber_port in the table (sync_subscribers).
                            '''
                            exist_record = query_exist_result_syncfile[0]
                            subscribe_status = int(exist_record[0])
                            if subscribe_status in [1, 2]:

                                '''
                                    set the subscribe_status of the record corresponding to the the subscriber_ip and subscriber_port to 0.
                                    0 indicates unsubscribed
                                    1 indicates running.
                                    2 indicates subscribed.
                                '''
                                update_subscribers_sql_syncfile = '''UPDATE sync_subscribers SET subscribe_status = 0 WHERE subscriber_ip=%s AND subscriber_port=%s'''
                                update_subscribers_sql_syncfile_flag = False
                                while not update_subscribers_sql_syncfile_flag:
                                    try:
                                        conn_syncfile = self.pool_syncfile.connection()
                                        cursor_syncfile = conn_syncfile.cursor()
                                        cursor_syncfile.execute(update_subscribers_sql_syncfile,(str(subscriber_ip), int(subscriber_port)))
                                        conn_syncfile.commit()
                                        cursor_syncfile.close()
                                        conn_syncfile.close()
                                        update_subscribers_sql_syncfile_flag = True
                                    except MySQLdb.OperationalError as operational_error:
                                        print('class ServerWorker operational_error', operational_error)
                                        try:
                                            if conn_syncfile:
                                                conn_syncfile.rollback()
                                        except Exception as exception_error:
                                            print('class ServerWorker operational_error -> exception_error', exception_error)
                                        continue
                                    except Exception as exception_error:
                                        print('class ServerWorker exception_error', exception_error)
                                        continue

                                '''
                                    construct replying information for the requestor and send it to the requestor.
                                '''
                                send_key = 'UNSUBSCRIBE_SUCCESS'
                                send_value = received_msg
                                send_msg = send_key + ':' + send_value
                                worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])

                            elif subscribe_status in [-1, 0]:
                                '''
                                    There exists the unsubscribed record corresponding to the subscriber_ip and subscriber_port in the table (sync_subscribers).
                                    construct replying information for the requestor and send it to the requestor.
                                '''
                                send_key = 'UNSUBSCRIBE_SUCCESS'
                                send_value = received_msg
                                send_msg = send_key + ':' + send_value
                                worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])

                        else:
                            cursor_syncfile.close()
                            conn_syncfile.close()
                            '''
                                There not exist the record corresponding to the subscriber_ip and subscriber_port in the table (sync_subscribers).
                                constructe replying information for the requestor and send it to the requestor.
                            '''
                            send_key = 'UNSUBSCRIBE_SUCCESS'
                            send_value = received_msg
                            send_msg = send_key + ':' + send_value
                            worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])
                    else:
                        '''
                            The received command is not supported!
                            construct replying information for the requestor and send it to the requestor
                        '''
                        send_value = 'The command:%s is not supported!' % received_command
                        send_key = 'NULL'
                        send_msg = send_key + ':' + send_value
                        worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])

                else:
                    '''
                        The format of received message (msg) is illegal.
                        The format of legal message (msg) is Command:subscriberIp_subscriberPort_subscriberStartDate_portNumber.
                        The legal message (msg) such as: SUBSCRIBE:127.0.0.1_1024_1440633600.0_4
                        default_subscriber_start_date=1440633600.0   '2015-08-27T00:00:00.000'
                        default port_number=4

                        construct replying information for the requestor and send it to the requestor.
                    '''
                    send_value = 'The format of sent message (%s) is illegal' % msg
                    send_key = 'ILLEGAL_FORMAT'
                    send_msg = send_key + ':' + send_value
                    worker.send_multipart([ident, bytes(send_msg,encoding='utf-8')])
            else:
                continue
        worker.close()
