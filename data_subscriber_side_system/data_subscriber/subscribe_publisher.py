#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import zmq
import sys
import IPy
import threading
import time
import random
#import os
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
# from DBConnectionPool import MySQL_config
from main_config import mySQL_conf as MySQL_config
from main_config import localhost_conf
from commonUtils import network_port_functions


# default_subscriber_start_date=1440633600.0   '2015-08-27T00:00:00.000'
def tprint(msg):
    """like print, but won't get newlines confused with multiple threads"""
    sys.stdout.write(msg + '\n')
    sys.stdout.flush()


class ClientTaskForSubscription(threading.Thread):
    """ClientTaskForSubscription"""

    def __init__(self,publisher_ip,subscriber_ip,identity,subscriber_start_date=1440633600.0,port_number=4):

        self.publisher_ip = publisher_ip
        self.subscriber_ip = subscriber_ip
        self.id = identity
        self.subscriber_start_date = subscriber_start_date
        self.port_number = port_number
        threading.Thread.__init__(self)

    def run(self):

        #time.sleep(6)
        subscriber_port = -1
        if self.publisher_ip == self.subscriber_ip:
            #当数据发布端和数据订阅端部署在同一台机器上时，需要从sync_subscribers和sync_publishers中找出最大的已经使用的端口，然后再去找可用的端口
            #目前只实现了查找sync_subscribers中的最大端口，当数据发布端和数据订阅端部署在同一台机器上时，可能会使找到的可用端口是已经分配出去但是还未被绑定的端口，目前的程序版本还有BUG存在
            query_max_publisher_port_sql = '''SELECT publisher_port FROM sync_subscribers ORDER BY publisher_port DESC LIMIT 1'''
            query_max_publisher_port_sql_flag = False
            while not query_max_publisher_port_sql_flag:
                try:
                    conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name, user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(query_max_publisher_port_sql)
                    query_max_publisher_port_results = cursor_syncfile.fetchall()
                    row_count = cursor_syncfile.rowcount
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    query_max_publisher_port_sql_flag = True
                except MySQLdb.OperationalError:
                    if cursor_syncfile:
                        cursor_syncfile.close()
                    if conn_syncfile:
                        conn_syncfile.close()
                    continue
                # except MySQLdb.InvalidConnection:
                #     if cursor_syncfile:
                #         cursor_syncfile.close()
                #     if conn_syncfile:
                #         conn_syncfile.close()
                #     continue
                except Exception as _:
                    if cursor_syncfile:
                        cursor_syncfile.close()
                    if conn_syncfile:
                        conn_syncfile.close()
                    continue
            '''
                select results is null: ()
            '''
            if row_count == 0:
                subscriber_port = network_port_functions.find_one_available_port_for_Root()
            else:
                subscriber_port = network_port_functions.find_one_available_port_for_Root(int(query_max_publisher_port_results[0][0]))
        else:
            # query_max_subscriber_port_sql = '''SELECT subscriber_port FROM sync_publishers ORDER BY publisher_port DESC LIMIT 1'''
            query_max_subscriber_port_sql = '''SELECT subscriber_port FROM sync_publishers ORDER BY subscriber_port DESC LIMIT 1'''
            query_max_subscriber_port_sql_flag = False
            while not query_max_subscriber_port_sql_flag:
                try:
                    conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                                    db=MySQL_config.db_name,
                                                    user=MySQL_config.db_user,
                                                    passwd=MySQL_config.db_user_passwd,
                                                    charset=MySQL_config.db_charset)
                    cursor_syncfile = conn_syncfile.cursor()
                    cursor_syncfile.execute(query_max_subscriber_port_sql)
                    query_max_subscriber_port_results = cursor_syncfile.fetchall()
                    row_count = cursor_syncfile.rowcount
                    cursor_syncfile.close()
                    conn_syncfile.close()
                    query_max_subscriber_port_sql_flag = True
                except MySQLdb.OperationalError:
                    if cursor_syncfile:
                        cursor_syncfile.close()
                    if conn_syncfile:
                        conn_syncfile.close()
                    continue
                # except MySQLdb.InvalidConnection:
                #     if cursor_syncfile:
                #         cursor_syncfile.close()
                #     if conn_syncfile:
                #         conn_syncfile.close()
                #     continue
                except Exception as _:
                    if cursor_syncfile:
                        cursor_syncfile.close()
                    if conn_syncfile:
                        conn_syncfile.close()
                    continue
            if row_count == 0:
                subscriber_port = network_port_functions.find_one_available_port_for_Root()
            else:
                subscriber_port = network_port_functions.find_one_available_port_for_Root(int(query_max_subscriber_port_results[0][0]))
        '''
            call the function (find_one_available_port_for_Root())
            If there exists one available port, try to subscribe the publisher.
            If there not exists one available port, give up trying to subscriber the publisher.
        '''
        if subscriber_port == -1:
            '''
               there not exists one available port, and give up trying to subscriber the publisher. 
            '''
            print('NOT AVAILABLE PORT IN SUBSCRIBER SIDE')
            sys.exit()

        else:
            '''
               there exists one available port, and try to subscribe the publisher. 
            '''

            '''
                create subscriber_socket with context.socket(zmq.DEALER)
                MUST set the identity for subscriber_socket.
                bind subscriber_socktet to the publisher(tcp://ip:por)
            '''
            context = zmq.Context()
            subscriber_socket = context.socket(zmq.DEALER)
            identity = u'%d' % self.id
            subscriber_socket.identity = identity.encode('ascii')
            subscriber_socket.connect('tcp://%s:65535' % self.publisher_ip)

            '''
                create zmq.Poller() for polling for feedback message from the publisher
            '''
            poll = zmq.Poller()
            poll.register(subscriber_socket, zmq.POLLIN)
            '''
                construct send_msg with the format: send_command:send_message
                    send_command: 'SUBSCRIBE'
                    send_message: self.subscriber_ip+'_'+str(subscriber_port)+'_'+str(self.subscriber_start_date)
            '''
            send_command = 'SUBSCRIBE'
            send_message = self.subscriber_ip+'_'+str(subscriber_port)+'_'+str(self.subscriber_start_date)+'_'+str(self.port_number)

            '''
                try to subscribe the publisher.
                send the send_msg and receive the feedback_msg(received_msg)
                if received_msg includes the information 'SUBSCRIBE_SUCCESS', insert the newly added publisher into the table (sync_publishers)
                if received_msg includes the information 'SUBSCRIBE_FAILURE', give up trying to subscribe the publisher.
            '''
            subscriber_to_publisher_connection_flag = False
            start_wait_time_for_publisher_obtaining_publishPort = time.time()
            while not subscriber_to_publisher_connection_flag:
                # 得到发布文件服务器端给该订阅分配的端口号

                send_msg = send_command + ':' + send_message
                subscriber_socket.send_string(send_msg)
                while True:

                    socks = dict(poll.poll(4000))
                    if socks.get(subscriber_socket) == zmq.POLLIN:
                        '''
                            receive the feedback message from the publisher
                        '''
                        received_msg = str(subscriber_socket.recv(),encoding='utf-8')
                        received_msg_split = received_msg.split(':')
                        received_msg_command = received_msg_split[0]
                        received_msg_content = received_msg_split[1]

                        if received_msg_command == 'SUBSCRIBE_SUCCESS':
                            '''
                                received_msg includes the information 'SUBSCRIBE_SUCCESS'
                                insert the newly added publisher into the table (sync_publishers)
                                create the received file table for the newly added publisher
                            '''
                            received_msg_content_split = received_msg_content.split('_')
                            publisher_port = received_msg_content_split[-1]

                            '''
                                insert the newly added publisher into the table (sync_publishers)
                            '''
                            insert_publisher_record_sql = '''INSERT INTO sync_publishers (subscriber_ip, subscriber_port, publisher_ip, publisher_port, publish_status) VALUES (%s, %s, %s, %s, %s)'''
                            insert_publisher_record_sql_flag = False
                            while not insert_publisher_record_sql_flag:
                                try:
                                    conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port, db=MySQL_config.db_name, user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd, charset=MySQL_config.db_charset)
                                    cursor_syncfile = conn_syncfile.cursor()
                                    cursor_syncfile.execute(insert_publisher_record_sql, (self.subscriber_ip, int(subscriber_port), self.publisher_ip, publisher_port, 2))
                                    conn_syncfile.commit()
                                    cursor_syncfile.close()
                                    conn_syncfile.close()
                                    insert_publisher_record_sql_flag = True
                                except MySQLdb.OperationalError:
                                    if conn_syncfile:
                                        conn_syncfile.rollback()
                                    if cursor_syncfile:
                                        cursor_syncfile.close()
                                    if conn_syncfile:
                                        conn_syncfile.close()
                                    continue
                                # except MySQLdb.InvalidConnection:
                                #     if cursor_syncfile:
                                #         cursor_syncfile.close()
                                #     if conn_syncfile:
                                #         conn_syncfile.close()
                                #     continue
                                except Exception as _:
                                    if cursor_syncfile:
                                        cursor_syncfile.close()
                                    if conn_syncfile:
                                        conn_syncfile.close()
                                    continue

                            '''
                                create the received file table for the newly added publisher
                            '''

                            print('SUBSCRIBE_SUCCESS: publisher_ip:%s, publisher_port:%s, local_ip:%s, local_feedback_info_port:%d'%(self.publisher_ip, publisher_port, self.subscriber_ip, int(subscriber_port)))
                            print('The corresponding subscribe-Thread and feedback-info-Thread will then start.')
                            subscriber_to_publisher_connection_flag = True
                            break

                        elif received_msg_command == 'SUBSCRIBE_FAILURE':
                            '''
                                received_msg includes the information 'SUBSCRIBE_FAILURE'
                                give up trying to subscribe the publisher.
                            '''
                            print('NOT AVAILABLE PORT IN PUBLISHER SIDE')
                            sys.exit()
                    else:
                        subscriber_socket.setsockopt(zmq.LINGER, 0)
                        subscriber_socket.close()
                        poll.unregister(subscriber_socket)
                        subscriber_socket = context.socket(zmq.DEALER)
                        identity = u'%d' % self.id
                        subscriber_socket.identity = identity.encode('ascii')
                        subscriber_socket.connect('tcp://%s:65535' % self.publisher_ip)
                        poll.register(subscriber_socket, zmq.POLLIN)
                        subscriber_socket.send_string(send_msg)
                        elapsed_time_for_waiting = time.time() - start_wait_time_for_publisher_obtaining_publishPort
                        if elapsed_time_for_waiting > 604800:
                            # 如果发布文件服务器端一周都无法分配相应都端口，结束该进程（不再等待）
                            print('**********************************************')
                            print('elapsed time for waiting: 7 * 24 * 60 * 60 (seconds)')
                            print('**********************************************')
                            return
            else:
                time.sleep(random.randint(0, 10))

        subscriber_socket.close()
        context.term()


def is_ip(address):
    try:
        IPy.IP(address)
        return True
    except Exception as  e:
        return False

if __name__ == '__main__':
    #退订功能实现：不再订阅数据，不再发布反馈消息
    # local_ip = '10.10.10.102'
    local_ip = localhost_conf.local_ip
    transfer_data_port_number = localhost_conf.transfer_data_port_number
    if len(sys.argv) < 2:
        print('Please input correct publisher-ip local-ip, such as:python subscribe_req_rep.py 222.197.210.38 222.197.210.39')
        sys.exit(0)
    publisher_ip_received = sys.argv[1]
    try:
        subscriber_ip = sys.argv[2]
    except Exception as _:
        subscriber_ip = local_ip

    if is_ip(publisher_ip_received):
        if is_ip(local_ip):
            auto_subscribe_process = ClientTaskForSubscription(sys.argv[1], local_ip,random.randint(0, int(time.time())),port_number=transfer_data_port_number)
            auto_subscribe_process.start()
        else:
            print('local_ip (%s) is illegal IP (local_ip=subscriber_ip)'%(local_ip))
            print('hostname are not supported as legal local ip')
            print('legal local ip such as 127.0.0.1')
            print('correct local_ip in the file--(subscribe_req_rep.py) or specify the correct subscriber_ip ')
            sys.exit(0)
    else:
        print('publisher_ip (%s) is illegal IP'%(publisher_ip_received))
        print('hostname are not supported as legal publisher ip')
        print('legal publisher ip such as 127.0.0.1')
        sys.exit(0)
