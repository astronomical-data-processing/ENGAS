#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import sys
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
import threading
import time
import zmq
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import random
# from DBConnectionPool import MySQL_config
from main_config import mySQL_conf as MySQL_config
#import os
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')


'''
    Assume that for the same subscriber_ip, there is no multiple subscription to the same publisher_ip
'''

def tprint(msg):
    """like print, but won't get newlines confused with multiple threads"""
    sys.stdout.write(msg + '\n')
    sys.stdout.flush()


class ClientTaskForUnsubscription(threading.Thread):
    """ClientTaskForUnsubscription"""
    def __init__(self,publisher_ip,subscriber_ip,identity):
        self.publisher_ip = publisher_ip
        self.subscriber_ip = subscriber_ip
        self.id = identity
        threading.Thread.__init__ (self)

    def run(self):

        '''
           retrieve and obtain the subscriber_port with subscriber_ip and publisher_ip from the table (sync_publishers).
        '''
        select_subscriber_port_status_sql = '''SELECT subscriber_port,publish_status FROM sync_publishers WHERE subscriber_ip=%s AND publisher_ip=%s'''
        select_subscriber_port_status_sql_flag = False
        while not select_subscriber_port_status_sql_flag:
            try:
                conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                                db=MySQL_config.db_name,
                                                user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                                charset=MySQL_config.db_charset)
                cursor_syncfile = conn_syncfile.cursor()
                cursor_syncfile.execute(select_subscriber_port_status_sql, (self.subscriber_ip, self.publisher_ip))
                row_count = cursor_syncfile.rowcount
                select_subscriber_port_result = cursor_syncfile.fetchall()
                cursor_syncfile.close()
                conn_syncfile.close()
                select_subscriber_port_status_sql_flag = True
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
        if row_count == 1:
            subscriber_port = select_subscriber_port_result[0][0]
            publish_status = int(select_subscriber_port_result[0][1])
            if publish_status == 0:
                pass
            elif publish_status in [1,2]:
                '''
                    update the publish_status with 0 for the unsubscriber in the table (sync_publishers) using the condition: subscriber_ip and publisher_ip
                '''
                unsubscribe_sql = '''UPDATE sync_publishers SET publish_status=0 WHERE subscriber_ip=%s AND publisher_ip=%s'''
                unsubscribe_sql_flag = False
                while not unsubscribe_sql_flag:
                    try:
                        conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                                        db=MySQL_config.db_name,
                                                        user=MySQL_config.db_user, passwd=MySQL_config.db_user_passwd,
                                                        charset=MySQL_config.db_charset)
                        cursor_syncfile = conn_syncfile.cursor()
                        cursor_syncfile.execute(unsubscribe_sql, (self.subscriber_ip, self.publisher_ip))
                        conn_syncfile.commit()
                        cursor_syncfile.close()
                        conn_syncfile.close()
                        unsubscribe_sql_flag = True
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

            '''
                tell the publisher that the subscriber has been unsubscribed.
                obtained 'SUBSCRIBE_UNSUBSCRIBE' message from the publisher.
            '''
            context = zmq.Context()
            unsubscribe_socket = context.socket(zmq.DEALER)
            identity = u'%d' % self.id
            unsubscribe_socket.identity = identity.encode('ascii')
            unsubscribe_socket.connect('tcp://%s:65535' % self.publisher_ip)
            poll = zmq.Poller()
            poll.register(unsubscribe_socket, zmq.POLLIN)
            unsubscribe_flag = False
            start_wait_time_for_publisher_obtaining_publishPort = time.time()
            while not unsubscribe_flag:
                send_command = 'UNSUBSCRIBE'
                send_message = str(self.subscriber_ip) + '_' + str(subscriber_port)
                # unsubscribe_socket.send_pyobj({send_command: send_message})
                send_msg = send_command + ':' + send_message
                unsubscribe_socket.send_string(send_msg)
                while True:
                    socks = dict(poll.poll(4000))
                    if socks.get(unsubscribe_socket) == zmq.POLLIN:
                        received_msg = str(unsubscribe_socket.recv(),encoding='utf-8')
                        received_msg_split = received_msg.split(':')
                        received_msg_command = received_msg_split[0]
                        if received_msg_command == 'UNSUBSCRIBE_SUCCESS':
                            print('UNSUBSCRIBE SECCESS!')
                            unsubscribe_flag = True
                            break
                    else:
                        unsubscribe_socket.setsockopt(zmq.LINGER, 0)
                        unsubscribe_socket.close()
                        poll.unregister(unsubscribe_socket)
                        unsubscribe_socket = context.socket(zmq.DEALER)
                        identity = u'%d' % self.id
                        unsubscribe_socket.identity = identity.encode('ascii')
                        unsubscribe_socket.connect('tcp://%s:65535' % self.publisher_ip)
                        poll.register(unsubscribe_socket, zmq.POLLIN)
                        # unsubscribe_socket.send_pyobj({send_command: send_message})
                        unsubscribe_socket.send_string(send_msg)
                        elapsed_time_for_waiting = time.time() - start_wait_time_for_publisher_obtaining_publishPort
                        if elapsed_time_for_waiting > 604800:
                            # 如果发布文件服务器端一周都无法分配相应都端口，结束该进程（不再等待）
                            print('**********************************************')
                            print('elapsed time for waiting: 7 * 24 * 60 * 60 (seconds)')
                            print('**********************************************')
                            return
            unsubscribe_socket.close()
            context.term()

        elif row_count == 0:
            print('The subscriber (%s) has been unsubscribed from the publisher (%s).' % (self.subscriber_ip, self.publisher_ip))
            print('Or there is not subscription relelationship between the subscriber (%s) and the publisher (%s)' % (self.subscriber_ip, self.publisher_ip))
        else:
            context = zmq.Context()
            for select_subscriber_port_result_record in select_subscriber_port_result:
                subscriber_port = select_subscriber_port_result_record[0]
                publish_status = int(select_subscriber_port_result_record[1])
                if publish_status == 0:
                    pass
                elif publish_status in [1, 2]:
                    '''
                        update the publish_status with 0 for the unsubscriber in the table (sync_publishers) using the condition: subscriber_ip and publisher_ip
                    '''
                    unsubscribe_sql = '''UPDATE sync_publishers SET publish_status=0 WHERE subscriber_ip=%s AND publisher_ip=%s'''
                    unsubscribe_sql_flag = False
                    while not unsubscribe_sql_flag:
                        try:
                            conn_syncfile = MySQLdb.connect(host=MySQL_config.db_host, port=MySQL_config.db_port,
                                                            db=MySQL_config.db_name,
                                                            user=MySQL_config.db_user,
                                                            passwd=MySQL_config.db_user_passwd,
                                                            charset=MySQL_config.db_charset)
                            cursor_syncfile = conn_syncfile.cursor()
                            cursor_syncfile.execute(unsubscribe_sql, (self.subscriber_ip, self.publisher_ip))
                            conn_syncfile.commit()
                            cursor_syncfile.close()
                            conn_syncfile.close()
                            unsubscribe_sql_flag = True
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

                '''
                    tell the publisher that the subscriber has been unsubscribed.
                    obtained 'SUBSCRIBE_UNSUBSCRIBE' message from the publisher.
                '''
                # context = zmq.Context()
                unsubscribe_socket = context.socket(zmq.DEALER)
                identity = u'%d' % self.id
                unsubscribe_socket.identity = identity.encode('ascii')
                unsubscribe_socket.connect('tcp://%s:65535' % self.publisher_ip)
                poll = zmq.Poller()
                poll.register(unsubscribe_socket, zmq.POLLIN)
                unsubscribe_flag = False
                start_wait_time_for_publisher_obtaining_publishPort = time.time()
                while not unsubscribe_flag:
                    send_command = 'UNSUBSCRIBE'
                    send_message = str(self.subscriber_ip) + '_' + str(subscriber_port)
                    # unsubscribe_socket.send_pyobj({send_command: send_message})
                    send_msg = send_command + ':' + send_message
                    unsubscribe_socket.send_string(send_msg)
                    while True:
                        socks = dict(poll.poll(4000))
                        if socks.get(unsubscribe_socket) == zmq.POLLIN:
                            received_msg = str(unsubscribe_socket.recv(),encoding='utf-8')
                            received_msg_split = received_msg.split(':')
                            received_msg_command = received_msg_split[0]
                            if received_msg_command == 'UNSUBSCRIBE_SUCCESS':
                                print('UNSUBSCRIBE SECCESS!')
                                unsubscribe_flag = True
                                break
                        else:
                            unsubscribe_socket.setsockopt(zmq.LINGER, 0)
                            unsubscribe_socket.close()
                            poll.unregister(unsubscribe_socket)
                            unsubscribe_socket = context.socket(zmq.DEALER)
                            identity = u'%d' % self.id
                            unsubscribe_socket.identity = identity.encode('ascii')
                            unsubscribe_socket.connect('tcp://%s:65535' % self.publisher_ip)
                            poll.register(unsubscribe_socket, zmq.POLLIN)
                            # unsubscribe_socket.send_pyobj({send_command: send_message})
                            unsubscribe_socket.send_string(send_msg)
                            elapsed_time_for_waiting = time.time() - start_wait_time_for_publisher_obtaining_publishPort
                            if elapsed_time_for_waiting > 604800:
                                # 如果发布文件服务器端一周都无法分配相应都端口，结束该进程（不再等待）
                                print('**********************************************')
                                print('elapsed time for waiting: 7 * 24 * 60 * 60 (seconds)')
                                print('**********************************************')
                                return
                unsubscribe_socket.close()
            context.term()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Please input correct publisher-Ip local-Ip, such as:python unsubscribe_req_rep.py 222.197.210.38 222.197.210.37')
        sys.exit(0)
    auto_subscribe_process = ClientTaskForUnsubscription(sys.argv[1],sys.argv[2],random.randint(0, int(time.time())))
    auto_subscribe_process.start()