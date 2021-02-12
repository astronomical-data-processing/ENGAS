#!/usr/bin/env python
# _*_ coding:utf-8 _*_
# import subprocess
import subprocess

def find_available_ports_for_Root(start=1024, end=65536, length=-1):
    port_list = []
    if end-start < length:
        print('end-start < length')
        return port_list
    else:
        if length == -1:
            for port in range(start, end):
                if port_is_available_for_Root(port):
                    port_list.append(port)
        else:
            for port in range(start, end):
                if port_is_available_for_Root(port):
                    port_list.append(port)
                if len(port_list) == length:
                    return port_list
    if len(port_list) == 0:
        print('there is no available between %d and %d' % (start, end))
        return port_list
    elif len(port_list) < length:
        print('there is(are) %d available port(s) between %d and %d' % (len(port_list), start, end))
        return port_list
    else:
        print('there exist(s) %d available port(s) between %d and %d' % (length, start, end))
        return port_list


def find_n_available_port_for_Root(port_number=1, input_port=0):
    port_list = []
    for temp_port in range(1024, 65533):
        port_flag = port_is_available_for_Root(temp_port)
        if port_flag and (temp_port > input_port) and len(port_list) < port_number:
            port_list.append(temp_port)
        elif len(port_list) == port_number:
            return port_list
    return port_list


def find_one_available_port_for_Root(input_port=0):
    port = -1
    for temp_port in range(1024, 65533):
        port_flag = port_is_available_for_Root(temp_port)
        if port_flag and (temp_port > input_port):
            return temp_port
        else:
            continue
    return port


def port_is_available_for_Root(port):
    shell_command = 'lsof -i:%d' % port
    status, output = subprocess.getstatusoutput(shell_command)
    if len(output) == 0:
        return True
    else:
        return False


def port_is_LISTEN_for_Root(port):

    shell_command = 'lsof -i:%d | grep -i %s' % (port, 'LISTEN')
    status, output = subprocess.getstatusoutput(shell_command)
    if len(output) == 0:
        return False
    else:
        return True


def ip_port_is_ESTABLISHED_for_Root(ip_port):
    ip_port_split = ip_port.split(':')
    port = int(ip_port_split[1])
    ip = ip_port_split[0]
    shell_command = 'lsof -i:%d | grep -i %s | grep -i %s' % (port, ip, 'ESTABLISHED')
    status, output = subprocess.getstatusoutput(shell_command)
    if len(output) == 0:
        return False
    else:
        return True


def localhost_connect_remoteHost_success(remote_ip,remote_port):
    #服务器使用该函数用于连接客户端的反馈端口
    #客户端（订阅数据端）使用该函数用于测试自己有没有连接上服务器端发送给该客户端数据时使用的端口
    remote_ip_split = remote_ip.split('.')
    pattern_active = r'.*{0[0]}\.{0[1]}\.{0[2]}\.{0[3]}.{1}.*\(ESTABLISHED\)'.format(remote_ip_split,remote_port)
    #pattern_active = r'.*{0[0]}\.{0[1]}\.{0[2]}\.{0[3]}.*{1[0]}\.{1[1]}\.{1[2]}\.{1[3]}\.{1[4]}.*\(ESTABLISHED\)'.format(ip_split,remote_host_split)
    #shell_command = 'lsof -i:%d | grep -i %s | grep -i %s' % (port, ip, 'ESTABLISHED')
    shell_command = 'lsof -i:%d -n -P|grep -i %s'%(remote_port,pattern_active)
    status, output = subprocess.getstatusoutput(shell_command)
    if len(output) == 0:
        return False
    else:
        return True


def localHost_connected_ip_success(local_port,remote_ip):
    #服务器用该函数测试有没有客户端连接上发布数据的端口
    #客户端（反馈信息端）使用该函数用于测试服务器端有没有连上该客户端端发送反馈信息的端口
    remote_ip_split = remote_ip.split('.')
    pattern_active = r'.*{0}.*{1[0]}\.{1[1]}\.{1[2]}\.{1[3]}.*\(ESTABLISHED\)'.format(local_port,remote_ip_split)
    # pattern_active = r'.*{0[0]}\.{0[1]}\.{0[2]}\.{0[3]}.*{1[0]}\.{1[1]}\.{1[2]}\.{1[3]}\.{1[4]}.*\(ESTABLISHED\)'.format(ip_split,remote_host_split)
    # shell_command = 'lsof -i:%d | grep -i %s | grep -i %s' % (port, ip, 'ESTABLISHED')
    shell_command = 'lsof -i:%d -n -P|grep -i %s' % (local_port, pattern_active)
    status, output = subprocess.getstatusoutput(shell_command)
    if len(output) == 0:
        return False
    else:
        return True


def host_port_is_LISTEN(host_port=65534, host_ip='127.0.0.1'):
    #服务器使用该函数用于连接客户端的反馈端口
    #客户端（订阅数据端）使用该函数用于测试自己有没有连接上服务器端发送给该客户端数据时使用的端口
    host_ip_split = host_ip.split('.')
    pattern_active = r'.*{0[0]}\.{0[1]}\.{0[2]}\.{0[3]}.{1}.*\(LISTEN\)'.format(host_ip_split, host_ip)
    #pattern_active = r'.*{0[0]}\.{0[1]}\.{0[2]}\.{0[3]}.*{1[0]}\.{1[1]}\.{1[2]}\.{1[3]}\.{1[4]}.*\(ESTABLISHED\)'.format(ip_split,remote_host_split)
    #shell_command = 'lsof -i:%d | grep -i %s | grep -i %s' % (port, ip, 'ESTABLISHED')
    shell_command = 'lsof -i:%d -n -P|grep -i %s'%(host_port,pattern_active)
    status, output = subprocess.getstatusoutput(shell_command)
    if len(output) == 0:
        return False
    else:
        return True
