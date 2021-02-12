#!/usr/bin/env python
# _*_ coding:utf-8 _*_

#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
import sys
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
from astropy.io import fits
import os
import pycurl
import time
import simple_http_server
from main_config import localhost_conf

def get_archived_directory():
    archived_dir = localhost_conf.ngas_archived_dir
    if archived_dir == '':
        store_file_directory = os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep + 'simulated_experiment_data_files' + os.sep
    elif os.path.isdir(archived_dir):
        if archived_dir[-1] == '/':
            store_file_directory = archived_dir
        else:
            store_file_directory = archived_dir + os.sep
    else:
        store_file_directory = os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep + 'simulated_experiment_data_files' + os.sep
    archived_dir = store_file_directory
    return archived_dir


def auto_single_file_archive(archive_url):
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL,archive_url)
    curl.perform()
    curl.close()


def auto_prepare_archive_url_and_archive(server_ip, server_port, archived_ip, archived_port, archived_dir):
    httpd = simple_http_server.init_simple_http_server(archived_ip,archived_port,archived_dir)
    simple_http_server.start_simple_http_server(httpd)
    print('Starting archive to server(http://%s:%d)'%(server_ip,server_port))
    archived_data_file_dirs = localhost_conf.ngas_archived_data_file_dirs
    archived_url = 'http://%s:%d/QARCHIVE?filename=http://%s:%d/%s'
    for dict_key in archived_data_file_dirs:
        archived_file_dir = archived_dir + archived_data_file_dirs[dict_key] + os.sep
        archived_files = os.listdir(archived_file_dir)
        for archived_file in archived_files:
            if not archived_file.startswith('.'):
                # print(archived_url%(server_ip,server_port,archived_ip,archived_port,archived_file))
                archived_filename = archived_data_file_dirs[dict_key] + os.sep + archived_file
                print(archived_url%(server_ip,server_port,archived_ip,archived_port,archived_filename))
                auto_single_file_archive(archived_url % (server_ip, server_port, archived_ip, archived_port, archived_filename))
    # archived_files = os.listdir(archived_dir)
    # for archived_file in archived_files:
    #     if not archived_file.startswith('.'):
    #         #print(archived_url%(server_ip,server_port,archived_ip,archived_port,archived_file))
    #         auto_archive(archived_url%(server_ip,server_port,archived_ip,archived_port,archived_file))
    print('Finished archive to server(http://%s:%d)' % (server_ip, server_port))
    simple_http_server.stop_simple_http_server(httpd)


def main():
    server_ip = localhost_conf.ngas_server_ip
    server_port = localhost_conf.ngas_server_port
    archived_ip = localhost_conf.ngas_archived_ip
    archived_port = localhost_conf.ngas_archived_port
    archived_dir = get_archived_directory()
    auto_prepare_archive_url_and_archive(server_ip, server_port, archived_ip, archived_port, archived_dir)


if __name__ == '__main__':
    main()
