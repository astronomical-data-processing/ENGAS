#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import sys
import os
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
# from DBConnectionPool import MySQL_config
from main_config import mySQL_conf as MySQL_config


'''sql for creating database (syncfile)'''
syncfile_db_create_sql = """CREATE DATABASE IF NOT EXISTS %s""" % (MySQL_config.db_name)


'''sql for creating the subscriber table for data provider'''
syncfile_create_subscriber_table_for_data_provider = """ CREATE TABLE IF NOT EXISTS sync_subscribers (
                                        subscriber_ip varchar(32) not null,
                                        subscriber_port int not null,
                                        publisher_ip varchar(32) not null,
                                        publisher_port varchar(256) not null,
                                        subscriber_start_date real not null,
                                        last_file_sync_date real not null,
                                        subscribe_status int default 2,
                                        primary key (subscriber_port)
                                    ); """
"""
subscribe_status:
2:new_comer
1:subscribed and published
0:unsubscribed
"""


'''sql for dropping  the backlog file table for data provider, if exists'''
syncfile_drop_exist_backlog_file_table_for_provider = """ DROP TABLE IF EXISTS %s;"""


'''sql for creating the backlog file table for data provider'''
syncfile_create_backlog_file_table_for_provider = """ CREATE TABLE IF NOT EXISTS %s (
                                        file_name varchar(255) not null,
                                        file_id varchar(64) not null,
                                        file_version int not null,
                                        format varchar(32) not null,
                                        file_status int default 2,
                                        change_status_timestamp real not null,
                                        primary key(file_name,file_id,file_version,format)
                                    ); """
"""
file_status:
    2:new_comer
    1:send to subscriber and not receive the received successful feedback message from the subscriber
    0:receive the received successful feedback message from the subscriber
"""


'''sql for creating the publisher table for data consumer'''
syncfile_create_publisher_table_for_consumer = """ CREATE TABLE IF NOT EXISTS sync_publishers (
                                        subscriber_ip varchar(32) not null,
                                        subscriber_port int not null,
                                        publisher_ip varchar(32) not null,
                                        publisher_port varchar(256) not null,
                                        publish_status int default 2,
                                        primary key (subscriber_port)
                                    ); """
"""
publish_status:
2:new-comer
1:running
0:unsubscribed
# -1:unsubscribed
"""



'''sql for dropping  the receieved file table for data consumer, if exists'''
syncfile_drop_exist_recieved_file_table_for_consumer = """ DROP TABLE IF EXISTS %s ; """


'''sql for creating the receieved file table for data consumer'''
syncfile_create_recieved_file_table_for_consumer = """ CREATE TABLE IF NOT EXISTS %s (
                                        file_name varchar(255) not null,
                                        file_id varchar(64) not null,
                                        file_version int not null,
                                        format varchar(32) not null,
                                        ingestion_date real not null,
                                        primary key(file_name,file_id,file_version,format)
                                    ); """


'''sql for creating server running status table in the data provider side'''
syncfile_create_server_running_status_table_in_data_provider_side = """ CREATE TABLE IF NOT EXISTS server_running_status (
                                        running_status int not null,
                                        server_ip varchar(64) not null,
                                        server_port int not null,
                                        primary key (server_ip, server_port)
                                    ); """


'''sql for creating server running status table in the data consumer side'''
syncfile_create_server_running_status_table_in_data_consumer_side = """ CREATE TABLE IF NOT EXISTS client_running_status (
                                        running_status int not null,
                                        client_ip varchar(64) not null,
                                        primary key (client_ip)
                                    ); """


'''sql for creating sallow_suicide_repair_flag_for_subscriber table in the data consumer side'''
syncfile_create_allow_suicide_repair_flag_for_subscriber_table_in_data_consumer_side = """ CREATE TABLE IF NOT EXISTS allow_suicide_repair_flag_for_subscriber (
                                        allow_suicide_repair_flag int not null,
                                        client_ip varchar(64) not null,
                                        primary key (client_ip)
                                    ); """


'''sql for creating the ngas files test table for data store and synchronize system'''
syncfile_create_ngas_files_test_table_for_syncfile = """
CREATE TABLE ngas_files_test
(
  disk_id                varchar(128)   not null,
  file_name              varchar(255)   not null,
  file_id                varchar(64)    not null,
  file_version           int            default 1,
  format                 varchar(32)    not null,
  file_size              numeric(20, 0) not null,
  uncompressed_file_size numeric(20, 0) not null,
  compression            varchar(32)    null,
  ingestion_date         real    not null,
  file_ignore            smallint       null,
  checksum               varchar(64)    null,
  checksum_plugin        varchar(64)    null,
  file_status            char(8)        default '00000000',
  creation_date          varchar(23)    null,
  container_id           varchar(36)    null,
  ingestion_rate         real            null,
  io_time                numeric(20, 0) default -1
);
"""