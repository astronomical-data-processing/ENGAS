#!/usr/bin/env python
# _*_ coding:utf-8 _*_

# produce experiment data
ngas_server_ip='127.0.0.1'
ngas_server_port=7777
ngas_archived_ip='localhost'
ngas_archived_port=8000
ngas_archived_dir='/home/ngas/data_publisher_side_system_v1/simulated_experiment_data_files/'
ngas_archived_data_file_dirs = {'1':'sixTKB','10':'sixHKB','100':'sixMB','1000':'sixTMB','10000':'sixHMB','100000':'sixGB'}

ngas_store_simulated_experiment_data_files = '/home/ngas/data_publisher_side_system_v1/simulated_experiment_data_files/'
# sixTKB.fits:66240Bytes
ngas_simulated_original_fits_files_dict = {'1':'sixTKB.fits,2008-11-11T11:11:11.111','10':'sixHKB.fits,2009-11-11T11:11:11.111','100':'sixMB.fits,2010-11-11T11:11:11.111','1000':'sixTMB.fits,2011-11-11T11:11:11.111','10000':'sixHMB.fits,2012-11-11T11:11:11.111','100000':'sixGB.fits,2013-11-11T11:11:11.111'}
ngas_simulated_experiment_file_number = 3
ngas_generated_experiment_data_files_dict = {'1':'sixTKB.fits','10':'sixHKB.fits','100':'sixMB.fits','1000':'sixTMB.fits','10000':'sixHMB.fits','100000':'sixGB.fits'}

#start publisher
#ngas_default_no_replication_volume1 = "/home/ngas/NGAS/volume1/"
ngas_volume1 = "/home/ngas/NGAS/volume1/"
#ngas_default_no_replication_volume2 = "/home/ngas/NGAS/volume2/"
ngas_volume2 = "/home/ngas/NGAS/volume2/"

# transfer experiment
engas_server_ip='10.6.7.48'
engas_server_port=65535
engas_process_feedbck_info_thread_number = 20
transfer_data_port_number = 20 
#end publisher

#start subscriber
local_ip = '10.6.7.47'
#received_data_files_dir = '/home/engas/data_subscriber_side_system_v5/received_data_files_dir/'
received_data_files_dir = '/home/ngas/NGAS/volume1'
process_data_worker_number_for_one_receiver = 2
#end subscriber
