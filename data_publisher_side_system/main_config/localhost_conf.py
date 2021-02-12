#!/usr/bin/env python
# _*_ coding:utf-8 _*_

# produce experiment data
ngas_server_ip='10.6.7.48'
ngas_server_port=7777
ngas_archived_ip='10.6.7.48'
ngas_archived_port=8000
ngas_archived_dir='/home/ngas/data_publisher_side_system_v1/simulated_experiment_data_files/'
#ngas_archived_data_file_dirs = {'1':'sixTKB','10':'sixHKB','100':'sixMB','1000':'sixTMB','10000':'sixHMB','100000':'sixGB'}

ngas_archived_data_file_dirs = {'1':'sixTKB'}
ngas_store_simulated_experiment_data_files = '/home/ngas/data_publisher_side_system_v1/simulated_experiment_data_files/'
# sixTKB.fits:66240Bytes
#ngas_simulated_original_fits_files_dict = {'1':'sixTKB.fits,2008-11-11T11:11:11.111','10':'sixHKB.fits,2009-11-11T11:11:11.111','100':'sixMB.fits,2010-11-11T11:11:11.111','1000':'sixTMB.fits,2011-11-11T11:11:11.111','10000':'sixHMB.fits,2012-11-11T11:11:11.111','100000':'sixGB.fits,2013-11-11T11:11:11.111'}
ngas_simulated_original_fits_files_dict = {'1':'sixTKB.fits,2008-11-11T11:11:11.111'}
ngas_simulated_experiment_file_number = 2000000
#ngas_generated_experiment_data_files_dict = {'1':'sixTKB.fits','10':'sixHKB.fits','100':'sixMB.fits','1000':'sixTMB.fits','10000':'sixHMB.fits','100000':'sixGB.fits'}
ngas_generated_experiment_data_files_dict = {'1':'sixTKB.fits'}

#ngas_default_no_replication_volume1 = "/home/ngas/NGAS/volume1/"
ngas_volume1 = "/home/ngas/NGAS/volume1/"
#ngas_default_no_replication_volume2 = "/home/ngas/NGAS/volume2/"
ngas_volume2 = "/home/ngas/NGAS/volume2/"

# transfer experiment
engas_server_ip='10.6.7.48'
engas_server_port=65535
engas_process_feedbck_info_worker_number = 4
#engas_timeout_republish unit:minute
#engas_timeout_republish = 0.05 
engas_timeout_republish = 5 
