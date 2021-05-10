# eNGAS

eNGAS is an enhanced remote astronomical archive system based on the File-Level  Unlimited Sliding Window (USW) Technique.

eNGAS is written in Python, and thus highly portable. It supports Python 3.5+.

# Installation


Install dependencies:

1) install Anaconda for Python 3.5+

Install pre-requisites:

1) pip install pymysql
2) pip install DBUtils==1.3
3) pip install IPy

# Getting started

1. For eNGAS Provider

	a) configure
	
	Upate the configurations in the files (localhost_conf.py and mySQL_conf.py) basing on your situations. 
	
	The configuration files locate in the path: eNGAS/data_publisher_side_system/main_config/
	
	b) initialize the database
	
	python eNGAS/data_publisher_side_system/Database_and_data/mysql_db_create_functions.py
	
	c) to test
	
	python eNGAS/data_publisher_side_system/Database_and_data/mysql_init_ngas_files_test_data_table.py
	
	Note that:
	
	The table ngas_files_test is based on the ngas_files of the Next Generation Archive System (NGAS). Therefore, eNGAS Provider needs to the records archived by NGAS in its current version.
	
	NGAS is a very feature rich, archive handling and management system. In its core it is a HTTP based object storage system. It can be deployed on single small servers, or in globally distributed clusters. NGAS is written in Python, and thus highly portable. As of version 11 it supports both Python 2.7 and 3.5+. 
	
	NGAS has been originally developed and used extensively at the European Southern Observatory to archive the data at the observatories in Chile and mirror the data to the headquarters in Germany. NGAS has also been deployed at the National Radio
Astronomy Observatory (NRAO) in Socorro and Charlottesville and the Atacama Large Millimeter/Submillimeter Array (ALMA) is using the system to collect the data directly at the control site in the Chilean Andes and then distribute the data to Santiago and further on to the ALMA regional centers in the US, Japan and Germany. 
	The version of NGAS delivered [in this distribution](https://github.com/ICRAR/ngas) is a branch of the original ALMA version. International Centre for Radio Astronomy Research ([ICRAR](http://www.icrar.org)) had further developed and adopted NGAS to deal with the much higher data rate of the Murchison Widefield Array (MWA) and quite some work went into the installation and automatic test features.
	 
	 NGAS's documentation is [online](https://ngas.readthedocs.io/en/master/) and the NGAS storage system is [online](https://github.com/ICRAR/ngas).
	
	
	d) start the Provider:
	
	python eNGAS/data_publisher_side_system/data_provider/start_publish.py

	f) stop the Provider:
	
	python eNGAS/data_publisher_side_system/data_provider/start_publish.py
	
	
2. For eNGAS Subscriber

	a) configure
	
	Upate the configurations in the files (localhost_conf.py and mySQL_conf.py) basing on your situations. 
	
	The configuration files locate in the path: e-NGAS/data_subscriber_side_system/main_config/
	
	b) initialize the database
	
	python eNGAS/data_subscriber_side_system/Database_and_data/mysql_db_create_functions.py
	
	c) start the Subscriber:
	
	python eNGAS/data_subscriber_side_system/data_subscriber/start_subscribe.py
	
	d) stop the Subscriber:
	
	python eNGAS/data_subscriber_side_system/data_subscriber/stop_subscribe.py
	
	e) subscribe the Provider:
	
	python eNGAS/data_subscriber_side_system/data_subscriber/subscribe_publisher.py publisher-ip local-ip
	
	f) unsubscribe the Provider:
	
	python eNGAS/data_subscriber_side_system/data_subscriber/unsubscribe_publisher.py publisher-ip local-ip
	
	
