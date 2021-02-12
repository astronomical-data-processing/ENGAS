# e-NGAS

e-NGAS is an enhanced remote astronomical archive system based on the File-Level  Unlimited Sliding Window (USW) Technique.

e-NGAS is written in Python, and thus highly portable. It supports Python 3.5+.

# Installation


Install dependencies:

1) install Anaconda for Python 3.5+

Install pre-requisites:

1) pip install pymysql
2) pip install DBUtils==1.3
3) pip install IPy

# Getting started

1. For e-NGAS Provider

	a) configure
	
	Upate the configurations in the files (localhost_conf.py and mySQL_conf.py) basing on your situations. 
	
	The configuration files locate in the path: e-NGAS/data_publisher_side_system/main_config/
	
	b) initialize the database
	
	python e-NGAS/data_publisher_side_system/Database_and_data/mysql_db_create_functions.py
	
	c) to test
	
	python e-NGAS/data_publisher_side_system/Database_and_data/mysql_init_ngas_files_test_data_table.py
	
	d) start the Provider:
	
	python e-NGAS/data_publisher_side_system/data_provider/start_publish.py

	f) stop the Provider:
	
	python e-NGAS/data_publisher_side_system/data_provider/start_publish.py
	
2. For e-NGAS Subscriber

	a) configure
	
	Upate the configurations in the files (localhost_conf.py and mySQL_conf.py) basing on your situations. 
	
	The configuration files locate in the path: e-NGAS/data_subscriber_side_system/main_config/
	
	b) initialize the database
	
	python e-NGAS/data_subscriber_side_system/Database_and_data/mysql_db_create_functions.py
	
	c) start the Subscriber:
	
	python e-NGAS/data_subscriber_side_system/data_subscriber/start_subscribe.py
	
	d) stop the Subscriber:
	
	python e-NGAS/data_subscriber_side_system/data_subscriber/stop_subscribe.py
	
	e) subscribe the Provider:
	
	python e-NGAS/data_subscriber_side_system/data_subscriber/subscribe_publisher.py publisher-ip local-ip
	
	f) unsubscribe the Provider:
	
	python e-NGAS/data_subscriber_side_system/data_subscriber/unsubscribe_publisher.py publisher-ip local-ip
	
	
