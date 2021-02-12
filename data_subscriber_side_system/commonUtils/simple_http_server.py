#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import sys
import os
import time
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
HandlerClass = BaseHTTPRequestHandler
ServerClass  = HTTPServer
Protocol     = "HTTP/1.0"

def init_simple_http_server(archived_ip,archived_port,archived_dir='.'):
	os.chdir(archived_dir)
	server_address = (archived_ip, archived_port)
	HandlerClass.protocol_version = Protocol
	httpd = ServerClass(server_address, HandlerClass)
	return httpd


def start_simple_http_server(httpd):
	sa = httpd.socket.getsockname()
	print("Serving HTTP on", sa[0], "port", sa[1])
	server_serve = threading.Thread(target=httpd.serve_forever)
	server_serve.daemon = True
	server_serve.start()


def stop_simple_http_server(httpd):
	sa = httpd.socket.getsockname()
	assassin = threading.Thread(target=httpd.shutdown)
	assassin.daemon = True
	assassin.start()
	print("Serving HTTP off", sa[0], "port", sa[1])

# if __name__ == '__main__':
# 	httpd = init_simple_http_server('127.0.0.1',8000,'/Users/shicongming/Documents/Python2Program/data_publisher_side_system_v1/data_provider')
# 	start_simple_http_server(httpd)
# 	time.sleep(30)
# 	print('sleeping 30 over')
# 	stop_simple_http_server(httpd)