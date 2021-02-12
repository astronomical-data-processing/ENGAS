#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import psutil
import re
import sys


def get_process_info(process_name):

    pids_list = psutil.pids()
    pid_numbers = len(pids_list)
    process_name_re = re.compile(process_name, re.I)
    position = 0
    for pids_item in pids_list:
        process = psutil.Process(pids_item)
        try:
            #print process.name(), process.memory_info(), process.pid, process.cmdline()
            cmdline_list = process.cmdline()
            if cmdline_list[1] == "start_client.py":
                print(process.name(), process.memory_info(), process.pid, process.cmdline())
                print(psutil.virtual_memory())
                print(psutil.swap_memory())
        except (psutil.ZombieProcess, psutil.AccessDenied, psutil.NoSuchProcess, IndexError):
            continue
        except Exception as _:
            continue
        position = position + 1
        if position == 10:
            break


if __name__ == '__main__':
    get_process_info("start_client")
    print('test')