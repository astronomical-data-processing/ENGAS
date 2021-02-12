#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import sys
import threading
from astropy.io import fits
import numpy as np
import os
import time
import collections
import calendar
#sys.path.insert(0,'/Users/shicongming/Documents/PythonProgram/data_store_synchronize_system/')
sys.path.insert(0,os.path.split(os.path.split(os.path.realpath(__file__))[0])[0]+os.sep)
from main_config import localhost_conf

_formatspec = collections.namedtuple('_formatspec', 'format msecs')
FMT_DATE_ONLY        = _formatspec('%Y-%m-%d',          False)
FMT_TIME_ONLY        = _formatspec('%H:%M:%S',          True)
FMT_TIME_ONLY_NOMSEC = _formatspec('%H:%M:%S',          False)
#FMT_DATETIME         = _formatspec('%Y-%m-%dT%H:%M:%S.%f', True)
FMT_DATETIME         = _formatspec('%Y-%m-%dT%H:%M:%S', True)
FMT_DATETIME_NOMSEC  = _formatspec('%Y-%m-%dT%H:%M:%S', False)

def fromiso8601(s, local=False, fmt=FMT_DATETIME):
    """
    Converts string `s` to the number of seconds since the epoch,
    as returned by time.time.

    `s` should be expressed in the ISO 8601 extended format indicated by the
    `fmt` flag.

    If `local` is False then `s` is assumed to represent time at UTC,
    otherwise it is interpreted as local time. In either case `s` should contain
    neither time-zone information nor the 'Z' suffix.
    """

    ms = 0.
    if fmt.msecs:
        s, ms = s.split(".")
        ms = float(ms)/1000.

    totime = calendar.timegm if not local else time.mktime
    t = totime(time.strptime(s, fmt.format))
    return t + ms


def toiso8601(t=None, local=False, fmt=FMT_DATETIME):
    """
    Converts the time value `t` to a string formatted using the ISO 8601
    extended format indicated by the `fmt` flag.

    `t` is expressed in numbers of seconds since the epoch, as returned by
    time.time. If `t` is not given, the current time is used instead.

    If `local` is False then the resulting string represents the time at UTC.
    If `local` is True then the resulting string represents the local time.
    In either case the string contains neither time-zone information nor the 'Z'
    suffix.
    """

    if t is None:
        t = time.time()

    totuple = time.gmtime if not local else time.localtime
    timeStamp = time.strftime(fmt.format, totuple(t))
    if fmt.msecs:
        t = (t - int(t)) * 1000
        timeStamp += '.%03d' % int(t)

    return timeStamp


# def get_store_generated_original_simulated_data_files_directory():
#     store_generated_original_simulated_data_files_dir = localhost_conf.ngas_store_generated_original_simulated_data_files_dir
#     if store_generated_original_simulated_data_files_dir == '':
#         store_file_directory = os.path.split(os.path.realpath(__file__))[
#                                    0] + os.sep + 'generated_original_simulated_data_files' + os.sep
#     elif os.path.isdir(store_generated_original_simulated_data_files_dir):
#         if store_generated_original_simulated_data_files_dir[-1] == '/':
#             store_file_directory = store_generated_original_simulated_data_files_dir
#         else:
#             store_file_directory = store_generated_original_simulated_data_files_dir + os.sep
#     else:
#         store_file_directory = os.path.split(os.path.realpath(__file__))[
#                                    0] + os.sep + 'generated_original_simulated_data_files' + os.sep
#     store_generated_original_simulated_data_files_dir = store_file_directory
#     return store_generated_original_simulated_data_files_dir


def get_store_simulated_experiment_data_files_directory():
    store_simulated_experiment_data_files = localhost_conf.ngas_store_simulated_experiment_data_files
    if store_simulated_experiment_data_files == '':
        store_file_directory = os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep + 'simulated_experiment_data_files' + os.sep
    elif os.path.isdir(store_simulated_experiment_data_files):
        if store_simulated_experiment_data_files[-1] == '/':
            store_file_directory = store_simulated_experiment_data_files
        else:
            store_file_directory = store_simulated_experiment_data_files + os.sep
    else:
        store_file_directory = os.path.split(os.path.split(os.path.realpath(__file__))[0])[0] + os.sep + 'simulated_experiment_data_files' + os.sep
    store_simulated_experiment_data_files = store_file_directory
    return store_simulated_experiment_data_files

def simulate_original_simulated_data_fits_files():
    # 由于ngas中的fits文件头需要有ARCFILE关键字，（格式：ARCFILE = 'NCU.2003-11-11T11:11:11.111' / ARCFILE = 'TEST.2001-05-08T15:25:00.123'）
    # 所以模拟生成的fits中加入ARCFILE关键字
    # sixTKB.fits:66240Bytes
    arcfile_prefix = 'Simulation'
    #simulated_archived_time = '2008-11-11T11:11:11.111'
    #fits_dict = {'1':'sixTKB.fits,2008-11-11T11:11:11.111','10':'sixHKB.fits,2009-11-11T11:11:11.111','100':'sixMB.fits,2010-11-11T11:11:11.111','1000':'sixTMB.fits,2011-11-11T11:11:11.111','1000':'sixHMB.fits,2012-11-11T11:11:11.111','10000':'sixGB.fits,2013-11-11T11:11:11.111'}
    fits_dict = localhost_conf.ngas_simulated_original_fits_files_dict
    for fits_key in fits_dict:
        fits_value = fits_dict[fits_key]
        fits_value_split = fits_value.split(',')
        fake_filename = fits_value_split[0]
        simulated_archived_time = fits_value_split[1]
        factor = int(fits_key)
        simulated_header = fits.Header()
        simulated_header['ARCFILE'] = arcfile_prefix + '.' + simulated_archived_time
        simulated_data = np.arange((23 * factor - 1) * 360 * 1.0)
        hdu = fits.PrimaryHDU(simulated_data, header=simulated_header)
        #fits_filename = get_store_generated_original_simulated_data_files_directory() + fake_filename
        fits_filename = get_store_simulated_experiment_data_files_directory() + 'generated_original_simulated_data_files' + os.sep + fake_filename
        hdu.writeto(fits_filename)
    print('Finished simulated original fits file:\n' + str(fits_dict.values()))


def generate_experiment_data_files():
    file_number = localhost_conf.ngas_simulated_experiment_file_number
    experiment_data_file_dict = localhost_conf.ngas_generated_experiment_data_files_dict
    #simulated_original_file_dir_path = get_store_generated_original_simulated_data_files_directory()
    simulated_experiment_data_files_dir = get_store_simulated_experiment_data_files_directory()
    simulated_original_file_dir_path = simulated_experiment_data_files_dir + 'generated_original_simulated_data_files' + os.sep
    for dick_key in experiment_data_file_dict:
        fake_filename = experiment_data_file_dict[dick_key]
        fits_filename = simulated_original_file_dir_path + fake_filename
        for i in range(file_number):
            with fits.open(fits_filename) as fits_file:
                arcfile = fits_file[0].header['ARCFILE']
                arcfile_prefix = arcfile[0:10]
                arcfile_suffix = arcfile[11:]
                time_value = fromiso8601(arcfile_suffix)
                new_time_value = time_value + i
                arcfile_suffix = toiso8601(new_time_value)
                arcfile_value = arcfile_prefix + '.' + arcfile_suffix
                fits_file[0].header['ARCFILE'] = arcfile_value
                # print(arcfile_value)
                experiment_data_filename = simulated_experiment_data_files_dir + fake_filename.split('.')[0] + os.sep + str(i) + '.fits'
                fits_file.writeto(experiment_data_filename)
    print('Finished experiment_data_files\n'+ str(experiment_data_file_dict.values()))


if __name__ == '__main__':
    simulate_original_simulated_data_fits_files()
    generate_experiment_data_files()

