#!/usr/bin/env python
# _*_ coding:utf-8 _*_
import time
import collections
import calendar

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
    """

    ms = 0.
    h = s
    if fmt.msecs:
        s, ms = s.split(".")
        ms = float(ms)/float(10**6)


    totime = calendar.timegm if not local else time.mktime
    #t = totime(time.strptime(s, fmt.format))
    t = totime(time.strptime(h, fmt.format))
    return t + ms
    """

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
    """
    if t is None:
        t = time.time()

    totuple = time.gmtime if not local else time.localtime
    timeStamp = time.strftime(fmt.format, totuple(t))
    timeStamp = timeStamp.split('.')[0]
    if fmt.msecs:
        t = (t - int(t)) * pow(10, 6)
        format_defined = '.%0' + str(6) + 'd'
        timeStamp += format_defined%int(t)
    return timeStamp
    """


_UNIX_EPOCH_AS_MJD = 40587.
def tomjd(t=None):
    """
    Returns the Modified Julian Date for the given Unix time.
    """
    if t is None:
        t = time.time()
    return t/86400. + _UNIX_EPOCH_AS_MJD

def frommjd(mjd):
    """
    Returns the Unix time for the given Modified Julian Date.

    This algorithm is suitable at least for dates after the Unix Epoch.
    """
    # 40587 is the MJD for the Unix Epoch
    return (mjd - _UNIX_EPOCH_AS_MJD)*86400.