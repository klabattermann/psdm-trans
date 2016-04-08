#!/usr/bin/env  python3 

#
# Parse a gridftp log files and print stats 
#
# Usage:
#    parse_log.py <gridftp server log file>
#  
#


import sys
import gzip
import socket
import datetime

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'GGi', suffix)

ip_name_map = dict()
def hostname(ip_addr):

    name = ip_name_map.get(ip_addr, None)
    if name:
        return name

    try:
        name = socket.gethostbyaddr(ip_addr)[0]
    except:
        name = "unknown"

    ip_name_map[ip_addr] = name
    return name
        
    
    


def read_log(fn):

    if fn.endswith(".gz"):
        fp = gzip.open(fn)
    else:
        fp = open(fn)

    for line in fp:
        data = dict( x.split('=') for x in line.split())
        yield data


    fp.close()


if __name__ == "__main__":
    
    
    for d in read_log(sys.argv[1]):
        start = datetime.datetime.strptime(d['START'], "%Y%m%d%H%M%S.%f")
        stop = datetime.datetime.strptime(d['DATE'], "%Y%m%d%H%M%S.%f")
        delta = stop - start
        rate = float(d['NBYTES']) / delta.total_seconds() / pow(2,20)

        
        hname = hostname(d['DEST'][1:-1])
        print("date {} elap: {:.1f} rate: {:.1f} size: {} fn: {} dest: {}".format(
            start.strftime("%Y%m%d %H:%M"), delta.total_seconds(), rate,
            sizeof_fmt(float(d['NBYTES'])), d['FILE'], hname))
              
        
