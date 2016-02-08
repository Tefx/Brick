#!/usr/bin/env python

import sys
from sockserver import SockClient
import time

if __name__ == '__main__':
    port = int(sys.argv[1])
    #try:
    client = SockClient(("localhost", port))
    print "SID\tConfig\tLT\t\tTT\tStaus\tTask\tQueue"
    for status in client.get_status():
        sid, conf, st, ft, status, queue = status
        status, current_task = status
        st = time.strftime("%H:%M (%d/%m)",time.localtime(st))
        ft = time.strftime("%H:%M (%d/%m)",time.localtime(ft)) if ft else "Active"
        print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t" % \
              (sid, conf, st, ft, status, current_task, queue)
    #except StandardError:
    #    print "Cannot connect to the server."
