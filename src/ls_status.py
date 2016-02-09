#!/usr/bin/env python

import sys
import time

from tabulate import tabulate

from sockserver import SockClient

if __name__ == '__main__':
    port = int(sys.argv[1])
    try:
        client = SockClient(("localhost", port))
        headers = ["SID", "Conf", "Start", "Finish", "Staus", "T", "Q"]
        table = []
        for status in client.get_status():
            sid, conf, st, ft, status, queue = status
            if status != "Not available":
                status, current_task = status
                if current_task in queue:
                    queue.remove(current_task)
            else:
                current_task = None
            st = time.strftime("%H:%M (%d/%m)", time.localtime(st))
            ft = time.strftime("%H:%M (%d/%m)", time.localtime(ft)) if ft else "Active"
            table.append([sid, conf, st, ft, status, current_task, queue])
        print tabulate(table, headers=headers, tablefmt="psql")
    except StandardError:
        print "Cannot connect to the server."
