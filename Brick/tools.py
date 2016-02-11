import sys
import time

from tabulate import tabulate

from Brick.sockserver import SockClient


def list_status():
    port = int(sys.argv[1])
    client = SockClient(("localhost", port))
    headers = ["SID", "CONF", "BOOT", "TERM", "STATE", "T", "Q"]
    table = []
    for status in client.get_status():
        sid, conf, st, ft, status, queue = status
        if status != "Unknown":
            status, current_task = status
            if current_task in queue:
                queue.remove(current_task)
        else:
            current_task = None
        st = time.strftime("%H:%M (%d/%m)", time.localtime(st))
        ft = time.strftime("%H:%M (%d/%m)", time.localtime(ft)) if ft else "Active"
        table.append([sid, conf, st, ft, status, current_task, queue])
    print tabulate(table, headers=headers, tablefmt="psql")
