import curses
import datetime
import itertools
import sys
import time

from tabulate import tabulate

from Brick.sockserver import SockClient


def build_info(client):
    headers = ["SID", "CONF", "BOOT", "TERM", "TIME", "STATE", "CPU%", "MEM%", "TASK", "NQ", "QUEUE"]
    table = []
    res = []
    for status in client.get_status():
        sid, conf, st, ft, status, current_task, queue, cpu, memory = status
        nq = len(queue)
        if nq > 5:
            queue = str(queue[:5])[:-1] + "...]"
        st = datetime.datetime.fromtimestamp(st)
        if ft:
            ft = datetime.datetime.fromtimestamp(ft)
            rt = ft - st
        else:
            rt = datetime.datetime.now() - st
        rt -= datetime.timedelta(microseconds=rt.microseconds)
        st = st.strftime("%Y-%m-%d %H:%M:%S")
        ft = ft.strftime("%Y-%m-%d %H:%M:%S") if ft else "Running"

        res.append([sid, conf, st, ft, rt, status, cpu, memory, current_task, nq, queue])

    work = sorted([x for x in res if x[3] == "Running"], key=lambda x: x[0])
    idle = sorted([y for y in res if y[3] != "Running"], key=lambda x: x[0])

    for item in itertools.chain(work, idle):
        table.append(item)
    return tabulate(table, headers=headers, tablefmt="psql")


def list_status():
    port = int(sys.argv[1])
    client = SockClient(("localhost", port))
    print build_info(client)


def brick_top():
    port = int(sys.argv[1])
    client = SockClient(("localhost", port))

    def output(window):
        curses.use_default_colors()
        while True:
            window.clear()
            window.addstr(build_info(client))
            window.refresh()
            time.sleep(1)

    curses.wrapper(output)
