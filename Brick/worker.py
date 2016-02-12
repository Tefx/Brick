#!/usr/bin/env python
import gevent
import sys
import time
from gevent.event import AsyncResult
from gevent.lock import Semaphore

import Husky
import gipc
import psutil

from Brick.sockserver import SockServer, SockClient


def process_worker(task_queue):
    while True:
        tid, task_info = task_queue.get()
        task_queue.put((-1, ("Working", tid)))
        (task, argv, kwargs) = Husky.loads(task_info)
        start_time = time.time()
        res = task(*argv, **kwargs)
        used_time = time.time() - start_time
        task_queue.put((tid, Husky.dumps((res, used_time))))
        task_queue.put((-1, ("Idle", None)))


class Puppet(object):
    def __init__(self):
        self.results = {}
        self.status = ("Idle", None)
        self.worker = None
        self.operator = None
        self.lock = Semaphore()

    def receive_info(self):
        while True:
            (info_id, info) = self.task_queue.get()
            if info_id > 0:
                self.results[info_id].set(info)
            elif info_id == -1:
                self.status = info

    def submit_task(self, tid, task_info):
        self.results[tid] = AsyncResult()
        self.lock.acquire()
        self.task_queue.put((tid, task_info))
        self.lock.release()

    def fetch_result(self, tid):
        res = self.results[tid].get()
        del self.results[tid]
        return res

    def hire_worker(self):
        tq_worker, self.task_queue = gipc.pipe(duplex=True)
        self.worker = gipc.start_process(target=process_worker, args=(tq_worker,))
        self.operator = gevent.spawn(self.receive_info)

    def fire_worker(self):
        self.worker.terminate()
        self.operator.kill()

    def get_attr(self, attr):
        if attr == "cpu":
            return psutil.cpu_percent(interval=None)
        elif attr == "memory":
            return psutil.virtual_memory().percent
        else:
            return getattr(self, attr, "No such attribute")

    def current_tasks(self):
        return self.results.keys()


def run_worker():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    SockServer(Puppet).run(port)


def test_worker():
    host = sys.argv[1]
    port = int(sys.argv[2])
    client = SockClient((host, port))
    client.hire_worker()
    gevent.sleep(1)
    client.fire_worker()
    client.shutdown()
    print "Successed!"
