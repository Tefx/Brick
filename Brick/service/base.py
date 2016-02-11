import time
from gevent.lock import Semaphore

import Husky


class ServiceBase(object):
    def __init__(self, s_id, conf):
        self.s_id = s_id
        self.conf = conf
        self.start_time = None
        self.finish_time = None
        self.puppet = None
        self.started = False
        self.lock = Semaphore()
        self.queue = set()

    def start(self):
        self.lock.acquire()
        if self.started:
            self.lock.release()
            return
        print "Starting service", self.s_id
        self.start_time = time.time()
        self.real_start()
        self.started = True
        self.lock.release()

    def terminate(self):
        self.real_terminate()
        self.finish_time = time.time()
        self.started = False

    def real_start(self):
        raise NotImplementedError

    def real_terminate(self):
        raise NotImplementedError

    def record_task(self, task):
        self.queue.add(task.tid)

    def run(self, tid, task, *argv, **kwargs):
        task_info = Husky.dumps((task, argv, kwargs))
        self.puppet.submit_task(tid, task_info)
        res = self.puppet.fetch_result(tid)
        self.queue.discard(tid)
        return Husky.loads(res)

    def __getattr__(self, item):
        try:
            if item == "tasks":
                return list(self.queue)
            elif item == "status":
                if self.puppet:
                    return self.puppet.get_attr("status")
                else:
                    return "Booting"
        except:
            return "Booting"

    def __repr__(self):
        return "%s-%d" % (self.conf, self.s_id)
