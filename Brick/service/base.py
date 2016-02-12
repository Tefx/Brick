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
        self.lock = Semaphore()
        self.queue = set()
        self._status = "Unknown"
        self.started = False

    def start(self):
        self.lock.acquire()
        if self.started:
            self.lock.release()
            return
        print "Starting service", self.s_id
        self.start_time = time.time()
        self._status = "Booting"
        self.real_start()
        self.started = True
        self.lock.release()

    def terminate(self):
        if not self.puppet:
            return
        self.real_terminate()
        self._status = "Terminated"
        self.puppet = None
        self.finish_time = time.time()

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
                    return self._status
            elif item in ["cpu", "memory"]:
                if self.puppet:
                    return self.puppet.get_attr(item)
                else:
                    return None
        except:
            return "Unknown"

    def __repr__(self):
        return "%s-%d" % (self.conf, self.s_id)
