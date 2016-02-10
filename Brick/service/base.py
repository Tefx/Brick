import time

import Husky


class ServiceBase(object):
    def __init__(self, s_id, conf):
        self.s_id = s_id
        self.conf = conf
        self.start_time = None
        self.finish_time = None
        self.puppet = None
        self.started = False

    def start(self):
        print "Starting service", self.s_id
        self.start_time = time.time()
        self.started = True

    def terminate(self):
        self.finish_time = time.time()
        self.started = False

    def run(self, tid, task, *argv, **kwargs):
        task_info = Husky.dumps((task, argv, kwargs))
        self.puppet.submit_task(tid, task_info)
        res = self.puppet.fetch_result(tid)
        return Husky.loads(res)

    def __getattr__(self, item):
        if not self.puppet:
            return "Not available"
        if item == "tasks":
            return self.puppet.current_tasks()
        elif item == "status":
            return self.puppet.get_attr("status")

    def __repr__(self):
        return "%s-%d" % (self.conf, self.s_id)
