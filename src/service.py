import time
import gipc
import Husky
from sockserver import SockServer, SockClient
from worker import Puppet


class ServiceBase(object):
    def __init__(self, s_id, conf):
        self.s_id = s_id
        self.conf = conf
        self.start_time = None
        self.finish_time = None

    def start(self):
        self.start_time = time.time()

    def terminate(self):
        self.finish_time = time.time()

    def calculate_cost(self):
        raise NotImplementedError

    def run(self, tid, task, *argv, **kwargs):
        raise NotImplementedError

    def __getattr__(self, item):
        if item == "tasks":
            raise NotImplementedError
        elif item == "status":
            raise NotImplementedError

    def __repr__(self):
        return "%s-%d" % (self.conf, self.s_id)


class LocalService(ServiceBase):
    def __init__(self, s_id, conf):
        super(LocalService, self).__init__(s_id, conf)
        self.puppet_process = None
        self.puppet = None

    def start(self):
        super(LocalService, self).start()
        pr,pw = gipc.pipe()
        self.puppet_process = gipc.start_process(target=SockServer(Puppet).run,kwargs={"pipe":pw})
        port = pr.get()
        self.puppet = SockClient(("localhost", port), keep_alive=False)
        self.puppet.hire_worker()

    def terminate(self):
        self.puppet.fire_worker()
        self.puppet.shutdown()
        self.puppet_process.terminate()
        super(LocalService, self).terminate()

    def run(self, tid, task, *argv, **kwargs):
        task_info = Husky.dumps((task, argv, kwargs))
        self.puppet.submit_task(tid, task_info)
        res = self.puppet.fetch_result(tid, True)
        return Husky.loads(res)

    def __getattr__(self, item):
        if item == "tasks":
            return self.puppet.current_tasks()
        elif item == "status":
            return self.puppet.get_attr("status")