import time
import gipc
import Husky
from sockserver import SockServer, SockClient
from worker import Puppet
from sh import lxc

class ServiceBase(object):
    def __init__(self, s_id, conf):
        self.s_id = s_id
        self.conf = conf
        self.start_time = None
        self.finish_time = None

    def start(self, **kwargs):
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


class LocalLXCService(ServiceBase):

    def __init__(self, s_id, conf):
        super(LocalLXCService, self).__init__(s_id, conf)
        self.puppet = None
        self.name = "brick-%s" % self.s_id

    def get_ip(self, nic="eth0"):
        info = lxc.info(self.name)
        port = None
        if nic in info:
            for line in lxc.info(self.name).splitlines():
                if nic in line:
                    port = line.split()[-2]
                    break
            return port
        else:
            time.sleep(0.5)
            return self.get_ip(nic)

    def start(self, image):
        lxc.launch(image, self.name, p=self.conf)
        print self.get_ip()

    def terminate(self):
        lxc.delete(self.name)


if __name__ == '__main__':
    c = LocalLXCService(0, "tiny")
    c.start("t0")

    c.terminate()