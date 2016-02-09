import time
from gevent.subprocess import Popen, PIPE, check_output

import Husky
import gipc
from sh import lxc

from sockserver import SockServer, SockClient
from worker import Puppet


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

    def calculate_cost(self):
        raise NotImplementedError

    def run(self, tid, task, *argv, **kwargs):
        task_info = Husky.dumps((task, argv, kwargs))
        self.puppet.submit_task(tid, task_info)
        res = self.puppet.fetch_result(tid, True)
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


class LocalService(ServiceBase):
    def __init__(self, s_id, conf):
        super(LocalService, self).__init__(s_id, conf)
        self.puppet_process = None

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


class LocalLXCService(ServiceBase):
    image = "brick-worker"
    cmd_path = "/root/Brick/src/worker.py"

    def __init__(self, s_id, conf):
        super(LocalLXCService, self).__init__(s_id, conf)
        self.puppet_process = None
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

    def start(self):
        super(LocalLXCService, self).start()
        check_output(["lxc", "launch", self.image, self.name, "-p", self.conf])
        host = self.get_ip()
        args = ["lxc", "exec", self.name, self.cmd_path]
        p = Popen(args=args, stdout=PIPE).stdout
        port = int(p.readline().strip())
        self.puppet = SockClient((host, port), keep_alive=False)
        self.puppet.hire_worker()

    def terminate(self):
        self.puppet.fire_worker()
        self.puppet.shutdown()
        lxc.delete(self.name)
        super(LocalLXCService, self).terminate()


if __name__ == '__main__':
    c = LocalLXCService(0, "tiny")
    c.start()
    r0 = c.run(1, lambda x: x + 1, 1)
    print c.s_id, c.conf, c.start_time, c.finish_time
    print c.status, c.tasks
    r1 = c.run(2, lambda r0: r0 + 1, r0)
    c.terminate()