import time
from gevent.subprocess import Popen, PIPE, check_output

import gipc
from sh import lxc

from Brick.sockserver import SockServer, SockClient
from Brick.worker import Puppet
from base import ServiceBase


class ProcessService(ServiceBase):
    def __init__(self, s_id, conf):
        super(ProcessService, self).__init__(s_id, conf)
        self.puppet_process = None

    def start(self):
        super(ProcessService, self).start()
        pr, pw = gipc.pipe()
        self.puppet_process = gipc.start_process(target=SockServer(Puppet).run, kwargs={"pipe": pw})
        port = pr.get()
        self.puppet = SockClient(("localhost", port), keep_alive=False)
        self.puppet.hire_worker()

    def terminate(self):
        self.puppet.fire_worker()
        self.puppet.shutdown()
        self.puppet_process.terminate()
        super(ProcessService, self).terminate()


class LXCService(ServiceBase):
    image = "brick-worker"
    cmd_path = "brick-worker"

    def __init__(self, s_id, conf):
        super(LXCService, self).__init__(s_id, conf)
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
        super(LXCService, self).start()
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
        super(LXCService, self).terminate()
