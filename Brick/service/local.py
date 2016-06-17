import gevent
import gipc
import sh
from gevent import socket

from Brick.sockserver import SockServer, SockClient
from Brick.worker import Puppet
from base import ServiceBase


def try_until(f):
    try:
        return f()
    except socket.error:
        gevent.sleep(1)
        return try_until(f)


class ProcessService(ServiceBase):
    def __init__(self, s_id, conf):
        super(ProcessService, self).__init__(s_id, conf)
        self.puppet_process = None

    def real_start(self):
        pr, pw = gipc.pipe()
        self.puppet_process = gipc.start_process(target=SockServer(Puppet).run, kwargs={"pipe": pw})
        port = pr.get()
        self.puppet = SockClient(("localhost", port), keep_alive=False)
        self.puppet.hire_worker()

    def real_terminate(self):
        self.puppet.fire_worker()
        self.puppet.shutdown()
        self.puppet_process.terminate()


class LXCService(ServiceBase):
    image = "brick-worker"
    # cmd_path = "brick-worker"
    port = 42424

    def __init__(self, s_id, conf):
        super(LXCService, self).__init__(s_id, conf)
        self.puppet_process = None
        self.name = "brick-%s" % self.s_id

    def get_ip(self, nic="eth0"):
        info = sh.lxc.info(self.name)
        if nic in info:
            for line in sh.lxc.info(self.name).splitlines():
                if nic in line:
                    groups = line.strip().split()
                    if len(groups) > 3 and "inet" == groups[1]:
                        return groups[2]
        gevent.sleep(0.5)
        return self.get_ip(nic)

    def real_start(self):
        sh.lxc.launch(self.image, self.name, p=self.conf)
        host = self.get_ip()
        self.puppet = SockClient((host, self.port), keep_alive=False)
        try_until(self.puppet.hire_worker)

    def real_terminate(self):
        self.puppet.fire_worker()
        self.puppet.shutdown()
        sh.lxc.stop(self.name)
        sh.lxc.delete(self.name)
