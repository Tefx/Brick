import gevent

import qingcloud.iaas

from Brick.sockserver import SockClient
from base import ServiceBase


class QingService(ServiceBase):
    port = 42424

    def __init__(self, s_id, conf,
                 api_keypath, zone, image, keypair, vxnets):
        super(QingService, self).__init__(s_id, conf)
        with open(api_keypath) as f:
            self.api_id = f.readline().split()[1].strip("'")
            self.api_key = f.readline().split()[1].strip("'")
        self.zone = zone
        self.image = image
        self.keypair = keypair
        self.instance_id = None
        self.vxnets = vxnets
        self.host = None

    def wait_booting(self, conn):
        ret = conn.describe_instances(instances=self.instance_id)
        if ret["instance_set"][0]["status"] != "running":
            gevent.sleep(3)
            return self.wait_booting(conn)
        elif not ret["instance_set"][0]["vxnets"][0]["private_ip"]:
            gevent.sleep(3)
            return self.wait_booting(conn)
        else:
            return ret["instance_set"][0]

    def conn_puppet(self):
        self.puppet = SockClient((self.host, self.port), keep_alive=False)
        self.puppet.hire_worker()

    def real_start(self):
        conn = qingcloud.iaas.connect_to_zone(self.zone,
                                              self.api_id,
                                              self.api_key)
        ret = conn.run_instances(image_id=self.image,
                                 instance_type=self.conf,
                                 login_mode="keypair",
                                 login_keypair=self.keypair,
                                 vxnets=[self.vxnets])
        self.instance_id = ret["instances"]
        ret = self.wait_booting(conn)
        self.host = ret["vxnets"][0]["private_ip"]
        self.conn_puppet()

    def real_terminate(self):
        self.puppet.fire_worker()
        self.puppet.shutdown()
        conn = qingcloud.iaas.connect_to_zone(self.zone,
                                              self.api_id,
                                              self.api_key)
        conn.terminate_instances(self.instance_id)


if __name__ == '__main__':
    s = QingService(s_id=0,
                    conf="c1m1",
                    api_keypath="access_key.csv",
                    zone="pek2",
                    image="img-x18zen9y",
                    keypair="kp-p2h7c1sp",
                    vxnets="vxnet-0domhwj")
    s.start()
    print s.run("test", lambda x: "Hello!", 1)
    gevent.sleep(10)
    s.terminate()
