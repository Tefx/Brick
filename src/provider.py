import gevent

from service import *


class ProviderBase(object):
    _service_class_ = NotImplemented
    _config_ = NotImplemented

    def __init__(self):
        self.services = {}

    def start_service(self, s_id, s_type):
        self.services[s_id] = self._service_class_(s_id, s_type)
        # self.services[s_id].start()
        return self.services[s_id]

    def stop_service(self, service):
        self.services[service.s_id].terminate()

    def get_service(self, s_id):
        return self.services[s_id]

    def get_service_info(self):
        return self._service_class_.get_config()

    def total_cost(self):
        return sum(self.calculate_price(s) for s in self.services.itervalues())

    def __iter__(self):
        for sid,s in self.services.iteritems():
            yield sid, s

    def shutdown(self):
        for s in self.services.itervalues():
            self.stop_service(s)

    def configurations(self):
        return self._config_.keys()

    def get_config(self, conf):
        return self._config_[conf]

    def calculate_price(self, service):
        raise NotImplementedError

    def quota(self):
        raise None


class LocalProcessProvider(ProviderBase):
    _service_class_ = LocalService
    _config_ = {"local": {"cpu_scale": 1}}

    def calculate_price(self, service):
        return 0


class LocalLXCProvider(ProviderBase):
    _service_class_ = LocalLXCService
    _config_ = {"tiny": {"cpu_scale": 1},
                "small": {"cpu_scale": 2}}

    def calculate_price(self, service):
        return 0

if __name__ == '__main__':
    def long_run(x):
        gevent.sleep(3)
        return x+42


    # provider = LocalProcessProvider()
    provider = LocalLXCProvider()
    s = provider.start_service(0, "tiny")
    if not s.started:
        s.start()
    service = provider.get_service(0)

    let = gevent.spawn(service.run, "test", long_run, 10)
    for sid, s in provider:
        print sid, s.conf, repr(s.statue)
    gevent.sleep(1)
    for sid, s in provider:
        print sid, s.conf, s.statue

    let.join()
    print let.value

    for sid, s in provider:
        print sid, s.conf, s.statue

    provider.stop_service(service)
