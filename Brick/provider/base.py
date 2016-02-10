class ProviderBase(object):
    _service_class_ = NotImplemented
    _config_ = NotImplemented
    _quota_ = NotImplemented

    def __init__(self, *service_args, **service_kwargs):
        self.services = {}
        self.service_args = service_args
        self.service_kwargs = service_kwargs

    def start_service(self, s_id, s_type):
        self.services[s_id] = self._service_class_(s_id, s_type, *self.service_args, **self.service_kwargs)
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
        for sid, s in self.services.iteritems():
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
