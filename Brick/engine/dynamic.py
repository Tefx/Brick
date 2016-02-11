from base import EngineBase


class FullEngine(EngineBase):
    def __init__(self, provider, **kwargs):
        super(FullEngine, self).__init__(provider, **kwargs)
        self.conf = self.provider.configurations()[0]
        self.sid = 0
        self.services = set()

    def which_service(self, task):
        self.sid += 1
        s = self.provider.start_service(self.sid, self.conf)
        self.services.add(s)
        return s

    def current_services(self):
        return list(self.services)

    def after_task(self, task, service):
        self.provider.stop_service(service)
        self.services.discard(service)


class SingleEngine(EngineBase):
    def __init__(self, provider, **kwargs):
        super(SingleEngine, self).__init__(provider, **kwargs)
        self.conf = self.provider.configurations()[0]
        self.service = None

    def before_eval(self):
        self.service = self.provider.start_service(1, self.conf)

    def after_eval(self):
        self.provider.stop_service(self.service)
        self.service = None

    def which_service(self, task):
        return self.service

    def current_services(self):
        return [self.service]


class LimitEngine(EngineBase):
    def __init__(self, provider, n, **kwargs):
        super(LimitEngine, self).__init__(provider, **kwargs)
        self.conf = self.provider.configurations()[0]
        self.n = n
        self.services = []

    def after_eval(self):
        for s in self.services:
            self.provider.stop_service(s)
        self.services = []

    def which_service(self, task):
        for s in self.services:
            if len(s.tasks) == 0:
                return s
        if len(self.services) < self.n:
            s = self.provider.start_service(len(self.services) + 1, self.conf)
            self.services.append(s)
        else:
            s = min(self.services, key=lambda x: len(x.tasks))
        return s

    def current_services(self):
        return self.services
