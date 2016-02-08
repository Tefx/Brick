import gevent.pool
import gevent.queue
import gevent
import copy
from sockserver import SockServer


class MonitorServer(object):
    def __init__(self, engine, workflow):
        self.engine = engine
        self.workflow = workflow

    def get_status(self):
        return list(self.engine.current_status())


class EngineBase(object):
    def __init__(self, provider):
        self.dag = None
        self.provider = provider
        self.ready = set()
        self.greenlets = gevent.pool.Group()

    def start(self, workflow):
        self.dag = workflow.dag
        self.before_eval()
        for n,i in self.dag.in_degree_iter():
            if i == 0:
                self.ready.add(n)
        self.launch_ready()

    def shutdown(self):
        self.provider.shutdown()

    def launch_ready(self):
        ts = copy.copy(self.ready)
        self.ready.clear()
        for task in ts:
            service = self.which_service(task)
            self.greenlets.add(gevent.spawn(self.run_task, task, service))

    def run_task(self, task, service):
        task(service)
        for s in self.dag.successors(task):
            if all(p.status == "Finished" for p in self.dag.predecessors(s)):
                self.ready.add(s)
        self.after_task(task, service)
        self.launch_ready()

    def join(self):
        self.greenlets.join()
        self.after_eval()
        self.dag = None

    def start_with_server(self, workflow):
        self.start(workflow)
        queue = gevent.queue.Queue()
        server = gevent.spawn(SockServer(MonitorServer, self, workflow).run, pipe=queue)
        port = queue.get()
        print "Server started on", port
        self.join()
        server.kill()

    def current_status(self):
        for s in self.current_services():
            yield s.s_id, s.conf, s.start_time, s.finish_time, s.status, s.tasks

    def current_services(self):
        raise NotImplementedError

    def which_service(self, task):
        raise NotImplementedError

    def before_eval(self):
        pass

    def after_eval(self):
        pass

    def after_task(self, task, service):
        pass


class LocalFullEngine(EngineBase):
    def __init__(self, provider):
        super(LocalFullEngine, self).__init__(provider)
        self.conf = self.provider.configurations()[0]
        self.sid = 0
        self.services = set()

    def which_service(self, task):
        self.provider.start_service(self.sid, self.conf)
        service = self.provider.get_service(self.sid)
        self.sid += 1
        return service

    def current_services(self):
        return list(self.services)

    def after_task(self, task, service):
        self.provider.stop_service(service)
        self.services.discard(service)


class LocalSingleEngine(EngineBase):
    def __init__(self, provider):
        super(LocalSingleEngine, self).__init__(provider)
        self.conf = self.provider.configurations()[0]
        self.service = None

    def before_eval(self):
        self.service = self.provider.start_service(0, self.conf)

    def after_eval(self):
        self.provider.stop_service(self.service)
        self.service = None

    def which_service(self, task):
        return self.service

    def current_services(self):
        return [self.service]


class LocalFixedEngine(EngineBase):
    def __init__(self, provider, n):
        super(LocalFixedEngine, self).__init__(provider)
        self.conf = self.provider.configurations()[0]
        self.n = n
        self.services = []

    def after_eval(self):
        for s in self.services:
            self.provider.stop_service(s)
        self.services = []

    def which_service(self, task):
        if len(self.services) < self.n:
            s = self.provider.start_service(len(self.services), self.conf)
            self.services.append(s)
        else:
            s = min(self.services, key=lambda x: len(x.tasks))
        return s

    def current_services(self):
        return self.services
