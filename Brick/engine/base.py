import copy
import gevent
import gevent.lock
import gevent.pool
import gevent.queue

from Brick.sockserver import SockServer


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
        self.lock = gevent.lock.Semaphore()

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
            self.lock.acquire()
            service = self.which_service(task)
            service.record_task(task)
            self.lock.release()
            self.greenlets.add(gevent.spawn(self.run_task, task, service))

    def run_task(self, task, service):
        if not service.started:
            service.start()
        print "Launching task", task
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
        queue = gevent.queue.Queue()
        server = gevent.spawn(SockServer(MonitorServer, self, workflow).run, pipe=queue)
        port = queue.get()
        print "Server started on", port
        self.start(workflow)
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

    def __call__(self, f):
        def wrapped(*argv, **kwargs):
            res = f(*argv, **kwargs)
            w = res.workflow
            w.save("%s.dot" % f.func_name)
            self.start_with_server(w)
            return res.value

        return wrapped
