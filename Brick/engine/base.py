import copy
import gevent
import gevent.lock
import gevent.pool
import gevent.queue

import os.path

from Brick.sockserver import SockServer
from Brick.workflow import Workflow, Task


class MonitorServer(object):
    def __init__(self, engine, workflow):
        self.engine = engine
        self.workflow = workflow

    def get_status(self):
        return list(self.engine.current_status())


class EngineBase(object):
    def __init__(self, provider, workflow=None):
        self.dag = None
        self.provider = provider
        self.ready = set()
        self.greenlets = gevent.pool.Group()
        self.lock = gevent.lock.Semaphore()
        if workflow:
            self.workflow = workflow
        else:
            self.workflow = None

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
            status = s.status
            queue = s.tasks
            if isinstance(status, tuple):
                status, current_task = status
                if current_task in queue:
                    queue.remove(current_task)
                for t in self.dag.nodes_iter():
                    if t.tid == current_task:
                        current_task = str(t)
            else:
                current_task = None
            yield s.s_id, s.conf, s.start_time, s.finish_time, status, current_task, queue, s.cpu, s.memory

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
            if isinstance(res, Task):
                w = res.workflow
            elif isinstance(res, Workflow):
                w = res
            elif self.workflow:
                w = self.workflow
            else:
                return res
            time_file = "%s.time" % f.func_name
            if os.path.exists(time_file):
                w.load_time(time_file)
            w.save("%s.dot" % f.func_name)
            self.start_with_server(w)
            w.dump_time(time_file)
            if isinstance(res, Task):
                return res.value
            else:
                return res

        return wrapped
