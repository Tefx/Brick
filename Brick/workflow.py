import collections
import copy
import json
import time
from functools import partial
from itertools import chain

import networkx as nx
import os


def search_task2(var):
    ts = []
    if isinstance(var, Task):
        ts.append(var)
    elif isinstance(var, collections.Mapping):
        for v in var.itervalues():
            if isinstance(v, Task):
                ts.append(v)
    elif isinstance(var, collections.Iterable):
        for v in var:
            if isinstance(v, Task):
                ts.append(v)
    return ts


def repleace_task2(var, attr):
    if isinstance(var, Task):
        return getattr(var, attr)
    elif isinstance(var, collections.MutableMapping):
        var2 = copy.copy(var)
        for k, v in var2.iteritems():
            if isinstance(v, Task):
                var2[k] = getattr(v, attr)
        return var2
    elif isinstance(var, collections.MutableSequence):
        var2 = copy.copy(var)
        for i in range(len(var2)):
            if isinstance(var2[i], Task):
                var2[i] = getattr(var2[i], attr)
        return var2
    elif isinstance(var, collections.MutableSet):
        var2 = copy.copy(var)
        tmp = []
        for v in var2:
            if isinstance(v, Task):
                tmp.append(v)
        for v in tmp:
            var2.discard(v)
            var2.add(getattr(v, attr))
        return var2
    return var


class Task(object):
    def __init__(self, tid, f, argv, kwargs):
        self.f = f
        self.argv = list(argv)
        self.kwargs = kwargs
        self.tid = tid
        self.metadata = None
        self.status = "Not Started"
        self.ref_time = {}

    def __call__(self, service):
        self.status = "Running"
        start_time = time.time()
        argv = [repleace_task2(x, "value") for x in self.argv]
        kwargs = {k: repleace_task2(v, "value") for k, v in self.kwargs.items()}
        self.value = service.run(self.tid, self.f, *argv, **kwargs)
        finish_time = time.time()
        self.ref_time[service.conf] = finish_time - start_time
        self.status = "Finished"
        return self.value

    def __repr__(self):
        return "%d-%s" % (self.tid, self.f.__name__)

    def __hash__(self):
        return self.tid


def default_time_func(task, conf, conf_info):
    if conf in task.ref_time:
        return task.ref_time[conf]
    elif task.ref_time != {}:
        return task.ref_time.values()[0]
    else:
        return 1


class Workflow(object):
    def __init__(self):
        self.gid = 1
        self.ref_time = {}
        self.dag = nx.DiGraph()

    def get_gid(self):
        g = self.gid
        self.gid += 1
        return g

    def create_task(self,
                    meta_func=lambda *argv, **kwargs: (argv, kwargs),
                    time_func=default_time_func):
        def wrapper(f):
            def wrapped(*argv, **kwargs):
                tid = self.get_gid()
                task = Task(tid, f, argv, kwargs)
                argv_meta = [repleace_task2(x, "metadata") for x in argv]
                kwargs_meta = {k: repleace_task2(v, "metadata") for k, v in kwargs.items()}
                task.metadata = meta_func(*argv_meta, **kwargs_meta)
                task.time_func = partial(time_func, task)
                for v in chain(argv, kwargs.itervalues()):
                    for p in search_task2(v):
                        self.dag.add_edge(p, task)
                task.workflow = self
                return task
            return wrapped
        return wrapper

    def __iter__(self):
        for n in self.dag.nodes():
            yield n

    def save(self, path):
        with open(path, "w") as f:
            print >> f, "digraph {\n%s\n}" % \
                        os.linesep.join('    "%s"->"%s"' % (p, t) for p, t in self.dag.edges())

    def load_time(self, path):
        with open(path, "r") as f:
            self.ref_time = json.load(f)

    def dump_time(self, path):
        tt = {t.tid: t.ref_time for t in self.dag.nodes()}
        with open(path, "w") as f:
            json.dump(tt, f)