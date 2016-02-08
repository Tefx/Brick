from itertools import chain
import collections
import time
import networkx as nx
import os
import copy
import json


def search_task(top_var):
    ts = []
    if isinstance(top_var, Task):
        ts.append(top_var)
    elif isinstance(top_var, collections.MutableMapping):
        for inner_var in top_var.itervalues():
            ts += search_task(inner_var)
    elif isinstance(top_var, collections.Iterable):
        for inner_var in top_var:
            ts += search_task(inner_var)
    return ts


def replace_task(iterable, attr):
    if isinstance(iterable, collections.MutableMapping):
        for k, v in iterable.items():
            if isinstance(v, Task):
                iterable[k] = getattr(v, attr)
            elif isinstance(v, collections.Iterable):
                iterable[k] = replace_task(v, attr)
    elif isinstance(iterable, collections.MutableSequence):
        for k in range(len(iterable)):
            v = iterable[k]
            if isinstance(v, Task):
                iterable[k] = getattr(v, attr)
            elif isinstance(v, collections.Iterable):
                iterable[k] = replace_task(v, attr)
    elif isinstance(iterable, collections.MutableSet):
        for v in iterable:
            if isinstance(v, Task):
                iterable.discard(v)
                iterable.add(getattr(v, attr))
            elif isinstance(v, collections.Iterable):
                iterable.discard(v)
                iterable.add(replace_task(v, attr))
    return iterable


class Task(object):
    def __init__(self, engine, f, argv, kwargs):
        self.f = f
        self.argv = list(argv)
        self.kwargs = kwargs
        self.tid = engine.get_gid()
        self.metadata = None
        self.status = "Not Started"
        self.exec_time = {}

    def __call__(self, service):
        self.status = "Running"
        start_time = time.time()
        argv = replace_task(list(self.argv), "value")
        kwargs = replace_task(dict(self.kwargs), "value")
        self.value = service.run(self.tid, self.f, *argv, **kwargs)
        finish_time = time.time()
        self.exec_time[service.conf] = finish_time - start_time
        self.status = "Finished"
        return self.value

    def __str__(self):
        return "%d-%s" % (self.tid, self.f.__name__)

    def __hash__(self):
        return self.tid


class Workflow(object):
    def __init__(self):
        self.gid = 1
        self.exectime = {}
        self.dag = nx.DiGraph()

    def get_gid(self):
        g = self.gid
        self.gid += 1
        return g

    def create_task(self,
                    meta_func=lambda *argv, **kwargs: (argv, kwargs),
                    time_func=lambda exec_time, conf, *argv, **kwargs: sum(exec_time[conf])/len(exec_time[conf])):
        def wrapper(f):
            def wrapped(*argv, **kwargs):
                task = Task(self, f, argv, kwargs)
                argv_meta = list(copy.deepcopy(argv))
                kwargs_meta = dict(copy.deepcopy(kwargs))
                argv_meta = replace_task(argv_meta, "metadata")
                kwargs_meta = replace_task(kwargs_meta, "metadata")
                task.metadata = meta_func(*argv_meta, **kwargs_meta)
                for v in chain(argv, kwargs.itervalues()):
                    for p in search_task(v):
                        self.dag.add_edge(p, task)
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
            self.exectime = json.load(f)

    def dump_time(self, path):
        tt = {t.tid:t.exec_time for t in self.dag.nodes()}
        with open(path, "w") as f:
            json.dump(tt, f)
