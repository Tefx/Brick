import Husky
from gevent.event import AsyncResult
import gevent
import gipc


def process_worker(task_queue):
    while True:
        tid, task_info = task_queue.get()
        task_queue.put((-1, ("Working", tid)))
        (task, argv, kwargs) = Husky.loads(task_info)
        res = task(*argv, **kwargs)
        task_queue.put((tid, Husky.dumps(res)))
        task_queue.put((-1, ("Idle", None)))


class Puppet(object):
    def __init__(self):
        self.results = {}
        self.status = ("Idle", None)

    def receive_info(self):
        while True:
            (info_id, info) = self.task_queue.get()
            if info_id > 0:
                self.results[info_id].set(info)
            elif info_id == -1:
                self.status = info

    def submit_task(self, tid, task_info):
        self.results[tid] = AsyncResult()
        self.task_queue.put((tid, task_info))

    def fetch_result(self, tid, wait=False):
        res = self.results[tid].get()
        del self.results[tid]
        return res

    def hire_worker(self):
        tq_worker, self.task_queue = gipc.pipe(duplex=True)
        self.worker = gipc.start_process(target=process_worker, args=(tq_worker,))
        self.operator = gevent.spawn(self.receive_info)

    def fire_worker(self):
        self.worker.terminate()
        self.operator.kill()

    def get_attr(self, attr):
        return getattr(self, attr, "No such attribute")

    def current_tasks(self):
        return self.results.keys()

if __name__ == '__main__':
    from sockserver import SockServer
    SockServer(Puppet).run()