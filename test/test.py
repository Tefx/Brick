import time

from Brick.engine import *
from Brick.provider import local as LP
from Brick.workflow import Workflow


def test_diamond():
    w = Workflow()

    @w.create_task()
    def start_task(x):
        time.sleep(10)
        return x + 3

    @w.create_task()
    def mid_task(x):
        time.sleep(10)
        return x + 42

    @w.create_task()
    def final(l):
        time.sleep(10)
        return sum(l)

    y = start_task(1)
    res = final([mid_task(y) for _ in range(10)])
    w.save("dag.dot")

    # p = LP.ProcessProvider()
    p = LP.LXCProvider()
    # p = QingProvider(api_keypath="access_key.csv",
    #                  zone="pek2",
    #                  image="img-x18zen9y",
    #                  keypair="kp-p2h7c1sp",
    #                  vxnets="vxnet-0domhwj")
    e = LimitEngine(p, 4)

    st = time.time()
    e.start_with_server(w)
    ft = time.time()

    w.dump_time("exectime.json")

    print "Result:", res.value
    print "Cost:", p.total_cost()
    print "Makespan:", ft - st


def test_meta_merge():
    w =  Workflow()

    @w.create_task(lambda x: x*2)
    def start(x):
        return x+42

    @w.create_task()
    def mid(x):
        return x*2

    @w.create_task()
    def final(l):
        return sum(l)

    a = start(3)
    b = [mid(a) for _ in range(a.metadata)]
    c = final(b)

    w.save("dag.dot")
    p = LP.ProcessProvider()
    e = LimitEngine(p, 1)
    e.start(w)

    e.join()
    print c.value

if __name__ == '__main__':
    test_diamond()
