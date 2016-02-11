# Brick
scripting and running scientific workflows in Python

test/MoWSC/test.py is a real-world example which I use to run my paper experiments.

Following is a simple example.

# Example

    from time import sleep

    from Brick.engine import LimitEngine
    from Brick.provider import local
    from Brick.workflow import Workflow


    @LimitEngine(local.LXCProvider(), 4)
    def test_mapper(a, n):
        w = Workflow()

        @w.create_task()
        def start(x):
            sleep(10)
            return x + 3

        @w.create_task()
        def middle(x):
            sleep(10)
            return x + 42

        @w.create_task()
        def final(res):
            sleep(10)
            return sum(res)

        y = start(a)
        l = [middle(y) for _ in range(n)]
        return final(l)


    if __name__ == '__main__':
        print "The answer is: %d" % test_mapper(1, 5)
