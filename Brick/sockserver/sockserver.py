import gevent
from functools import partial
from gevent import socket

from port import ObjPort


class SockServer(object):
    def __init__(self, C, *args):
        self.instance = C(*args)

    def run(self, port=0, pipe=None):
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.bind(("", port))
        listen_sock.listen(10000)
        if pipe:
            pipe.put(listen_sock.getsockname()[1])
        else:
            print listen_sock.getsockname()[1]
        while True:
            sock, _ = listen_sock.accept()
            gevent.spawn(self.handle_let, sock)

    def handle_let(self, sock):
        port = ObjPort(sock)
        while True:
            message = port.read()
            if message:
                port.write(self.handle(message))
            else:
                break

    def handle(self, message):
        func, args = message
        f = getattr(self.instance, func, lambda _: None)
        return f(*args)


class SockClient(object):
    def __init__(self, worker_addr, keep_alive=True):
        self.keep_alive = keep_alive
        self.worker_addr = worker_addr
        if self.keep_alive:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect(worker_addr)
            self.port = ObjPort(sock)
        else:
            self.port = None

    def shutdown(self):
        if self.port:
            self.port.close()

    def __getattr__(self, func):
        if not self.port:
            return partial(remote_call, self.worker_addr, func)

        def call(*args):
            st = self.port.write((func, args))
            if st:
                msg = self.port.read()
                if msg:
                    return msg
        return call


def remote_call(addr, func, *args):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(addr)
    port = ObjPort(sock)
    port.write((func, args))
    return port.read()
