
from befh.util import Logger
import re
import nanomsg
import json
import time
import threading

class NanomsgConn:

    def __init__(self, mode):

        mapping = {
            'pub': nanomsg.PUB,
            'sub': nanomsg.SUB,
            'push': nanomsg.PUSH,
            'pull': nanomsg.PULL
        }

        self.conn = nanomsg.Socket(mapping[mode])
        self.run_flag = False
        pass

    def connect(self, addr):
        self.conn.connect(addr=addr)

    def bind(self, addr):
        self.conn.bind(addr)

    def send(self, buff):
        self.conn.send(buff)

    def recv(self):
        return self.conn.recv()

    def start(self):
        self.run_flag = True

    def stop(self):
        self.run_flag = False

    def recv_iter(self):
        while self.run_flag:
            yield self.conn.recv()

        



