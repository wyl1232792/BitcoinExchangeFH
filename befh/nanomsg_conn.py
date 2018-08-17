
from befh.util import Logger
import re
import nanomsg
import json
import time

class NanomsgConn:

    def __init__(self, mode):

        mapping = {
            'pub': nanomsg.PUB,
            'sub': nanomsg.SUB,
            'push': nanomsg.PUSH,
            'pull': nanomsg.PULL
        }

        self.conn = nanomsg.Socket(mapping[mode])
        pass

    def connect(self, addr):
        self.conn.connect(addr=addr)

    def bind(self, addr):
        self.conn.bind(addr=addr)

    def send(self, buff):
        self.conn.send(buff)

    def recv(self):
        return self.conn.recv()
        



