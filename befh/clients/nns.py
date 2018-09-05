import threading
import re
from nanomsg import Socket, PUB, PUSH, PULL
import json
import time

addr = "ipc:///tmp/haha"
conn = Socket(PULL)

conn.bind(addr)

while True:
    print(conn.recv())

