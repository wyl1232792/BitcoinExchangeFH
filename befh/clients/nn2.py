import threading
import re
from nanomsg import Socket, PUB, PUSH, PULL
import json
import time

addr = "ipc:///tmp/haha"
conn = Socket(PUSH)

conn.connect(addr)

while True:
    conn.send('xx1')
    time.sleep(1)

