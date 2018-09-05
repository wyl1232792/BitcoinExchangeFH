import threading
import re
from nanomsg import Socket, PUB, PUSH, PULL
import json
import time

addr = "ipc:///tmp/haha"
conn = Socket(PUSH)

conn.connect(addr)
for counter in range(0, 10000):
    conn.send(str(counter))
time.sleep(1)
conn.close()
