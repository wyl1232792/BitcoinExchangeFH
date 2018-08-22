import befh.nanomsg_conn as nnconn
import json
import sys
# test

def main():

    connSink = nnconn.NanomsgConn("sub")
    connSink.connect("tcp://127.0.0.1:3303")
    connSink.start()

    print('Start listening')
    # listen msg from nanomsg
    for buffer in connSink.recv_iter():
        print(str(buffer))


if __name__ == '__main__':
    main()