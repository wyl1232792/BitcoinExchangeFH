import befh.bitcoin_trade_pb2 as proto
import befh.nanomsg_conn as nnconn
from befh.traders.trade_manager import TraderManager

# test

def main():
    m = proto.RequestMsg()

    conn = nnconn.NanomsgConn("pull")
    conn.bind("ipc:///tmp/trade_request.ipc")
    conn.start()

    connSink = nnconn.NanomsgConn("pub")
    connSink.bind("ipc:///tmp/trade_feed.ipc")
    connSink.start()

    manager = TraderManager(connSink)
    # preload accounts from config file

    print('Start listening')
    # listen msg from nanomsg
    for buffer in conn.recv_iter():
        m.ParseFromString(buffer)
        manager.handleMsg(m)


if __name__ == '__main__':
    main()