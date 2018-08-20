import befh.bitcoin_trade_pb2 as proto
import befh.nanomsg_conn as nnconn
from befh.traders.trade_manager import TraderManager
import json
import sys
# test

def main():
    accounts_path = './accounts.json'

    m = proto.RequestMsg()

    conn = nnconn.NanomsgConn("pull")
    conn.bind("ipc:///tmp/trade_request.ipc")
    conn.start()

    connSink = nnconn.NanomsgConn("pub")
    connSink.bind("ipc:///tmp/trade_feed.ipc")
    connSink.start()

    manager = TraderManager(connSink)
    # manager = TraderManager(None)
    # preload accounts from config file
    try:
        f = open(accounts_path, 'r')
        traders_preload = json.load(f)
        print('Preload %d accounts\n' % len(traders_preload))
        f.close()

        manager.init_traders(traders_preload)
    except FileNotFoundError:
        pass

    print('Start listening')
    # listen msg from nanomsg
    for buffer in conn.recv_iter():
        m.ParseFromString(buffer)
        manager.handleMsg(m)


if __name__ == '__main__':
    main()