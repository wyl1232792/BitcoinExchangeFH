import befh.bitcoin_trade_pb2 as proto
import befh.nanomsg_conn as nnconn
from befh.traders.trade_manager import TraderManager
import json
from befh.logger import get_logger
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
        get_logger().info('Preload %d accounts' % len(traders_preload))
        f.close()

        manager.init_traders(traders_preload)
    except FileNotFoundError:
        get_logger().error('file error', exc_info=True)

    get_logger().info('Start listening')
    # listen msg from nanomsg
    for buffer in conn.recv_iter():
        m.ParseFromString(buffer)

        try:
            manager.handleMsg(m)
        except:
            get_logger().error('error handling msg \n'+\
                    '****begin****\n%s*****end*****' % str(m), exc_info=True)


if __name__ == '__main__':
    main()
