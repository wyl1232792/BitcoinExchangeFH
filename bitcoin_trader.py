import befh.bitcoin_trade_pb2 as proto
import befh.nanomsg_conn as nnconn

# test

m = proto.RequestMsg()

conn = nnconn.NanomsgConn("pull")
conn.bind("ipc:///tmp/")

while True:
    print("listen")
    b = conn.recv()
    m.ParseFromString(b)



