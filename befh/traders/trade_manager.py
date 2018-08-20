import befh.traders.base_trader
from befh.traders.client_context import Client, Account
import befh.bitcoin_trade_pb2 as proto

class TraderManager:

    def __init__(self, pubSink):
        self.clients = {}
        self.traders = []
        self.pubSink = pubSink

    def init_traders(self, config):
        pass

    def new_trader(self, config):
        pass

    def resume(self):
        pass

    def pause(self):
        pass

    def handleMsg(self, msg):
        if msg.HasField('register'):
            mapping = msg.register.mappingType
            print('register')
            print('ref=%d' % msg.ref)
            print('client=%d' % msg.client)
            print('name=%s' % msg.register.name)
            print('mappingType=' + mapping)

            # register client and bind trader
            # simple means fetch or create registered account
            if mapping == 'simple':
                if msg.client in self.clients.keys():
                    # exists client
                    refresh = False
                    if not refresh:
                        # do nothing but send msg
                        pass
                else:
                    # create client
                    nc = Client(self, msg.client, msg.ref)

                    # bind trader

                # ack success
                ack = proto.NotifyMsg()
                ack.client = msg.client
                ack.ref = msg.ref
                ack.status = proto.Status()
                ack.status.code = proto.SUCCESS

                self.pub(ack)

        elif msg.HasField('submit'):

            print('s')

        elif msg.HasField('cancel'):
            print('c')

        elif msg.HasField('unregister'):
            print('u')


    def pub(self, notifyMsg):
        self.pubSink.send(notifyMsg.SerializeToString())


