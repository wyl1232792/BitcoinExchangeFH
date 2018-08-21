import befh.traders.base_trader
from befh.traders.client_context import Client, Account
import befh.bitcoin_trade_pb2 as proto
import json
from befh.traders.bitmex_trader import BitmexTrader
from befh.traders.sim_trader import SimTrader

__traders__ = {
    'sim': SimTrader,
    'bitmex': BitmexTrader
}

class SimpleTraderProvider:
    def __init__(self, t):
        self.trader = t
    def get(self):
        return self.trader

class TraderManager:

    def __init__(self, pubSink):
        self.clients = {}
        self.traders = {}
        self.pubSink = pubSink

    def init_traders(self, config):
        for client in config:
            self.traders[client['name']] = __traders__[client['api']](client)
        pass

    def new_trader(self, config):
        pass

    def resume(self):
        pass

    def pause(self):
        pass

    @classmethod
    def produce_mapping_config(cls, msg):
        return {
            'name': msg.register.name,
            'mapping': msg.register.mappingType
        }

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
                    # get trader
                    if (msg.register.name not in self.traders.keys()):
                        # not found
                        ack = proto.NotifyMsg()
                        ack.client = msg.client
                        ack.ref = msg.ref
                        ack.status.code = proto.INVALID_REQUEST
                        ack.status.msg = 'name not found'
                        self.pub(ack)
                        return

                    trader = self.traders[msg.register.name]
                    # create client
                    _account = trader.get_account(self.produce_mapping_config(msg))
                    nc = Client(self, msg.client, _account, msg.ref)
                    self.clients[msg.ref] = nc
                    nc.set_trader_provider(SimpleTraderProvider(trader))
                    # bind client to trader
                    trader.add_client(nc)

                # ack success
                ack = proto.NotifyMsg()
                ack.client = msg.client
                ack.ref = msg.ref
                ack.status.code = proto.SUCCESS
                ack.status.msg = ''

                self.pub(ack)

            else:
                print('error')

        elif msg.HasField('submit'):
            print('s')

        elif msg.HasField('cancel'):
            print('c')

        elif msg.HasField('unregister'):
            print('u')


    def pub(self, notifyMsg):
        self.pubSink.send(notifyMsg.SerializeToString())


