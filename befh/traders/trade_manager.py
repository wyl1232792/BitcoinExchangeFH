import befh.traders.base_trader
from befh.traders.client_context import Client, Account
from befh.logger import get_logger
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
            if (client['api'] not in __traders__.keys()):
                get_logger().error("unknown api %s" % client['api'])
                continue
            self.traders[client['name']] = __traders__[client['api']](client)
        pass

    def new_trader(self, config):
        self.traders[client['name']] = __traders__[client['api']](client)
        return self.traders[client['name']]

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

    def rspAck(self, client, ref, code, msg):
        ack = proto.NotifyMsg()
        ack.client = client
        ack.ref = ref
        ack.status.code = code
        ack.status.msg = msg
        self.pub(ack)


    def handleMsg(self, msg):
        if msg.HasField('register'):
            mapping = msg.register.mappingType

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

                        self.rspAck(msg.client, msg.ref, proto.INVALID_REQUEST, 'name not found')

                        return

                    trader = self.traders[msg.register.name]
                    # create client
                    _account = trader.get_account(self.produce_mapping_config(msg))
                    nc = Client(self, msg.client, _account, msg.ref)
                    self.clients[msg.client] = nc
                    nc.set_trader_provider(SimpleTraderProvider(trader))
                    # bind client to trader
                    trader.add_client(nc)

                # ack success
                self.rspAck(msg.client, msg.ref, proto.SUCCESS, '')

            else:
                get_logger().info('error')

        elif msg.HasField('submit'):
            trader = self.clients[msg.client].get_trader_provider().get()
            trader.submit_order(msg)

        elif msg.HasField('cancel'):
            trader = self.clients[msg.client].get_trader_provider().get()
            trader.cancel_order(msg)

        elif msg.HasField('unregister'):
            pass

    def pub(self, notifyMsg):
        self.pubSink.send(notifyMsg.SerializeToString())


