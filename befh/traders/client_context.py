

import befh.bitcoin_trade_pb2 as proto
'''
    to track account and order
    Account class should be overridden to support each Trader
'''
class Account:


    def __init__(self):
        self.pos = [0, 0]
        self.cash_flow = 0
        self.price = 0

    def on_order(self, order_id, order_ref, state, fill, quantity, side, open_or_close, symbol, price):
        pass

    def on_price(self, price, symbol):
        pass

    def on_trade(self, price, quantity, side, open_or_close, symbol):
        pass

    def set_client(self, client):
        self.client = client
        self.sender = self.client.manager

    def notify_update(self):
        msg = proto.NotifyMsg()
        msg.client = self.client.id
        msg.ref = -100
        msg.storageUpdate.longPos = self.pos[0]
        msg.storageUpdate.shortPos = self.pos[1]
        msg.storageUpdate.GrossPnl = self.cash_flow
        self.sender.pub(msg)


class Client:

    def __init__(self, manager, client_id, account, ref_id=0):
        self.id = client_id
        self.ref = ref_id
        self.manager = manager
        self.account = account
        self.account.set_client(self)
        self.trader_provider = None

    def is_alive(self):
        return True

    def get_trader_provider(self):
        return self.trader_provider

    def set_trader_provider(self, data):
        self.trader_provider = data
