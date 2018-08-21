

'''
    to track account and order
'''
class Account:

    def __init__(self):

        pass

    def on_order(self, order_id, state, quantity, side, open_or_close, symbol):
        pass

    def on_price(self, price, symbol):
        pass

    def on_trade(self, price, quantity, side, open_or_close, symbol):
        pass


class Client:

    def __init__(self, manager, client_id, account, ref_id=0):
        self.id = client_id
        self.ref = ref_id
        self.manager = manager
        self.account = account
        self.trader_provider = None

    def is_alive(self):
        return True

    def get_trader_provider(self):
        return self.trader_provider

    def set_trader_provider(self, data):
        self.trader_provider = data
