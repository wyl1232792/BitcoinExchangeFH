
from befh.traders.base_trader import BaseTrader
import json
class SimTrader(BaseTrader):

    def __init__(self, config):
        self.auth_data = {}

        self.set_alias_name(config['name'])

        super().__init__()

    def get_name(self):
        return 'sim'

    def connect(self):
        super().connect()

    def get_auth_data(self):
        return self.auth_data

    def submit_order(self, msg):
        submit_msg = msg.submit
        client = msg.client
        ref = msg.ref

        # should always deal
        for c in self.clients:
            if c.id == client:

                c.account.on_trade(
                    price=msg.submit.price,
                    quantity=msg.submit.quantity,
                    open_or_close=msg.submit.openOrClose,
                    side=msg.submit.side,
                    symbol=msg.submit.symbol
                )
                # c.account.on_order(
                #
                # )

        pass

    def cancel_order(self, msg):
        cancel_msg = msg.cancel
        client = msg.client
        ref = msg.ref

        # always fail

        pass



    def get_balance(self):
        return []

    def get_pnl(self):
        super().get_pnl()

    def get_host_link(self):
        super().get_host_link()

