
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

    def submit_order(self):
        super().submit_order()

    def cancel_order(self):
        super().cancel_order()

    def get_balance(self):
        super().get_balance()

    def get_pnl(self):
        super().get_pnl()

    def get_host_link(self):
        super().get_host_link()

