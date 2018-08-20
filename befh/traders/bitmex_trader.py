
from befh.traders.base_trader import BaseTrader
import bitmex
import json
class BitmexTrader(BaseTrader):

    def __init__(self, config):
        self.auth_data = {}
        self.set_alias_name(config['name'])
        self.test = config['test']
        self.auth_data['api_key'] = config['api_key']
        self.auth_data['api_secret'] = config['api_secret']
        self.default_instr = config['default_instr'] if 'default_instr' in config.keys() else None
        self.connect()
        super().__init__()

    def get_name(self):
        return 'bitmex'

    def connect(self):
        super().connect()
        self.client = bitmex.bitmex(test=self.test, api_key=self.auth_data['api_key'], api_secret=self.auth_data['api_secret'])
        print(self.client.Position.Position_get().result())

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

