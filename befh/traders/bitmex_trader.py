
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
        self.query_pos()
        super().__init__()

    def get_name(self):
        return 'bitmex'

    def connect(self):
        super().connect()
        self.client = bitmex.bitmex(test=self.test, api_key=self.auth_data['api_key'], api_secret=self.auth_data['api_secret'])

    def get_auth_data(self):
        return self.auth_data

    def submit_order(self, msg):
        pass

    def cancel_order(self, msg):
        pass

    def get_balance(self):
        super().get_balance()

    def get_pnl(self):
        super().get_pnl()

    def get_host_link(self):
        super().get_host_link()

    def query_pos(self):
        self.initial_pos = {}
        result = self.client.Position.Position_get().result()
        for o in result:
            self.initial_pos[o['symbol']] = o