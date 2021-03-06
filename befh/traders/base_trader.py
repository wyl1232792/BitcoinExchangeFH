from befh.traders.client_context import Account, Client
from befh.logger import get_logger

'''
    basic the class should hold all symbols' balances in trader
'''
class BaseTrader():

    def generate_auto_cremental_id(self):
        self.order_id += 1
        return '%08s' % self.order_id

    def __init__(self):
        try:
            if (self.alias_name is None):
                self.alias_name = 'unnamed'
        except:
            self.alias_name = 'unnamed'
        get_logger().info('New trader [api=%s, name=%s]' % (self.get_alias_name(), self.get_name()))
        self.order_id = 0

        self.clients = []
        pass

    def connect(self):
        pass

    def get_auth_data(self):
        pass

    def submit_order(self, msg):
        pass

    def cancel_order(self, msg):
        pass

    def generate_order_ref(self, msg):
        return '%05d%08d' % (msg.client, msg.ref)

    def get_balance(self):
        pass

    def get_pnl(self):
        pass

    def get_host_link(self):
        pass

    def get_name(self):
        return ''

    def get_alias_name(self):
        return self.alias_name

    def set_alias_name(self, name):
        self.alias_name = name

    def get_account(self, config):
        return Account()

    def add_client(self, client):
        self.clients.append(client)
