

class BaseTrader():


    def __init__(self):
        self.alias_name = 'unnamed'
        pass

    def connect(self):
        pass

    def get_auth_data(self):
        pass

    def submit_order(self):
        pass

    def cancel_order(self):
        pass

    def get_balance(self):
        pass

    def get_pnl(self):
        pass

    def get_host_link(self):
        pass

    def get_name(self):
        return ''

    def get_alias_name(self):
        return self.get_alias_name()

    def set_alias_name(self, name):
        self.alias_name = name