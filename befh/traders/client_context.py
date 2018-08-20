

'''
    to track account and order
'''
class Account:

    def __init__(self):
        pass

    def on_order(self):
        pass

    def on_price(self):
        pass

    def on_trade(self):
        pass



class Client:

    def __init__(self, manager, client_id, ref_id=0):
        self.id = client_id
        self.ref = ref_id
        self.manager = manager

    def is_alive(self):
        return True


