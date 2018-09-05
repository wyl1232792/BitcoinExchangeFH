
from befh.traders.base_trader import BaseTrader

# this api holds simple restful apis and listen for requests
class RestTrader(BaseTrader):

    def __init__(self):
        BaseTrader.__init__(self)

