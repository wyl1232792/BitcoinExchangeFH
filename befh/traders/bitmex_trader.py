

from befh.traders.ws_trader import WebsocketTrader

class BitmexTrader(WebsocketTrader):


    def get_name(self):
        return 'bitmex'