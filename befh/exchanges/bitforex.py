from befh.ws_api_socket import WebSocketApiClient
from befh.market_data import L2Depth, Trade
from befh.exchanges.gateway import ExchangeGateway
from befh.instrument import Instrument
from befh.util import Logger
from befh.clients.sql_template import SqlClientTemplate
import time
import threading
import json
from functools import partial
from datetime import datetime


class ExchBitforex(WebSocketApiClient):
    """
    Exchange socket
    """
    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchGwBitforex')

    @classmethod
    def get_order_book_timestamp_field_name(cls):
        return 'timestamp'

    @classmethod
    def get_trades_timestamp_field_name(cls):
        return 'timestamp'

    @classmethod
    def get_bids_field_name(cls):
        return 'bids'

    @classmethod
    def get_asks_field_name(cls):
        return 'asks'

    @classmethod
    def get_trade_side_field_name(cls):
        return 'side'

    @classmethod
    def get_trade_id_field_name(cls):
        return 'trdMatchID'

    @classmethod
    def get_trade_price_field_name(cls):
        return 'price'

    @classmethod
    def get_trade_volume_field_name(cls):
        return 'size'

    @classmethod
    def get_link(cls):
        return 'wss://ws.bitforex.com/mkapi/coinGroup1/ws'

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return '[{"type": "subHq", "event": "depth10", "param": {"businessType": "%s", "dType":0, "size": 100}}]' % instmt.get_instmt_code()

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return '[{"type": "subHq", "event": "trade", "param": {"businessType": "%s", "dType":0, "size": 100}}]' % instmt.get_instmt_code()

    def get_ping_msg(self):
        return 'ping_p'

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = instmt.get_l2_depth()
        keys = list(raw.keys())

        # if raw['type'].startswith('ticker'):
        #     pass
        if 'data' in keys:
            l2_depth.date_time = datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
            data = raw['data']
            depth = 5
            bids = data['bids']
            asks = data['asks']
            bids_len = min(len(bids), depth)
            asks_len = min(len(asks), depth)
            for i in range(bids_len):
                l2_depth.bids[i].price = float(bids[i]['price'])
                l2_depth.bids[i].volume = float(bids[i]['amount'])
            for i in range(asks_len):
                l2_depth.asks[i].price = float(asks[i]['price'])
                l2_depth.asks[i].volume = float(asks[i]['amount'])

        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        trade = Trade()
        keys = list(raw.keys())

        if 'price' in keys and \
           'amount' in keys and \
           'time' in keys and \
           'direction' in keys and \
           'tid' in keys:

            # Date time
            trade.date_time = datetime.utcfromtimestamp(raw['time'] / 1000).strftime("%Y%m%d %H:%M:%S.%f")

            # Trade side
            trade.trade_side = Trade.Side.NONE

            # Trade id
            trade.trade_id = raw['tid']

            # Trade price
            trade.trade_price = raw['price']

            # Trade volume
            trade.trade_volume = raw['amount']
        else:
            raise Exception('Does not contain trade keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))

        return trade


class ExchGwBitforex(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchBitforex(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Bitforex'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        if not instmt.get_subscribed():
            ws.send(self.api_socket.get_order_book_subscription_string(instmt))
            ws.send(self.api_socket.get_trades_subscription_string(instmt))
            instmt.set_subscribed(True)

    def on_close_handler(self, instmt, ws):
        """
        Socket on close handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is unsubscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        instmt.set_subscribed(False)

    def on_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        keys = message.keys()
        if 'event' in keys:
            if message['param']['businessType'] == instmt.get_instmt_code():
                if message['event'] == 'depth10':
                    instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                    l2_depth = self.api_socket.parse_l2_depth(instmt, message)
                    if l2_depth is not None and l2_depth.is_diff(instmt.get_prev_l2_depth()):
                        instmt.set_l2_depth(l2_depth)
                        instmt.incr_order_book_id()
                        self.insert_order_book(instmt)
                elif message['event'] == 'trade':
                    for trade_raw in message['data']:
                        trade = self.api_socket.parse_trade(instmt, trade_raw)
                        if trade.trade_id != instmt.get_exch_trade_id():
                            instmt.incr_trade_id()
                            instmt.set_exch_trade_id(trade.trade_id)
                            self.insert_trade(instmt, trade)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(5))
        instmt.set_prev_l2_depth(L2Depth(5))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        return [self.api_socket.connect(self.api_socket.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]

if __name__ == '__main__':
    exchange_name = 'Bitforex'
    instmt_name = 'btc-usdt'
    instmt_code = 'coin-usdt-btc'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    Logger.init_log()
    exch = ExchGwBitforex([db_client])
    td = exch.start(instmt)
