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
import pytz
import re
from tzlocal import get_localzone


class ExchGwApiBinanceWs(WebSocketApiClient):
    """
    Exchange Socket
    """

    Client_Id = int(time.mktime(datetime.now().timetuple()))

    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchApiBinanceWs')

    @classmethod
    def get_bids_field_name(cls):
        return 'bids'

    @classmethod
    def get_asks_field_name(cls):
        return 'asks'

    @classmethod
    def get_link(cls, instmt=None):
        if instmt is None:
            return 'wss://stream.binance.com:9443/stream?streams='
        else:
            return ['wss://stream.binance.com:9443/stream?streams=', instmt.instmt_code, '/']

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return '%s@depth5' % instmt.instmt_code

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return '%s@trade' % instmt.instmt_code

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        {
            "type": "ticker.btcusdt",
            "seq": 680035,
            "ticker": [
              7140.890000000000000000,
              1.000000000000000000,
              7131.330000000,
              233.524600000,
              7140.890000000,
              225.495049866,
              7140.890000000,
              7140.890000000,
              7140.890000000,
              1.000000000,
              7140.890000000000000000
            ]
        }
        depth
        {
          "type": "depth.L20.ethbtc",
          "ts": 1523619211000,
          "seq": 120,
          "bids": [0.000100000, 1.000000000, 0.000010000, 1.000000000],
          "asks": [1.000000000, 1.000000000]
        }
        """
        l2_depth = instmt.get_l2_depth()
        keys = list(raw.keys())

        # if raw['type'].startswith('ticker'):
        #     pass
        if raw['type'].startswith('depth'):
            timestamp = raw['ts']
            l2_depth.date_time = datetime.utcfromtimestamp(timestamp / 1000.0).strftime("%Y%m%d %H:%M:%S.%f")
            depth = 5
            bids = raw['bids']
            asks = raw['asks']
            bids_len = min(len(bids) / 2, depth)
            asks_len = min(len(asks) / 2, depth)
            for i in range(bids_len):
                l2_depth.bids[i].price = float(bids[i * 2])
                l2_depth.bids[i].volume = float(bids[i * 2 + 1])
            for i in range(asks_len):
                l2_depth.asks[i].price = float(asks[i * 2])
                l2_depth.asks[i].volume = float(asks[i * 2 + 1])

        return l2_depth


    @classmethod
    def parse_trade(cls, instmt, raws):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        trade
        {
          "type":"trade.ethbtc",
          "id":76000,
          "amount":1.000000000,
          "ts":1523419946174,
          "side":"sell",
          "price":4.000000000
        }
        """
        trades = []

        trade = Trade()
        trade.date_time = datetime.utcfromtimestamp(raws['ts'] / 1000.0).strftime("%Y%m%d %H:%M:%S.%f")
        # Trade side
        # Buy = 0
        # Side = 1
        trade.trade_side = Trade.Side.BUY if raws['side'] == 'buy' else Trade.Side.SELL
        # Trade id
        trade.trade_id = str(raws['id'])
        # Trade price
        trade.trade_price = raws['price']
        # Trade volume
        trade.trade_volume = raws['amount']
        trades.append(trade)

        # for item in raws:
        #     trade = Trade()
        #     today = datetime.today().date()
        #     time = item[3]
        #     #trade.date_time = datetime.utcfromtimestamp(date_time/1000.0).strftime("%Y%m%d %H:%M:%S.%f")
        #     #Convert local time as to UTC.
        #     date_time = datetime(today.year, today.month, today.day,
        #                          *list(map(lambda x: int(x), time.split(':'))),
        #                          tzinfo = get_localzone()
        #     )
        #     trade.date_time = date_time.astimezone(pytz.utc).strftime('%Y%m%d %H:%M:%S.%f')
        #     # Trade side
        #     # Buy = 0
        #     # Side = 1
        #     trade.trade_side = Trade.parse_side(item[4])
        #     # Trade id
        #     trade.trade_id = str(item[0])
        #     # Trade price
        #     trade.trade_price = item[1]
        #     # Trade volume
        #     trade.trade_volume = item[2]
        #     trades.append(trade)
        return trades


class ExchGwBinanceWs(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiBinanceWs(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'BinanceWs'

    def notify_all_added(self):
        print('notify_all_added')
        self.api_socket.connect_collect()

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Not sending data: Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_name(), instmt.get_exchange_name()))
        # if not instmt.get_subscribed():
        #     Logger.info(self.__class__.__name__, 'order book string:{}'.format(self.api_socket.get_order_book_subscription_string(instmt)))
        #     Logger.info(self.__class__.__name__, 'trade string:{}'.format(self.api_socket.get_trades_subscription_string(instmt)))
        #     ws.send(self.api_socket.get_order_book_subscription_string(instmt))
        #     ws.send(self.api_socket.get_trades_subscription_string(instmt))
        #     instmt.set_subscribed(True)

    def on_close_handler(self, instmt, ws):
        """
        Socket on close handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is unsubscribed in channel %s" % \
                  (instmt.get_instmt_name(), instmt.get_exchange_name()))
        instmt.set_subscribed(False)

    def on_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        {
            "type": "ticker.btcusdt",
            "seq": 680035,
            "ticker": [
              7140.890000000000000000,
              1.000000000000000000,
              7131.330000000,
              233.524600000,
              7140.890000000,
              225.495049866,
              7140.890000000,
              7140.890000000,
              7140.890000000,
              1.000000000,
              7140.890000000000000000
            ]
        }
        depth
        {
          "type": "depth.L20.ethbtc",
          "ts": 1523619211000,
          "seq": 120,
          "bids": [0.000100000, 1.000000000, 0.000010000, 1.000000000],
          "asks": [1.000000000, 1.000000000]
        }
        trade
        {
          "type":"trade.ethbtc",
          "id":76000,
          "amount":1.000000000,
          "ts":1523419946174,
          "side":"sell",
          "price":4.000000000
        }
        """
        if 'type' in message:
            # if message['type'] == "ticker.%s" % instmt.instmt_code:
            #     instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
            #     self.api_socket.parse_l2_depth(instmt, message)
            #     if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
            #         instmt.incr_order_book_id()
            #         self.insert_order_book(instmt)
            if message['type'] == "depth.L20.%s" % instmt.instmt_code:
                instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                self.api_socket.parse_l2_depth(instmt, message)
                if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)
            if message['type'] == "trade.%s" % instmt.instmt_code:
                trades = self.api_socket.parse_trade(instmt, message)
                for trade in trades:
                    if trade.trade_id != instmt.get_exch_trade_id():
                        instmt.incr_trade_id()
                        instmt.set_exch_trade_id(trade.trade_id)
                        self.insert_trade(instmt, trade)
        # for item in message:
        #     if 'channel' in item:
        #         if re.search(r'ok_sub_futureusd_(.*)_depth_(.*)', item['channel']):
        #             instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
        #             self.api_socket.parse_l2_depth(instmt, item['data'])
        #             if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
        #                 instmt.incr_order_book_id()
        #                 self.insert_order_book(instmt)
        #         elif re.search(r'ok_sub_futureusd_(.*)_trade_(.*)', item['channel']):
        #             trades = self.api_socket.parse_trade(instmt, item['data'])
        #             for trade in trades:
        #                 if trade.trade_id != instmt.get_exch_trade_id():
        #                     instmt.incr_trade_id()
        #                     instmt.set_exch_trade_id(trade.trade_id)
        #                     self.insert_trade(instmt, trade)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(20))
        instmt.set_prev_l2_depth(L2Depth(20))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        Logger.info(self.__class__.__name__, 'instmt snapshot table: {}'.format(instmt.get_instmt_snapshot_table_name()))
        return [self.api_socket.connect(self.api_socket.get_link(instmt),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]



if __name__ == '__main__':
    import logging
    import websocket
    websocket.enableTrace(True)
    logging.basicConfig()
    Logger.init_log()
    exchange_name = 'BinanceWs'
    instmt_name = 'BTCUSDT'
    instmt_code = 'btcusdt'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    exch = ExchGwBinanceWs([db_client])
    td = exch.start(instmt)
    pass
