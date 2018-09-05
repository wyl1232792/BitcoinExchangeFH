import json
from befh.traders.base_trader import BaseTrader
from befh.traders.rest_trader import RestTrader
import websocket
import threading
import time

from befh.logger import get_logger

class WsListener:
    def on_open(self):
        pass
    def on_close(self):
        pass
    def on_message(self, msg):
        pass
    def on_error(self, error):
        pass

class WebsocketTrader(RestTrader):

    def __init__(self):
        RestTrader.__init__(self)
        self.end_point = ''
        self.auth = None
        self.ping_msg = None
        self.ws = None              # Web socket
        self.wst = None             # Web socket thread
        self._connecting = False
        self._connected = False
        self.listener = None
        self.ping_thread = None

    def set_listener(self, listener):
        self.listener = listener

    def set_ws_url(self, end_point):
        self.end_point = end_point

    def set_auth(self, auth):
        self.auth = auth

    def get_ping_msg(self):
        return None

    def send_ws(self, msg):
        """
        Send message
        :param msg: Message
        :return:
        """
        self.ws.send(msg)


    def send_interval_ping_msg(self):
        while True:
            time.sleep(10)
            if self._connected:
                self.ws.send(self.get_ping_msg())

    def __on_message(self, ws, m):
        if (self.listener is not None):
            self.listener.on_message(m)


    def __on_open(self, ws):
        self._connected = True

        if self.get_ping_msg() is not None:
            self.ping_thread = threading.Thread(target=self.send_interval_ping_msg)
            self.ping_thread.start()

        if (self.listener is not None):
            self.listener.on_open()


    def __on_close(self, ws):
        self._connecting = False
        self._connected = False
        if (self.listener is not None):
            self.listener.on_close()

    def __on_error(self, ws, error):
        if (self.listener is not None):
            self.listener.on_error(error)
        print(error)

    def connect(self,
                reconnect_interval=1):

        if not self._connecting and not self._connected:
            if self.end_point is not None:
                self._connecting = True
                self.ws = websocket.WebSocketApp(self.end_point,
                                                 on_message=self.__on_message,
                                                 on_close=self.__on_close,
                                                 on_open=self.__on_open,
                                                 on_error=self.__on_error)
                self.wst = threading.Thread(target=lambda: self.__start(reconnect_interval=reconnect_interval))
                self.wst.start()

        return self.wst

    def __start(self, reconnect_interval=10):

        while True:
            self.ws.run_forever()
            get_logger().info("Socket <%s> is going to reconnect...", self.get_alias_name())
            time.sleep(reconnect_interval)

if __name__ == '__main__':
    s = WebsocketTrader()
    class haha(WsListener):
        def on_open(self):
            s.send_ws(msg)
        def on_message(self, m):
            print('haha')
            print(m)

    ws = haha()
    s.set_listener(ws)
    msg = {'event':'addChannel','channel':'ok_sub_futureusd_btc_ticker_this_week'}
    msg = json.dumps(msg)

    s.set_ws_url('wss://real.okex.com:10440/websocket/okexapi')
    s.connect()
