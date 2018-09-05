
from befh.traders.base_trader import BaseTrader
import json
from befh.traders.client_context import Account, Client
from befh.logger import get_logger
import befh.bitcoin_trade_pb2 as proto

class SimAccount(Account):

    def __init__(self):
        Account.__init__(self)

    def on_order(self, order_id, order_ref, state, fill, quantity, side, open_or_close, symbol, price):
        msg = proto.NotifyMsg()
        msg.client = self.client.id
        msg.ref = -100

        msg.orderChange.code = state
        msg.orderChange.order.symbol = symbol
        msg.orderChange.order.side = side
        msg.orderChange.order.orderType = proto.LIMIT
        msg.orderChange.order.fill = fill
        msg.orderChange.order.quantity = quantity
        msg.orderChange.order.openOrClose = open_or_close
        msg.orderChange.order.orderRefId = order_ref
        msg.orderChange.order.orderSysId = order_id
        msg.orderChange.order.exch = "sim"
        msg.orderChange.order.price = price
        self.sender.pub(msg)

    def on_price(self, price, symbol):

        self.notify_update()

    def on_trade(self, price, quantity, side, open_or_close, symbol):
        get_logger().info('on trade')
        # update self account cache
        # check trade is valid

        # update localstorage
        #  - update pos
        base_q = quantity if open_or_close==proto.OPEN else -quantity
        pos_ind = 0 if side == proto.LONG else 1
        self.pos[pos_ind] += base_q
        #  - update cashflow
        self.cash_flow += (-1 if pos_ind==0 else 1) * quantity * price
        # update c++ cache
        self.notify_update()





class SimTrader(BaseTrader):

    def __init__(self, config):
        self.auth_data = {}

        self.set_alias_name(config['name'])

        super().__init__()

    def get_name(self):
        return 'sim'

    def connect(self):
        super().connect()

    def get_auth_data(self):
        return self.auth_data

    def submit_order(self, msg):
        submit_msg = msg.submit
        client = msg.client
        ref = msg.ref

        # should always deal
        for c in self.clients:
            if c.id == client:
                refId = self.generate_order_ref(msg)
                # create order
                c.account.on_order(
                    refId,
                    refId,
                    proto.CREATED,
                    .0,
                    submit_msg.quantity,
                    submit_msg.side,
                    submit_msg.openOrClose,
                    submit_msg.symbol,
                    submit_msg.price
                )
                # set open order dead
                c.account.on_order(
                    refId,
                    refId,
                    proto.FILLED,
                    submit_msg.quantity,
                    submit_msg.quantity,
                    submit_msg.side,
                    submit_msg.openOrClose,
                    submit_msg.symbol,
                    submit_msg.price
                )
                # notify trade msg
                c.account.on_trade(
                    price=msg.submit.price,
                    quantity=msg.submit.quantity,
                    open_or_close=msg.submit.openOrClose,
                    side=msg.submit.side,
                    symbol=msg.submit.symbol
                )
                # normal ack for proto SUCCESS with refId
                c.manager.rspAck(c.id, msg.ref, proto.SUCCESS, refId)


    def cancel_order(self, msg):
        cancel_msg = msg.cancel
        client = msg.client
        ref = msg.ref

        # always fail
        for c in self.clients:
            if (c.id == client):
                c.manager.rspAck(c.id, msg.ref, proto.SUCCESS, 'cancel ack')

    def get_balance(self):
        return []

    def get_pnl(self):
        super().get_pnl()

    def get_host_link(self):
        super().get_host_link()

    def get_account(self, config):
        return SimAccount()

