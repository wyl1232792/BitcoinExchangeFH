import befh.traders.base_trader


class TraderManager:

    def init(self):
        pass

    def resume(self):
        pass

    def pause(self):
        pass

    def handleMsg(self, msg):
        if msg.HasField('register'):
            print('r')
        if msg.HasField('submit'):
            print('s')
        if msg.HasField('cancel'):
            print('c')
        if msg.HasField('unregister'):
            print('u')

