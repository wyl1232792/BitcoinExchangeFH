from befh.clients.database import DatabaseClient
from befh.util import Logger
import threading
import re
import zmq
from nanomsg import Socket, PUB
import json
import time
import os
import datetime

# TODO write a simple log with raw data into rolled file
class FileWriter:

    def __init__(self, prefix, max_size):
        self.id = 0
        self.prefix = prefix
        self.max_size = max_size
        self.date = self.get_date()
        fn = self.get_filename()
        self.size = self.get_size(fn)
        self.fd = open(fn, 'a')

    def write(self, buff):
        if (self.size > self.max_size):
            self.roll()
        self.fd.write(buff + '\n')
        print(self.fd)
        self.size += len(buff)+ 1

    def roll(self):
        self.fd.close()
        if self.get_date() != self.date:
            self.date = self.get_date()
            self.id = 0
        self.fd = open(self.get_filename(), 'a')
        self.size = 0

    def get_filename(self):
        while True:
            f = self.produce_filename(self.id)
            if (self.get_size(f) < self.max_size):
                return f
            self.id += 1

    def get_size(self, name):
        return os.path.getsize(name) if os.path.exists(name) else 0

    def get_date(self):
        d = datetime.date.today()
        return '%04d%02d%02d' % (d.year, d.month, d.day)

    def produce_filename(self, id):
        return '%s.%s.%04d.log' % (self.prefix, self.date, id)


class NanomsgClient(DatabaseClient):
    """
    Zmq Client
    """
    def __init__(self):
        """
        Constructor
        """
        DatabaseClient.__init__(self)
        self.conn = Socket(PUB)
        self.lock = threading.Lock()
        self.fw = FileWriter('nano_log', 1073741824)

    def connect(self, **kwargs):
        """
        Connect
        :param path: sqlite file to connect
        """
        addr = kwargs['addr']
        Logger.info(self.__class__.__name__, 'Nanomsg client is connecting to %s' % addr)
        self.conn.bind(addr)
        return self.conn is not None


    def execute(self, sql):
        """
        Execute the sql command
        :param sql: SQL command
        """
        return True

    def commit(self):
        """
        Commit
        """
        return True

    def fetchone(self):
        """
        Fetch one record
        :return Record
        """
        return []

    def fetchall(self):
        """
        Fetch all records
        :return Record
        """
        return []

    def create(self, table, columns, types, primary_key_index=(), is_ifnotexists=True):
        """
        Create table in the database.
        Caveat - Assign the first few column as the keys!!!
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        return True

    def insert(self, table, columns, types, values, primary_key_index=(), is_orreplace=False, is_commit=True):
        """
        Insert into the table
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param values: Value array
        :param primary_key_index: An array of indices of primary keys in columns,
                          e.g. [0] means the first column is the primary key
        :param is_orreplace: Indicate if the query is "INSERT OR REPLACE"
        """
        ret = dict(zip(columns, values))
        ret['table'] = table
        self.lock.acquire()
        s = json.dumps(ret)
        self.conn.send(s)
        self.fw.write(s)

        self.lock.release()
        return True

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
        """
        Select rows from the table
        :param table: Table name
        :param columns: Selected columns
        :param condition: Where condition
        :param orderby: Order by condition
        :param limit: Rows limit
        :param isFetchAll: Indicator of fetching all
        :return Result rows
        """
        return []

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        return True

if __name__ == '__main__':
    Logger.init_log()
    db_client = NanomsgClient()
    db_client.connect(addr='tcp://127.0.0.1:3334')
    for i in range(1, 100):
        db_client.insert('test', ['c1', 'c2', 'c3', 'c4'], [], ['abc', i, 1.1, 5])
        time.sleep(1)

