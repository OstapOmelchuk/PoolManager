import psycopg2
import logging
import time
from threading import RLock
from contextlib import contextmanager
from db_settings import database as db


class DBPool:
    def __init__(self, pool_max_size, ttl, host, port, user, password, db_name):
        self._connection_pool = []
        self.connection_pointer = 0
        self._pool_max_size = pool_max_size
        self.connection_ttl = ttl
        self.log = logging.getLogger('dbpool')
        self.lock = RLock()

        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db_name = db_name

    def _create_connection(self):
        con = psycopg2.connect(host=self._host,
                               port=self._port,
                               user=self._user,
                               password=self._password,
                               database=self._db_name)
        self.log.info(f"The connection object {con} has been created.")
        return {"connection": con,
                "creation_date": time.time()}

    def _get_connection(self):
        connection = None

        while not connection:
            if self._connection_pool:
                connection = self._connection_pool.pop()
            elif self.connection_pointer < self._pool_max_size:
                connection = self._create_connection()
                self.connection_pointer += 1
            time.sleep(0.05)
        return connection

    def _push_connection(self, connection):
        self.log.info(f"The connection {connection} was sent to the pool.")
        self._connection_pool.append(connection)

    def _close_connection(self, connection):
        self.log.info(f"The connection {connection} was closed.")
        connection["connection"].close()
        self.connection_pointer -= 1

    @contextmanager
    def manager(self):
        with self.lock:
            connection = self._get_connection()

        try:
            yield connection["connection"]
        except Exception as e:
            self._close_connection(connection)

        if connection["creation_date"] + self.connection_ttl < time.time():
            self._push_connection(connection)
        else:
            self._close_connection(connection)


db_pool = DBPool(pool_max_size=10, ttl=1, **db)
