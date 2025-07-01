import mysql.connector

from schemachange.config.utils import get_not_none_key_value
from schemachange.session.base import BaseSession

class MySQLSession(BaseSession):
    def _connect(self):
        connect_kwargs = {
            "host": self.session_kwargs.get("host"),
            "database": self.session_kwargs.get("database"),
            "user": self.session_kwargs.get("user"),
            "password": self.session_kwargs.get("password"),
        }
        self._connection = mysql.connector.connect(
            **get_not_none_key_value(data=connect_kwargs)
        )
        self._cursor = self._connection.cursor()