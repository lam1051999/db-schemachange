from typing import Any, Dict
import structlog

from schemachange.session.base import DatabaseType, BaseSession
from schemachange.session.mysql_session import MySQLSession
from schemachange.session.postgres_session import PostgresSession
from schemachange.session.sqlserver_session import SQLServerSession
from schemachange.session.oracle_session import OracleSession


def get_db_session(db_type: str, logger: structlog.BoundLogger, session_kwargs: Dict[str, Any]) -> BaseSession:
    if db_type == DatabaseType.POSTGRES:
        db_session = PostgresSession(logger=logger, session_kwargs=session_kwargs)
    elif db_type == DatabaseType.SQL_SERVER:
        db_session = SQLServerSession(logger=logger, session_kwargs=session_kwargs)
    elif db_type == DatabaseType.MYSQL:
        db_session = MySQLSession(logger=logger, session_kwargs=session_kwargs)
    elif db_type == DatabaseType.ORACLE:
        db_session = OracleSession(logger=logger, session_kwargs=session_kwargs)
    else:
        raise DatabaseType.validate_value(attr='db_type', value=db_type)

    return db_session