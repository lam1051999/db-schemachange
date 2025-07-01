import structlog
from textwrap import indent

from typing import Any, Dict, List, Optional, Tuple
from schemachange.config.utils import BaseEnum
from schemachange.config.change_history_table import ChangeHistoryTable

class DatabaseType(BaseEnum):
    POSTGRES = "POSTGRES"
    SQL_SERVER = "SQL_SERVER"
    MYSQL = "MYSQL"
    ORACLE = "ORACLE"
    SNOWFLAKE = "SNOWFLAKE"

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def clear(cls):
        _ = cls._instances.pop(cls, None)

    def clear_all(*args, **kwargs):
        Singleton._instances = {}


class BaseSession(metaclass=Singleton):
    def __init__(self, session_kwargs: Dict[str, Any], logger: structlog.BoundLogger):
        self.session_kwargs = session_kwargs
        self.logger = logger
        self.change_history_table: ChangeHistoryTable = session_kwargs.get("change_history_table")
        self._connection = None
        self._cursor = None

    @property
    def connection(self):
        if self._connection is None or not self._is_connection_alive():
            self._connect()
        return self._connection

    @property
    def cursor(self):
        if self._cursor is None or not self._is_connection_alive():
            self._cursor = self.connection.cursor()
        return self._cursor

    def _connect(self) -> None:
        pass

    def _is_connection_alive(self):
        if self._connection is None:
            return False
        try:
            self._cursor.execute("SELECT 1")
            self.get_executed_query_data(self._cursor)
            return True
        except:
            return False

    def get_executed_query_data(self, cursor) -> List[Dict[str, Any]]:
        columns = list(cursor.description)
        rows = cursor.fetchall()
        data = []
        for r in rows:
            tmp = {}
            for i, col in enumerate(columns):
                tmp[col[0].lower()] = r[i]
            data.append(tmp)

        return data

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        self.logger.debug(
            "Executing query",
            query=indent(query, prefix="\t"),
        )
        cursor = self.cursor
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith(("SELECT", "SHOW", "DESCRIBE", "WITH")):
                return self.get_executed_query_data(cursor)
            else:
                self.connection.commit()
                return cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            raise e

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None

        if self._connection:
            self._connection.close()
            self._connection = None

    









    def fetch_change_history_metadata(self) -> dict:
        # This should only ever return 0 or 1 rows
        query = f"""\
            SELECT
                CREATED,
                LAST_ALTERED
            FROM {self.change_history_table.database_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = REPLACE('{self.change_history_table.schema_name}','\"','')
                AND TABLE_NAME = REPLACE('{self.change_history_table.table_name}','\"','')
        """
        results = self.execute_snowflake_query(query=dedent(query), logger=self.logger)

        # Collect all the results into a list
        change_history_metadata = dict()
        for cursor in results:
            for row in cursor:
                change_history_metadata["created"] = row[0]
                change_history_metadata["last_altered"] = row[1]

        return change_history_metadata

    def change_history_schema_exists(self) -> bool:
        query = f"""\
            SELECT
                COUNT(1)
            FROM {self.change_history_table.database_name}.INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME = REPLACE('{self.change_history_table.schema_name}','\"','')
        """
        results = self.execute_snowflake_query(dedent(query), logger=self.logger)
        for cursor in results:
            for row in cursor:
                return row[0] > 0

    def create_change_history_schema(self, dry_run: bool) -> None:
        query = f"CREATE SCHEMA IF NOT EXISTS {self.change_history_table.fully_qualified_schema_name}"
        if dry_run:
            self.logger.debug(
                "Running in dry-run mode. Skipping execution.",
                query=indent(dedent(query), prefix="\t"),
            )
        else:
            self.execute_snowflake_query(dedent(query), logger=self.logger)

    def create_change_history_table(self, dry_run: bool) -> None:
        query = f"""\
            CREATE TABLE IF NOT EXISTS {self.change_history_table.fully_qualified} (
                VERSION VARCHAR,
                DESCRIPTION VARCHAR,
                SCRIPT VARCHAR,
                SCRIPT_TYPE VARCHAR,
                CHECKSUM VARCHAR,
                EXECUTION_TIME NUMBER,
                STATUS VARCHAR,
                INSTALLED_BY VARCHAR,
                INSTALLED_ON TIMESTAMP_LTZ
            )
        """
        if dry_run:
            self.logger.debug(
                "Running in dry-run mode. Skipping execution.",
                query=indent(dedent(query), prefix="\t"),
            )
        else:
            self.execute_snowflake_query(dedent(query), logger=self.logger)
            self.logger.info(
                f"Created change history table {self.change_history_table.fully_qualified}"
            )

    def change_history_table_exists(
        self, create_change_history_table: bool, dry_run: bool
    ) -> bool:
        change_history_metadata = self.fetch_change_history_metadata()
        if change_history_metadata:
            self.logger.info(
                f"Using existing change history table {self.change_history_table.fully_qualified}",
                last_altered=change_history_metadata["last_altered"],
            )
            return True
        elif create_change_history_table:
            schema_exists = self.change_history_schema_exists()
            if not schema_exists:
                self.create_change_history_schema(dry_run=dry_run)
            self.create_change_history_table(dry_run=dry_run)
            if dry_run:
                return False
            self.logger.info("Created change history table")
            return True
        else:
            raise ValueError(
                f"Unable to find change history table {self.change_history_table.fully_qualified}"
            )

    def get_script_metadata(
        self, create_change_history_table: bool, dry_run: bool
    ) -> tuple[
        dict[str, dict[str, str | int]] | None,
        dict[str, list[str]] | None,
        str | int | None,
    ]:
        change_history_table_exists = self.change_history_table_exists(
            create_change_history_table=create_change_history_table,
            dry_run=dry_run,
        )
        if not change_history_table_exists:
            return None, None, None

        change_history, max_published_version = self.fetch_versioned_scripts()
        r_scripts_checksum = self.fetch_repeatable_scripts()

        self.logger.info(
            "Max applied change script version %(max_published_version)s"
            % {"max_published_version": max_published_version}
        )
        return change_history, r_scripts_checksum, max_published_version

    def fetch_repeatable_scripts(self) -> dict[str, list[str]]:
        query = f"""\
        SELECT DISTINCT
            SCRIPT AS SCRIPT_NAME,
            FIRST_VALUE(CHECKSUM) OVER (
                PARTITION BY SCRIPT
                ORDER BY INSTALLED_ON DESC
            ) AS CHECKSUM
        FROM {self.change_history_table.fully_qualified}
        WHERE SCRIPT_TYPE = 'R'
            AND STATUS = 'Success'
        """
        results = self.execute_snowflake_query(dedent(query), logger=self.logger)

        # Collect all the results into a dict
        script_checksums: dict[str, list[str]] = defaultdict(list)
        for cursor in results:
            for script_name, checksum in cursor:
                script_checksums[script_name].append(checksum)
        return script_checksums

    def fetch_versioned_scripts(
        self,
    ) -> tuple[dict[str, dict[str, str | int]], str | int | None]:
        query = f"""\
        SELECT VERSION, SCRIPT, CHECKSUM
        FROM {self.change_history_table.fully_qualified}
        WHERE SCRIPT_TYPE = 'V'
        ORDER BY INSTALLED_ON DESC -- TODO: Why not order by version?
        """
        results = self.execute_snowflake_query(dedent(query), logger=self.logger)

        # Collect all the results into a list
        versioned_scripts: dict[str, dict[str, str | int]] = defaultdict(dict)
        versions: list[str | int | None] = []
        for cursor in results:
            for version, script, checksum in cursor:
                versions.append(version if version != "" else None)
                versioned_scripts[script] = {
                    "version": version,
                    "script": script,
                    "checksum": checksum,
                }

        # noinspection PyTypeChecker
        return versioned_scripts, versions[0] if versions else None