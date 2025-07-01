import dataclasses
from typing import ClassVar

from schemachange.config.utils import get_identifier_string


@dataclasses.dataclass(frozen=True)
class ChangeHistoryTable:
    _default_database_name: ClassVar[str] = "METADATA"
    _default_schema_name: ClassVar[str] = "SCHEMACHANGE"
    _default_table_name: ClassVar[str] = "CHANGE_HISTORY"

    table_name: str = "CHANGE_HISTORY"
    schema_name: str = "SCHEMACHANGE"
    database_name: str = "METADATA"

    @property
    def fully_qualified(self) -> str:
        return (
            f"{self.database_name}.{self.schema_name}.{self.table_name}"
            if self.schema_name
            else f"{self.database_name}.{self.table_name}"
        )

    @property
    def fully_qualified_schema_name(self) -> str:
        return (
            f"{self.database_name}.{self.schema_name}"
            if self.schema_name
            else self.database_name
        )

    @classmethod
    def from_str(cls, table_str: str, include_schema: bool):
        database_name = cls._default_database_name
        schema_name = cls._default_schema_name if include_schema else None
        table_name = cls._default_table_name

        if table_str is not None:
            table_name_parts = table_str.upper().strip().split(".")

            if include_schema:
                if len(table_name_parts) == 1:
                    table_name = table_name_parts[0]
                elif len(table_name_parts) == 2:
                    table_name = table_name_parts[1]
                    schema_name = table_name_parts[0]
                elif len(table_name_parts) == 3:
                    table_name = table_name_parts[2]
                    schema_name = table_name_parts[1]
                    database_name = table_name_parts[0]
                else:
                    raise ValueError(f"Invalid change history table name: {table_str}")
            else:
                if len(table_name_parts) == 1:
                    table_name = table_name_parts[0]
                elif len(table_name_parts) == 2:
                    table_name = table_name_parts[1]
                    database_name = table_name_parts[0]
                else:
                    raise ValueError(
                        f"Invalid change history table name: {table_str}, expect maximum 2 parts (database.table)"
                    )

        return cls(
            table_name=get_identifier_string(
                input_value=table_name, input_type="table_name"
            ),
            schema_name=get_identifier_string(
                input_value=schema_name, input_type="schema_name"
            ),
            database_name=get_identifier_string(
                input_value=database_name, input_type="database_name"
            ),
        )
