from . import storage
from .parser import DropTable, CreateTable
from .types import Types

_TYPE_MAP = {
    'STRING': Types.STRING,
    'NUMBER': Types.NUMBER,
    'BINARY': Types.BINARY,
}


def execute_drop(statement: DropTable) -> None:
    storage.drop_table(statement.table_name)


def execute_create(statement: CreateTable) -> None:
    defs = {col_name: _TYPE_MAP[type_str] for col_name, type_str in statement.columns}
    storage.create_table(statement.table_name, defs)
