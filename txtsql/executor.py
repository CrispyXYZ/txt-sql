from . import storage
from .parser import DropTable


def execute_drop(statement: DropTable) -> None:
    storage.drop_table(statement.table_name)
