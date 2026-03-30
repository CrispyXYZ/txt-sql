from decimal import Decimal

from . import storage
from .evaluator import evaluate_where
from .parser import DropTable, CreateTable, InsertValues, DeleteStatement
from .types import Types

_TYPE_MAP = {
    'STRING': Types.STRING,
    'NUMBER': Types.NUMBER,
    'BINARY': Types.BINARY,
}


def execute_delete(statement: DeleteStatement) -> int:
    """执行 DELETE 语句"""
    table = storage.get_table(statement.table_name)
    if table is None:
        raise ValueError(f'Table does not exist: {statement.table_name}')

    where_func = None
    if statement.where_clause is not None:
        where_func = evaluate_where(statement.where_clause.expression, table.defs)

    pre_count = table.count_rows()
    table.delete(where=where_func)
    post_count = table.count_rows()
    return pre_count - post_count


def execute_drop(statement: DropTable) -> None:
    storage.drop_table(statement.table_name)


def execute_create(statement: CreateTable) -> None:
    defs = {col_name: _TYPE_MAP[type_str] for col_name, type_str in statement.columns}
    storage.create_table(statement.table_name, defs)


def execute_insert(statement: InsertValues) -> None:
    table = storage.get_table(statement.table_name)
    if table is None:
        raise ValueError(f'Table does not exist: {statement.table_name}')

    all_columns = list(table.defs.keys())

    # Determine which columns to insert
    if statement.columns is None:
        # If no column names specified, insert all columns
        target_columns = all_columns
        if len(target_columns) != len(statement.values[0]):
            raise ValueError(f'Column count mismatch: expected {len(target_columns)}, got {len(statement.values[0])}')
    else:
        # If column names specified, check if columns exist
        for col in statement.columns:
            if col not in all_columns:
                raise ValueError(f'Column does not exist: {col}')
        target_columns = statement.columns

    # Insert all rows
    for value_row in statement.values:
        if len(target_columns) != len(value_row):
            raise ValueError(f'Column count mismatch: expected {len(target_columns)}, got {len(value_row)}')

        row_data = {}
        # Populate specified columns
        for col, val in zip(target_columns, value_row):
            row_data[col] = val

        # Add default values for unspecified columns
        for col in all_columns:
            if col not in row_data:
                col_type = table.defs[col]
                if col_type == 'STRING':
                    row_data[col] = ''  # String defaults to empty string
                elif col_type == 'NUMBER':
                    row_data[col] = Decimal('0')  # Number defaults to 0
                elif col_type == 'BINARY':
                    row_data[col] = b''  # Binary defaults to empty bytes
                else:
                    row_data[col] = None

        table.insert_values(row_data)
