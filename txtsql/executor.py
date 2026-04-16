from decimal import Decimal

from . import storage
from .evaluator import evaluate_where
from .parser import DropTable, CreateTable, InsertValues, DeleteStatement, SelectStatement, UpdateStatement, \
    AggFunc, AggregateColumn
from .storage import RowDict, DataValue
from .types import Types

_TYPE_MAP = {
    'STRING': Types.STRING,
    'NUMBER': Types.NUMBER,
    'BINARY': Types.BINARY,
}


def execute_delete(statement: DeleteStatement) -> int:
    """Execute DELETE statement"""
    table = storage.get_table(statement.table_name)
    if table is None:
        raise ValueError(f'Table does not exist: {statement.table_name}')

    where_func = None
    if statement.where_clause is not None:
        where_func = evaluate_where(statement.where_clause.expression, table.defs)

    return table.delete(where=where_func)


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

        # Unspecified columns default to NULL
        for col in all_columns:
            if col not in row_data:
                row_data[col] = None

        table.insert_values(row_data)


def _make_agg_func(agg_col: AggregateColumn):
    """Build an aggregation function for a single AggregateColumn."""
    func = agg_col.func
    col = agg_col.column
    match func:
        case AggFunc.COUNT:
            if col is None:
                return lambda group: Decimal(len(group))
            return lambda group, _c=col: Decimal(sum(1 for row in group if row.get(_c) is not None))
        case AggFunc.SUM:
            def _sum(group, _c=col):
                vals = [row[_c] for row in group if row.get(_c) is not None]
                return sum(vals, Decimal(0)) if vals else None
            return _sum
        case AggFunc.AVG:
            def _avg(group, _c=col):
                vals = [row[_c] for row in group if row.get(_c) is not None]
                return sum(vals, Decimal(0)) / len(vals) if vals else None
            return _avg
        case AggFunc.MIN:
            def _min(group, _c=col):
                vals = [row[_c] for row in group if row.get(_c) is not None]
                return min(vals) if vals else None
            return _min
        case AggFunc.MAX:
            def _max(group, _c=col):
                vals = [row[_c] for row in group if row.get(_c) is not None]
                return max(vals) if vals else None
            return _max


def execute_select(statement: SelectStatement) -> list[RowDict]:
    """Execute SELECT statement"""
    table = storage.get_table(statement.table_name)
    if table is None:
        raise ValueError(f'Table does not exist: {statement.table_name}')

    where_func = None
    if statement.where_clause is not None:
        where_func = evaluate_where(statement.where_clause.expression, table.defs)

    # Build aggregations dict
    aggregations = None
    if statement.aggregates:
        aggregations = {agg_col.alias: _make_agg_func(agg_col) for agg_col in statement.aggregates}

    # Build having function
    having_func = None
    if statement.having is not None:
        # Build schema for the aggregated result rows
        having_defs: dict[str, Types] = {}
        if statement.group_by:
            for gcol in statement.group_by:
                if gcol in table.defs:
                    having_defs[gcol] = table.defs[gcol]
        for agg_col in statement.aggregates:
            if agg_col.func in (AggFunc.MIN, AggFunc.MAX) and agg_col.column in table.defs:
                having_defs[agg_col.alias] = table.defs[agg_col.column]
            else:
                having_defs[agg_col.alias] = Types.NUMBER
        having_func = evaluate_where(statement.having.expression, having_defs)

    return table.select(
        columns=statement.columns,
        aggregations=aggregations,
        where=where_func,
        group_by=statement.group_by,
        having=having_func,
        order_by=statement.order_by,
        distinct=statement.distinct,
        limit=statement.limit,
        offset=statement.offset,
    )


def execute_update(statement: UpdateStatement) -> int:
    """Execute UPDATE statement, returns number of affected rows"""
    table = storage.get_table(statement.table_name)
    if table is None:
        raise ValueError(f'Table does not exist: {statement.table_name}')

    all_columns = list(table.defs.keys())
    for col, _ in statement.set_clauses:
        if col not in all_columns:
            raise ValueError(f'Column does not exist: {col}')

    where_func = None
    if statement.where_clause is not None:
        where_func = evaluate_where(statement.where_clause.expression, table.defs)

    # Count affected rows before updating
    affected = len(table.select(where=where_func))

    values: RowDict = {col: val for col, val in statement.set_clauses}
    table.update(values, where=where_func)

    return affected
