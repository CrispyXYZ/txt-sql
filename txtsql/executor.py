"""Statement executors: interpret AST and drive the storage engine."""

import csv
from dataclasses import dataclass
from decimal import Decimal

import openpyxl

from . import storage
from .ast import (
    DropTable, CreateTable, InsertValues, DeleteStatement, SelectStatement,
    UnionStatement, UpdateStatement, ImportStatement, ShowTables, DescribeTable,
    AggFunc, AggregateExpression, BinaryExpression, ColumnExpression,
    ConditionExpression, Expression, InExpression, AnyAllExpression,
    JoinClause, JoinType, LogicalExpression, NullCheckExpression, SelectExpression,
)
from .evaluator import (
    evaluate_where, evaluate_expression, resolve_column_name,
    _resolve_column, _to_bool, _is_number, _to_decimal,
)
from .exceptions import ColumnNotFoundError, TableNotFoundError, EngineError, TypeMismatchError
from .types import Types, RowDict, DataValue


_TYPE_MAP = {'STRING': Types.STRING, 'NUMBER': Types.NUMBER}


@dataclass(frozen=True)
class _VisibleColumn:
    """A column visible in the current joined relation."""
    qualifier: str
    name: str
    key: str
    coalesce_keys: tuple[str, ...] = ()


# ======================================================================
# DDL
# ======================================================================

def execute_drop(statement: DropTable) -> None:
    storage.drop_table(statement.table_name)


def execute_create(statement: CreateTable) -> None:
    defs = {col_name: _TYPE_MAP[type_str] for col_name, type_str in statement.columns}
    storage.create_table(statement.table_name, defs)


# ======================================================================
# DML
# ======================================================================

def _exec_subquery(sub: object, outer_row: RowDict | None = None) -> list[RowDict]:
    """Execute a nested SELECT or UNION subquery."""
    if isinstance(sub, SelectStatement):
        return execute_select(sub, outer_row=outer_row)
    if isinstance(sub, UnionStatement):
        return execute_union(sub, outer_row=outer_row)
    return []


def execute_delete(statement: DeleteStatement) -> int:
    table = _get_table(statement.table_name)
    where_func = None
    if statement.where_clause is not None:
        where_func = evaluate_where(
            statement.where_clause.expression,
            table.column_types(),
            subquery_exec=_exec_subquery,
            valid_single_qualifier=statement.table_name,
        )
    return table.delete(where=where_func)


def execute_insert(statement: InsertValues) -> None:
    table = _get_table(statement.table_name)
    all_columns = table.column_names()

    if statement.columns is None:
        target_columns = all_columns
    else:
        for col in statement.columns:
            if col not in all_columns:
                raise ColumnNotFoundError(f'Column does not exist: {col}')
        target_columns = statement.columns

    prepared_rows: list[RowDict] = []
    for value_row in statement.values:
        if len(target_columns) != len(value_row):
            raise EngineError('Column count mismatch')
        row_data: RowDict = {col: val for col, val in zip(target_columns, value_row)}
        for col in all_columns:
            if col not in row_data:
                row_data[col] = None
        prepared_rows.append(row_data)

    table.insert_rows(prepared_rows)


def _get_table(name: str):
    t = storage.get_table(name)
    if t is None:
        raise TableNotFoundError(f'Table does not exist: {name}')
    return t


# ======================================================================
# Aggregation helpers
# ======================================================================

def _has_aggregate(expr: Expression) -> bool:
    match expr:
        case AggregateExpression():
            return True
        case BinaryExpression(left, _, right) | ConditionExpression(left, _, right) | LogicalExpression(left, _, right):
            return _has_aggregate(left) or _has_aggregate(right)
        case NullCheckExpression(inner, _):
            return _has_aggregate(inner)
        case InExpression(inner, _, _) | AnyAllExpression(inner, _, _, _):
            return _has_aggregate(inner)
        case _:
            return False


def _eval_aggregate(
    agg_expr: AggregateExpression,
    group_rows: list[RowDict],
    valid_single_qualifier: str | None,
) -> DataValue:
    func, col = agg_expr.func, agg_expr.column
    if func == AggFunc.COUNT and col is None:
        return Decimal(len(group_rows))

    assert col is not None
    vals = [
        evaluate_expression(col, r, valid_single_qualifier=valid_single_qualifier)
        for r in group_rows
    ]
    non_null = [v for v in vals if v is not None]

    match func:
        case AggFunc.COUNT:
            return Decimal(len(non_null))
        case AggFunc.SUM:
            if not non_null:
                return None
            num_vals = [_to_decimal(v) for v in non_null]
            return sum(num_vals, Decimal(0))
        case AggFunc.AVG:
            if not non_null:
                return None
            num_vals = [_to_decimal(v) for v in non_null]
            return sum(num_vals, Decimal(0)) / Decimal(len(num_vals))
        case AggFunc.MIN:
            if not non_null:
                return None
            try:
                return min(non_null)
            except TypeError as exc:
                raise TypeMismatchError(f'Incompatible types in MIN: {exc}') from exc
        case AggFunc.MAX:
            if not non_null:
                return None
            try:
                return max(non_null)
            except TypeError as exc:
                raise TypeMismatchError(f'Incompatible types in MAX: {exc}') from exc


def _group_rows(
    rows: list[RowDict],
    group_by: list[ColumnExpression] | None,
    valid_single_qualifier: str | None,
) -> list[tuple[tuple, RowDict, list[RowDict]]]:
    """Group rows and build a representative row for each group containing only GROUP BY columns."""
    if not group_by:
        return [((), {}, rows)]

    grouped: dict[tuple, tuple[RowDict, list[RowDict]]] = {}
    for row in rows:
        key_vals: list[DataValue] = []
        rep_row: RowDict = {}
        for col in group_by:
            val = _resolve_column(row, col.column_name, col.table, valid_single_qualifier=valid_single_qualifier)
            key_vals.append(val)
            # Store both bare and qualified forms when unambiguous in the group row
            if col.table is not None:
                rep_row[f"{col.table}.{col.column_name}"] = val
            else:
                rep_row[col.column_name] = val
                matching_keys = [k for k in row if k.endswith(f".{col.column_name}")]
                if len(matching_keys) == 1:
                    rep_row[matching_keys[0]] = val
        key = tuple(
            (0, None) if v is None else (1, v) if isinstance(v, Decimal) else (2, v)
            for v in key_vals
        )
        if key not in grouped:
            grouped[key] = (rep_row, [])
        grouped[key][1].append(row)

    return [(key, rep, g_rows) for key, (rep, g_rows) in grouped.items()]


# ======================================================================
# SELECT & UNION
# ======================================================================

def _build_select_rows(
    statement: SelectStatement,
    outer_row: RowDict | None = None,
) -> tuple[list[RowDict], list[str]]:
    """Execute the core SELECT pipeline up through DISTINCT (before ORDER/LIMIT/OFFSET)."""
    sources = statement.sources
    tables = [_get_table(src.name) for src in sources]

    # Ensure unique effective table names / aliases in FROM + JOIN
    seen_aliases: set[str] = set()
    for src in sources:
        alias = src.alias or src.name
        if alias in seen_aliases:
            raise EngineError(f'Not unique table/alias: {alias}')
        seen_aliases.add(alias)
    for jc in statement.joins:
        alias = jc.source.alias or jc.source.name
        if alias in seen_aliases:
            raise EngineError(f'Not unique table/alias: {alias}')
        seen_aliases.add(alias)

    rows_per_table = [table.select() for table in tables]
    use_prefixes = len(sources) > 1 or bool(statement.joins)
    single_qualifier = (sources[0].alias or sources[0].name) if not use_prefixes else None

    combined: list[RowDict] = []
    visible_columns: list[_VisibleColumn] | None = None
    if use_prefixes:
        for combo in _cartesian_product(rows_per_table):
            row: RowDict = {}
            for src, r in zip(sources, combo):
                alias = src.alias or src.name
                row.update(_prefix_row(r, alias))
            combined.append(row)
        visible_columns = []
        for src, table in zip(sources, tables):
            alias = src.alias or src.name
            visible_columns.extend(
                _VisibleColumn(alias, col, f"{alias}.{col}") for col in table.column_names()
            )
    else:
        combined = [dict(r) for r in rows_per_table[0]]

    # Apply explicit JOIN clauses left-to-right.
    for jc in statement.joins:
        join_table = _get_table(jc.source.name)
        join_alias = jc.source.alias or jc.source.name
        right_columns = [
            _VisibleColumn(join_alias, col, f"{join_alias}.{col}")
            for col in join_table.column_names()
        ]
        right_rows = [_prefix_row(r, join_alias) for r in join_table.select()]
        combined, visible_columns = _apply_join(
            combined, visible_columns or [], right_rows, right_columns, jc,
            outer_row=outer_row,
        )
        tables.append(join_table)

    # WHERE applies after the full FROM/JOIN has been built.
    if statement.where_clause is not None:
        all_defs: dict[str, Types] = {}
        for t in tables:
            all_defs.update(t.column_types())
        where_func = evaluate_where(
            statement.where_clause.expression,
            all_defs,
            subquery_exec=_exec_subquery,
            outer_row=outer_row,
            valid_single_qualifier=single_qualifier,
        )
        combined = [r for r in combined if where_func(r)]

    has_agg_in_select = statement.columns is not None and any(
        _has_aggregate(se.expr) for se in statement.columns
    )
    has_agg_in_having = statement.having is not None and _has_aggregate(statement.having.expression)
    is_grouped = has_agg_in_select or statement.group_by is not None or has_agg_in_having

    # List of (raw_or_rep_row, projected_row) pairs to preserve ORDER BY access to unprojected columns
    paired: list[tuple[RowDict, RowDict]] = []

    if not is_grouped:
        if statement.columns is not None:
            for row in combined:
                proj = _project_select_expressions(
                    row, statement.columns,
                    subquery_exec=_exec_subquery,
                    agg_eval=None,
                    outer_row=outer_row,
                    valid_single_qualifier=single_qualifier,
                )
                paired.append((row, proj))
        elif visible_columns is not None:
            projected = _project_star(combined, visible_columns)
            paired = list(zip(combined, projected))
        else:
            paired = [(row, dict(row)) for row in combined]
    else:
        all_defs = {}
        for t in tables:
            all_defs.update(t.column_types())
        groups = _group_rows(combined, statement.group_by, single_qualifier)
        for _, rep_row, group_rows in groups:
            def _agg_eval(agg_expr: AggregateExpression, _gr=group_rows) -> DataValue:
                return _eval_aggregate(agg_expr, _gr, single_qualifier)

            if statement.columns is not None:
                proj = _project_select_expressions(
                    rep_row, statement.columns,
                    subquery_exec=_exec_subquery,
                    agg_eval=_agg_eval,
                    outer_row=outer_row,
                    valid_single_qualifier=single_qualifier,
                )
            else:
                proj = dict(rep_row)

            if statement.having is not None:
                having_row = {**rep_row, **proj}
                having_ok = _to_bool(evaluate_expression(
                    statement.having.expression,
                    having_row,
                    all_defs,
                    subquery_exec=_exec_subquery,
                    agg_eval=_agg_eval,
                    outer_row=outer_row,
                    valid_single_qualifier=single_qualifier,
                ))
                if not having_ok:
                    continue

            paired.append((rep_row, proj))

    # DISTINCT (dedup on projected row)
    if statement.distinct:
        seen = set()
        unique_pairs: list[tuple[RowDict, RowDict]] = []
        for raw_r, proj_r in paired:
            key = _row_dedup_key(proj_r)
            if key not in seen:
                seen.add(key)
                unique_pairs.append((raw_r, proj_r))
        paired = unique_pairs

    # ORDER BY (resolve against projected row first, then raw/group row)
    if statement.order_by:
        paired = _order_pairs(paired, statement.order_by, single_qualifier)

    result = [proj_r for _, proj_r in paired]

    # Determine canonical column headers even when result is empty
    if result:
        headers = list(result[0].keys())
    elif statement.columns is not None:
        headers = _compute_select_headers(statement.columns)
    elif visible_columns is not None:
        headers = _compute_star_headers(visible_columns)
    else:
        headers = tables[0].column_names()

    return result, headers


def execute_select(statement: SelectStatement, outer_row: RowDict | None = None) -> list[RowDict] | int:
    result, headers = _build_select_rows(statement, outer_row=outer_row)

    # LIMIT / OFFSET
    if statement.offset > 0:
        result = result[statement.offset:]
    if statement.limit is not None:
        result = result[:statement.limit]

    # INTO OUTFILE
    if statement.output_file is not None:
        return _write_output(result, headers, statement.output_file)

    return result


def execute_union(statement: UnionStatement, outer_row: RowDict | None = None) -> list[RowDict] | int:
    first_stmt, _ = statement.branches[0]
    combined, headers = _build_select_rows(first_stmt, outer_row=outer_row)
    expected_cols = len(headers)

    # Deduplicate first branch? Only if followed by a non-ALL UNION; handled below on merge.
    for branch_stmt, is_all in statement.branches[1:]:
        branch_rows, branch_headers = _build_select_rows(branch_stmt, outer_row=outer_row)
        if len(branch_headers) != expected_cols:
            raise EngineError('The used SELECT statements have a different number of columns')
        rekeyed = [
            {h: v for h, v in zip(headers, r.values())}
            for r in branch_rows
        ]
        if is_all:
            combined.extend(rekeyed)
        else:
            combined = _dedup_rows(combined + rekeyed)

    if statement.order_by:
        pairs = [(r, r) for r in combined]
        pairs = _order_pairs(pairs, statement.order_by, valid_single_qualifier=None)
        combined = [proj_r for _, proj_r in pairs]

    if statement.offset > 0:
        combined = combined[statement.offset:]
    if statement.limit is not None:
        combined = combined[:statement.limit]

    if statement.output_file is not None:
        return _write_output(combined, headers, statement.output_file)

    return combined


# ======================================================================
# Projection, Dedup, Ordering helpers
# ======================================================================

def _project_select_expressions(
    row: RowDict,
    select_exprs: list[SelectExpression],
    subquery_exec,
    agg_eval,
    outer_row: RowDict | None,
    valid_single_qualifier: str | None,
) -> RowDict:
    new_row: RowDict = {}
    for se in select_exprs:
        val = evaluate_expression(
            se.expr,
            row,
            subquery_exec=subquery_exec,
            agg_eval=agg_eval,
            outer_row=outer_row,
            valid_single_qualifier=valid_single_qualifier,
        )
        name = se.alias or resolve_column_name(se.expr) or '?'
        if se.alias is None and isinstance(se.expr, ColumnExpression) and se.expr.table is not None and name in new_row:
            name = f"{se.expr.table}.{se.expr.column_name}"
        new_row[name] = val
    return new_row


def _compute_select_headers(select_exprs: list[SelectExpression]) -> list[str]:
    headers: list[str] = []
    used: set[str] = set()
    for se in select_exprs:
        name = se.alias or resolve_column_name(se.expr) or '?'
        if se.alias is None and isinstance(se.expr, ColumnExpression) and se.expr.table is not None and name in used:
            name = f"{se.expr.table}.{se.expr.column_name}"
        used.add(name)
        headers.append(name)
    return headers


def _compute_star_headers(visible_columns: list[_VisibleColumn]) -> list[str]:
    used_names: set[str] = set()
    headers: list[str] = []
    for col in visible_columns:
        header = col.name
        if header in used_names:
            header = f"{col.qualifier}.{col.name}"
        used_names.add(col.name)
        headers.append(header)
    return headers


def _row_dedup_key(row: RowDict) -> tuple:
    return tuple(
        (k, (0, None) if v is None else (1, v) if isinstance(v, Decimal) else (2, v))
        for k, v in row.items()
    )


def _dedup_rows(rows: list[RowDict]) -> list[RowDict]:
    seen = set()
    result: list[RowDict] = []
    for r in rows:
        key = _row_dedup_key(r)
        if key not in seen:
            seen.add(key)
            result.append(r)
    return result


def _resolve_order_value(
    raw_row: RowDict,
    proj_row: RowDict,
    col_expr: ColumnExpression,
    valid_single_qualifier: str | None,
) -> DataValue:
    if col_expr.table is None:
        if col_expr.column_name in proj_row:
            return proj_row[col_expr.column_name]
        return _resolve_column(
            raw_row, col_expr.column_name, None,
            valid_single_qualifier=valid_single_qualifier,
        )
    qualified = f"{col_expr.table}.{col_expr.column_name}"
    if qualified in proj_row:
        return proj_row[qualified]
    return _resolve_column(
        raw_row, col_expr.column_name, col_expr.table,
        valid_single_qualifier=valid_single_qualifier,
    )


def _order_pairs(
    paired: list[tuple[RowDict, RowDict]],
    order_by: list[tuple[ColumnExpression, bool]],
    valid_single_qualifier: str | None,
) -> list[tuple[RowDict, RowDict]]:
    result = list(paired)
    for col_expr, desc in reversed(order_by):
        keyed = [
            (_resolve_order_value(raw_r, proj_r, col_expr, valid_single_qualifier), raw_r, proj_r)
            for raw_r, proj_r in result
        ]
        null_items = [(raw_r, proj_r) for val, raw_r, proj_r in keyed if val is None]
        non_null_items = [(val, raw_r, proj_r) for val, raw_r, proj_r in keyed if val is not None]
        try:
            non_null_items.sort(key=lambda item: item[0], reverse=desc)
        except TypeError as exc:
            raise TypeMismatchError(f'Cannot sort mixed types in column {col_expr.column_name}: {exc}') from exc
        result = [(raw_r, proj_r) for _, raw_r, proj_r in non_null_items] + null_items
    return result


# ======================================================================
# JOIN helpers
# ======================================================================

def _prefix_row(row: RowDict, alias: str) -> RowDict:
    """Prefix every column in a row with the table alias."""
    return {f"{alias}.{col}": val for col, val in row.items()}


def _coalesced_value(row: RowDict, col: _VisibleColumn) -> DataValue:
    value = row.get(col.key)
    if value is not None:
        return value
    for fallback in col.coalesce_keys:
        value = row.get(fallback)
        if value is not None:
            return value
    return None


def _apply_join(
    left_rows: list[RowDict],
    left_columns: list[_VisibleColumn],
    right_rows: list[RowDict],
    right_columns: list[_VisibleColumn],
    jc: JoinClause,
    outer_row: RowDict | None = None,
) -> tuple[list[RowDict], list[_VisibleColumn]]:
    """Apply one JOIN clause to the current left relation."""
    common = _common_join_columns(left_columns, right_columns, jc)
    null_right = {col.key: None for col in right_columns}
    null_left = {col.key: None for col in left_columns}
    new_rows: list[RowDict] = []
    matched_right: set[int] = set()

    if jc.join_type == JoinType.CROSS:
        for left_row in left_rows:
            for right_row in right_rows:
                new_rows.append({**left_row, **right_row})
    else:
        for left_row in left_rows:
            matches: list[tuple[int, RowDict]] = []
            for right_index, right_row in enumerate(right_rows):
                if _join_match(left_row, right_row, left_columns, right_columns, jc, common, outer_row):
                    matches.append((right_index, {**left_row, **right_row}))
                    if jc.join_type in (JoinType.RIGHT, JoinType.FULL):
                        matched_right.add(right_index)
            if matches:
                new_rows.extend(row for _, row in matches)
            elif jc.join_type in (JoinType.LEFT, JoinType.FULL):
                new_rows.append({**left_row, **null_right})

        if jc.join_type in (JoinType.RIGHT, JoinType.FULL):
            for right_index, right_row in enumerate(right_rows):
                if right_index not in matched_right:
                    new_rows.append({**null_left, **right_row})

    visible = _visible_after_join(left_columns, right_columns, jc, common)

    # For USING/NATURAL joins, expose coalesced unqualified column names on merged rows
    # so SELECT col / WHERE col / GROUP BY col / ORDER BY col work unambiguously even for RIGHT/FULL JOIN.
    if common is not None:
        coalesced_cols = [col for col in visible if col.coalesce_keys]
        for row in new_rows:
            for col in coalesced_cols:
                row[col.name] = _coalesced_value(row, col)

    return new_rows, visible


def _common_join_columns(
    left_columns: list[_VisibleColumn],
    right_columns: list[_VisibleColumn],
    jc: JoinClause,
) -> list[str] | None:
    """Return shared column names for USING/NATURAL joins, else None."""
    left_names = {col.name for col in left_columns}
    right_names = {col.name for col in right_columns}

    if jc.natural:
        common: list[str] = []
        for col in left_columns:
            if col.name in right_names and col.name not in common:
                common.append(col.name)
        return common

    if jc.using_columns is not None:
        missing = [
            name for name in jc.using_columns
            if name not in left_names or name not in right_names
        ]
        if missing:
            raise ColumnNotFoundError(
                f'USING column does not exist in both tables: {missing[0]}')
        return list(jc.using_columns)

    return None


def _join_match(
    left_row: RowDict,
    right_row: RowDict,
    left_columns: list[_VisibleColumn],
    right_columns: list[_VisibleColumn],
    jc: JoinClause,
    common: list[str] | None,
    outer_row: RowDict | None = None,
) -> bool:
    """Test whether one left/right row pair satisfies the JOIN condition."""
    if jc.condition is not None:
        return _to_bool(evaluate_expression(
            jc.condition,
            {**left_row, **right_row},
            subquery_exec=_exec_subquery,
            outer_row=outer_row,
        ))

    if common is not None:
        for name in common:
            left_cols = [col for col in left_columns if col.name == name]
            right_cols = [col for col in right_columns if col.name == name]
            if len(left_cols) != 1 or len(right_cols) != 1:
                raise EngineError(f'Ambiguous column in JOIN: {name}')
            left_value = _coalesced_value(left_row, left_cols[0])
            right_value = _coalesced_value(right_row, right_cols[0])
            if left_value is None or right_value is None or left_value != right_value:
                return False
        return True

    # CROSS JOIN / NATURAL JOIN with no common columns.
    return True


def _visible_after_join(
    left_columns: list[_VisibleColumn],
    right_columns: list[_VisibleColumn],
    jc: JoinClause,
    common: list[str] | None,
) -> list[_VisibleColumn]:
    """Compute the standard column order after a JOIN."""
    if common is None:
        return [*left_columns, *right_columns]

    common_set = set(common)
    visible: list[_VisibleColumn] = []

    # USING/NATURAL: shared columns first, then left-only, then right-only.
    for name in common:
        left_col = next((col for col in left_columns if col.name == name), None)
        right_col = next((col for col in right_columns if col.name == name), None)
        if left_col is not None and right_col is not None:
            visible.append(_VisibleColumn(
                left_col.qualifier,
                name,
                left_col.key,
                (*left_col.coalesce_keys, right_col.key),
            ))
        elif left_col is not None:
            visible.append(left_col)
        elif right_col is not None:
            visible.append(right_col)

    visible.extend(col for col in left_columns if col.name not in common_set)
    visible.extend(col for col in right_columns if col.name not in common_set)
    return visible


def _project_star(rows: list[RowDict], visible_columns: list[_VisibleColumn]) -> list[RowDict]:
    """Render SELECT * with standard unqualified column names.

    Duplicate bare names keep their table qualifier so every column remains
    addressable in a RowDict.
    """
    if not rows:
        return []

    headers = _compute_star_headers(visible_columns)
    result: list[RowDict] = []
    for row in rows:
        projected: RowDict = {}
        for col, header in zip(visible_columns, headers):
            projected[header] = _coalesced_value(row, col)
        result.append(projected)
    return result


def _cartesian_product(rows_per_table: list[list[RowDict]]) -> list[list[RowDict]]:
    """Generate all combinations of rows from multiple tables (cartesian product)."""
    if not rows_per_table:
        return []
    result = [[r] for r in rows_per_table[0]]
    for table_rows in rows_per_table[1:]:
        new_result = []
        for combo in result:
            for r in table_rows:
                new_result.append(combo + [r])
        result = new_result
    return result


# ======================================================================
# UPDATE
# ======================================================================

def execute_update(statement: UpdateStatement) -> int:
    table = _get_table(statement.table_name)
    all_columns = table.column_names()
    for col, _ in statement.set_clauses:
        if col not in all_columns:
            raise ColumnNotFoundError(f'Column does not exist: {col}')
    where_func = None
    if statement.where_clause is not None:
        where_func = evaluate_where(
            statement.where_clause.expression,
            table.column_types(),
            subquery_exec=_exec_subquery,
            valid_single_qualifier=statement.table_name,
        )
    values: RowDict = {col: val for col, val in statement.set_clauses}
    return table.update(values, where=where_func)


# ======================================================================
# System commands
# ======================================================================

def execute_show_tables(statement: ShowTables) -> list[RowDict]:
    rows: list[RowDict] = []
    for name, _, defs in storage.list_tables():
        table = storage.get_table(name)
        row_count = table.count_rows() if table else 0
        rows.append({'Table': name, 'Columns': ', '.join(defs.keys()), 'Rows': row_count})
    return rows


def execute_describe(statement: DescribeTable) -> list[RowDict]:
    table = storage.get_table(statement.table_name)
    if table is None:
        raise TableNotFoundError(f'Table does not exist: {statement.table_name}')
    rows: list[RowDict] = []
    for col_name, col_type in table.defs.items():
        rows.append({'Column': col_name, 'Type': col_type.value})
    rows.append({'Column': '__rows__', 'Type': str(table.count_rows())})
    return rows


# ======================================================================
# EXPORT / IMPORT
# ======================================================================

def _write_output(rows: list[RowDict], headers: list[str], filepath: str) -> int:
    ext = filepath.rsplit('.', 1)[-1].lower() if '.' in filepath else ''
    if ext == 'xlsx':
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(headers)
        for row in rows:
            ws.append([_cell_value(row.get(h)) for h in headers])
        wb.save(filepath)
    else:
        delim = '\t' if ext == 'tsv' else ','
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=delim, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(headers)
            for row in rows:
                writer.writerow([_cell_value(row.get(h)) for h in headers])
    return len(rows)


def _cell_value(value: object) -> object:
    return '' if value is None else value


def _infer_types(rows: list[tuple], headers: list[str]) -> dict[str, Types]:
    sample = rows[:100]
    defs: dict[str, Types] = {}
    for i, h in enumerate(headers):
        if not h:
            continue
        col_vals = [row[i] if i < len(row) else None for row in sample]
        non_null = [v for v in col_vals if v is not None]
        if not non_null:
            defs[h] = Types.STRING
        elif all(_is_number(v) for v in non_null):
            defs[h] = Types.NUMBER
        else:
            defs[h] = Types.STRING
    return defs


def execute_import(statement: ImportStatement) -> int:
    try:
        wb = openpyxl.load_workbook(statement.file_path, read_only=True, data_only=True)
    except FileNotFoundError:
        raise EngineError(f'File not found: {statement.file_path}')
    ws = wb.active
    raw_rows: list[tuple] = []
    empty_streak = 0
    for row in ws.iter_rows(values_only=True):
        if any(cell is not None for cell in row):
            raw_rows.append(row)
            empty_streak = 0
        elif raw_rows:
            empty_streak += 1
            if empty_streak > 10:
                break
    wb.close()
    if not raw_rows:
        raise EngineError('Excel file is empty')
    headers = [str(cell) if cell is not None else '' for cell in raw_rows[0]]
    col_defs = (
        {name: Types(t) for name, t in statement.columns}
        if statement.columns
        else _infer_types(raw_rows[1:], headers)
    )
    if not col_defs:
        raise EngineError('No valid columns found in Excel file')

    prepared_rows: list[RowDict] = []
    for row in raw_rows[1:]:
        values: RowDict = {}
        for i, h in enumerate(headers):
            if not h or h not in col_defs:
                continue
            raw_val = row[i] if i < len(row) else None
            if raw_val is None:
                values[h] = None
            elif col_defs[h] == Types.STRING:
                values[h] = str(raw_val)
            else:
                values[h] = _to_decimal(raw_val)
        prepared_rows.append(values)

    storage.create_table(statement.table_name, col_defs)
    try:
        table = _get_table(statement.table_name)
        table.insert_rows(prepared_rows)
    except Exception:
        try:
            storage.drop_table(statement.table_name)
        except Exception:
            pass
        raise
    return len(prepared_rows)
