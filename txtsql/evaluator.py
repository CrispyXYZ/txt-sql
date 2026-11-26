"""Expression evaluator: AST -> row predicates and computed values."""

from collections.abc import Callable
from decimal import Decimal

from .ast import (
    Expression, LiteralExpression, ColumnExpression, BinaryExpression,
    NullCheckExpression, ConditionExpression, LogicalExpression, AggregateExpression,
    InExpression, AnyAllExpression, ExistsExpression,
    LogicalOp, ComparisonOp, ArithmeticOp,
)
from .exceptions import ColumnNotFoundError, TypeMismatchError, EngineError
from .types import Types, RowDict, DataValue


# ======================================================================
# Public API
# ======================================================================

SubqueryExecutor = Callable[[Expression, RowDict | None], list[RowDict]] | None
AggregateEvaluator = Callable[[AggregateExpression], DataValue] | None


def evaluate_expression(
    expr: Expression,
    row: RowDict,
    table_defs: dict[str, Types] | None = None,
    subquery_exec: SubqueryExecutor = None,
    agg_eval: AggregateEvaluator = None,
    outer_row: RowDict | None = None,
    valid_single_qualifier: str | None = None,
) -> DataValue | bool:
    """Recursively evaluate any expression against a single row (with SQL 3VL)."""
    match expr:
        case LiteralExpression(value):
            return value

        case ColumnExpression(column_name, table):
            return _resolve_column(
                row, column_name, table,
                outer_row=outer_row,
                valid_single_qualifier=valid_single_qualifier,
            )

        case AggregateExpression():
            if agg_eval is None:
                raise EngineError("Aggregate expression is not valid in this context")
            return agg_eval(expr)

        case BinaryExpression(left, op, right):
            lv = evaluate_expression(
                left, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
            rv = evaluate_expression(
                right, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
            return _apply_arithmetic(lv, op, rv)

        case NullCheckExpression(inner_expr, is_null):
            val = evaluate_expression(
                inner_expr, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
            return (val is None) == is_null

        case ConditionExpression(left, op, right):
            lv = evaluate_expression(
                left, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
            rv = evaluate_expression(
                right, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
            return _compare(lv, rv, op)

        case LogicalExpression(left, op, right):
            lval = _to_tristate(evaluate_expression(
                left, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier))
            if op == LogicalOp.AND:
                if lval is False:
                    return False
                rval = _to_tristate(evaluate_expression(
                    right, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier))
                if rval is False:
                    return False
                if lval is True and rval is True:
                    return True
                return None
            else:  # OR
                if lval is True:
                    return True
                rval = _to_tristate(evaluate_expression(
                    right, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier))
                if rval is True:
                    return True
                if lval is False and rval is False:
                    return False
                return None

        case InExpression(inner_expr, subquery_stmt, not_in):
            if subquery_exec is None:
                raise EngineError("Subquery execution not available")
            sub_rows = subquery_exec(subquery_stmt, _build_outer_row(row, outer_row, valid_single_qualifier))
            if not sub_rows:
                return True if not_in else False
            val = evaluate_expression(
                inner_expr, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
            has_null = False
            for r in sub_rows:
                if len(r) != 1:
                    raise EngineError("Subquery in IN must return exactly one column")
                sub_val = next(iter(r.values()))
                cmp_res = _compare(val, sub_val, ComparisonOp.EQ)
                if cmp_res is True:
                    return False if not_in else True
                if cmp_res is None:
                    has_null = True
            if has_null:
                return None
            return True if not_in else False

        case AnyAllExpression(inner_expr, op, quantifier, subquery_stmt):
            if subquery_exec is None:
                raise EngineError("Subquery execution not available")
            sub_rows = subquery_exec(subquery_stmt, _build_outer_row(row, outer_row, valid_single_qualifier))
            if not sub_rows:
                return False if quantifier == 'ANY' else True
            val = evaluate_expression(
                inner_expr, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
            has_null = False
            if quantifier == 'ANY':
                for r in sub_rows:
                    if len(r) != 1:
                        raise EngineError("Subquery in ANY must return exactly one column")
                    cmp_res = _compare(val, next(iter(r.values())), op)
                    if cmp_res is True:
                        return True
                    if cmp_res is None:
                        has_null = True
                return None if has_null else False
            else:  # ALL
                for r in sub_rows:
                    if len(r) != 1:
                        raise EngineError("Subquery in ALL must return exactly one column")
                    cmp_res = _compare(val, next(iter(r.values())), op)
                    if cmp_res is False:
                        return False
                    if cmp_res is None:
                        has_null = True
                return None if has_null else True

        case ExistsExpression(subquery_stmt, negated):
            if subquery_exec is None:
                raise EngineError("Subquery execution not available")
            sub_rows = subquery_exec(subquery_stmt, _build_outer_row(row, outer_row, valid_single_qualifier))
            exists = len(sub_rows) > 0
            return not exists if negated else exists

        case _:
            raise ValueError(f"Unsupported expression type: {type(expr).__name__}")


def evaluate_where(
    expression: Expression,
    table_defs: dict[str, Types],
    subquery_exec: SubqueryExecutor = None,
    agg_eval: AggregateEvaluator = None,
    outer_row: RowDict | None = None,
    valid_single_qualifier: str | None = None,
) -> Callable[[RowDict], bool]:
    """Compile a WHERE/HAVING expression into a row predicate."""
    def predicate(row: RowDict) -> bool:
        val = evaluate_expression(
            expression, row, table_defs, subquery_exec, agg_eval, outer_row, valid_single_qualifier)
        return _to_bool(val)
    return predicate


def resolve_column_name(expr: Expression) -> str | None:
    """Extract a readable name from a SELECT expression for header display."""
    match expr:
        case ColumnExpression(column_name, _):
            return column_name
        case AggregateExpression(func, col):
            arg = '*' if col is None else (f'{col.table}.{col.column_name}' if col.table else col.column_name)
            return f'{func.value}({arg})'
        case BinaryExpression(left, op, right):
            ln = resolve_column_name(left)
            rn = resolve_column_name(right)
            return f"{ln or '?'}{op.value}{rn or '?'}"
        case LiteralExpression(value):
            return str(value) if value is not None else 'NULL'
    return None


# ======================================================================
# Internal helpers
# ======================================================================

def _build_outer_row(
    row: RowDict,
    outer_row: RowDict | None,
    valid_single_qualifier: str | None,
) -> RowDict:
    """Build combined outer row context for nested/correlated subqueries."""
    merged: RowDict = dict(outer_row) if outer_row else {}
    if valid_single_qualifier is not None and not any('.' in k for k in row):
        for k, v in row.items():
            merged[f"{valid_single_qualifier}.{k}"] = v
    else:
        merged.update(row)
    return merged


def _resolve_column(
    row: RowDict,
    column_name: str,
    table: str | None = None,
    outer_row: RowDict | None = None,
    valid_single_qualifier: str | None = None,
) -> DataValue:
    """Resolve a column reference against a possibly table-prefixed row."""
    if table is not None:
        key = f"{table}.{column_name}"
        if key in row:
            return row[key]
        # Single-table rows are stored unprefixed; verify qualifier matches table/alias.
        if column_name in row and not any('.' in k for k in row):
            if valid_single_qualifier is None or table == valid_single_qualifier:
                return row[column_name]
        if outer_row is not None:
            return _resolve_column(outer_row, column_name, table)
        raise ColumnNotFoundError(f'Column does not exist: {key}')

    # Unqualified column lookup
    if column_name in row:
        return row[column_name]

    candidates = [value for key, value in row.items() if key.endswith(f".{column_name}")]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise EngineError(f'Ambiguous column name: {column_name}')

    if outer_row is not None:
        return _resolve_column(outer_row, column_name, None)

    raise ColumnNotFoundError(f'Column does not exist: {column_name}')


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float, Decimal, bool))


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, bool):
        return Decimal(1) if value else Decimal(0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeMismatchError(f'Expected NUMBER value, got {type(value).__name__}')


def _to_tristate(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return value != 0
    if isinstance(value, str):
        return bool(value)
    return bool(value)


def _to_bool(value: object) -> bool:
    return _to_tristate(value) is True


def _apply_arithmetic(left: object, op: ArithmeticOp, right: object) -> DataValue:
    """Apply arithmetic operation, treating NULL as NULL propagation."""
    if left is None or right is None:
        return None
    if not _is_number(left) or not _is_number(right):
        raise TypeMismatchError(
            f"Arithmetic requires NUMBER types, got {type(left).__name__} and {type(right).__name__}")
    lv, rv = _to_decimal(left), _to_decimal(right)
    match op:
        case ArithmeticOp.ADD: return lv + rv
        case ArithmeticOp.SUB: return lv - rv
        case ArithmeticOp.MUL: return lv * rv
        case ArithmeticOp.DIV:
            if rv == 0:
                raise TypeMismatchError("Division by zero")
            return lv / rv


def _compare(left: object, right: object, op: ComparisonOp) -> bool | None:
    """Execute comparison using SQL three-valued logic (NULL yields None/UNKNOWN)."""
    if left is None or right is None:
        return None

    if _is_number(left) and _is_number(right):
        l_val = _to_decimal(left)
        r_val = _to_decimal(right)
    elif isinstance(left, str) and isinstance(right, str):
        l_val = left
        r_val = right
    else:
        raise TypeMismatchError(f"Cannot compare {type(left).__name__} and {type(right).__name__}")

    match op:
        case ComparisonOp.EQ: return l_val == r_val
        case ComparisonOp.NE: return l_val != r_val
        case ComparisonOp.GT: return l_val > r_val
        case ComparisonOp.LT: return l_val < r_val
        case ComparisonOp.GE: return l_val >= r_val
        case ComparisonOp.LE: return l_val <= r_val
