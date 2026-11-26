"""Expression evaluator: AST -> row predicates and computed values."""

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from decimal import Decimal

from .ast import (
    Expression, LiteralExpression, ColumnExpression, BinaryExpression,
    NullCheckExpression, ConditionExpression, LogicalExpression, NotExpression,
    AggregateExpression, InExpression, InListExpression, AnyAllExpression,
    ExistsExpression, BetweenExpression, LikeExpression, ScalarSubqueryExpression,
    LogicalOp, ComparisonOp, ArithmeticOp,
)
from .exceptions import ColumnNotFoundError, TypeMismatchError, EngineError
from .types import Types, RowDict, DataValue


# ======================================================================
# Context & Public API
# ======================================================================

SubqueryExecutor = Callable[[Expression, RowDict | None], list[RowDict]] | None
AggregateEvaluator = Callable[[AggregateExpression], DataValue] | None


@dataclass(frozen=True, slots=True)
class EvalContext:
    """Immutable evaluation context passed through expression evaluation."""
    table_defs: dict[str, Types] | None = None
    subquery_exec: SubqueryExecutor = None
    agg_eval: AggregateEvaluator = None
    outer_row: RowDict | None = None
    valid_single_qualifier: str | None = None

    def with_agg(self, agg_eval: AggregateEvaluator) -> 'EvalContext':
        return replace(self, agg_eval=agg_eval)

    def with_outer(self, outer_row: RowDict | None) -> 'EvalContext':
        return replace(self, outer_row=outer_row)


def evaluate_expression(
    expr: Expression,
    row: RowDict,
    ctx: EvalContext | None = None,
) -> DataValue | bool:
    """Recursively evaluate any expression against a single row (with SQL 3VL)."""
    c = ctx or EvalContext()
    match expr:
        case LiteralExpression(value):
            return value

        case ColumnExpression(column_name, table):
            return resolve_column(
                row, column_name, table,
                outer_row=c.outer_row,
                valid_single_qualifier=c.valid_single_qualifier,
            )

        case AggregateExpression():
            if c.agg_eval is None:
                raise EngineError("Aggregate expression is not valid in this context")
            return c.agg_eval(expr)

        case BinaryExpression(left, op, right):
            lv = evaluate_expression(left, row, c)
            rv = evaluate_expression(right, row, c)
            return _apply_arithmetic(lv, op, rv)

        case NullCheckExpression(inner_expr, is_null):
            val = evaluate_expression(inner_expr, row, c)
            return (val is None) == is_null

        case NotExpression(inner_expr):
            val = _to_tristate(evaluate_expression(inner_expr, row, c))
            return None if val is None else not val

        case ConditionExpression(left, op, right):
            lv = evaluate_expression(left, row, c)
            rv = evaluate_expression(right, row, c)
            return _compare(lv, rv, op)

        case LogicalExpression(left, op, right):
            lval = _to_tristate(evaluate_expression(left, row, c))
            if op == LogicalOp.AND:
                if lval is False:
                    return False
                rval = _to_tristate(evaluate_expression(right, row, c))
                return _tristate_and(lval, rval)
            else:  # OR
                if lval is True:
                    return True
                rval = _to_tristate(evaluate_expression(right, row, c))
                if rval is True:
                    return True
                if lval is False and rval is False:
                    return False
                return None

        case BetweenExpression(inner_expr, low_expr, high_expr, negated):
            val = evaluate_expression(inner_expr, row, c)
            low_val = evaluate_expression(low_expr, row, c)
            high_val = evaluate_expression(high_expr, row, c)
            ge = _compare(val, low_val, ComparisonOp.GE)
            le = _compare(val, high_val, ComparisonOp.LE)
            in_range = _tristate_and(ge, le)
            if in_range is None:
                return None
            return (not in_range) if negated else in_range

        case LikeExpression(inner_expr, pattern_expr, negated):
            val = evaluate_expression(inner_expr, row, c)
            pat = evaluate_expression(pattern_expr, row, c)
            if val is None or pat is None:
                return None
            if not isinstance(val, str) or not isinstance(pat, str):
                raise TypeMismatchError(
                    f"LIKE requires STRING types, got {type(val).__name__} and {type(pat).__name__}")
            matched = _like_match(val, pat)
            return (not matched) if negated else matched

        case InListExpression(inner_expr, items, not_in):
            val = evaluate_expression(inner_expr, row, c)
            candidates = [evaluate_expression(item, row, c) for item in items]
            return _eval_in_values(val, candidates, not_in)

        case InExpression(inner_expr, subquery_stmt, not_in):
            if c.subquery_exec is None:
                raise EngineError("Subquery execution not available")
            sub_rows = c.subquery_exec(
                subquery_stmt, _build_outer_row(row, c.outer_row, c.valid_single_qualifier))
            if not sub_rows:
                return True if not_in else False
            val = evaluate_expression(inner_expr, row, c)
            candidates: list[object] = []
            for r in sub_rows:
                if len(r) != 1:
                    raise EngineError("Subquery in IN must return exactly one column")
                candidates.append(next(iter(r.values())))
            return _eval_in_values(val, candidates, not_in)

        case AnyAllExpression(inner_expr, op, quantifier, subquery_stmt):
            if c.subquery_exec is None:
                raise EngineError("Subquery execution not available")
            sub_rows = c.subquery_exec(
                subquery_stmt, _build_outer_row(row, c.outer_row, c.valid_single_qualifier))
            if not sub_rows:
                return False if quantifier == 'ANY' else True
            val = evaluate_expression(inner_expr, row, c)
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
            if c.subquery_exec is None:
                raise EngineError("Subquery execution not available")
            sub_rows = c.subquery_exec(
                subquery_stmt, _build_outer_row(row, c.outer_row, c.valid_single_qualifier))
            exists = len(sub_rows) > 0
            return not exists if negated else exists

        case ScalarSubqueryExpression(subquery_stmt):
            if c.subquery_exec is None:
                raise EngineError("Subquery execution not available")
            sub_rows = c.subquery_exec(
                subquery_stmt, _build_outer_row(row, c.outer_row, c.valid_single_qualifier))
            if not sub_rows:
                return None
            if len(sub_rows) > 1:
                raise EngineError("Scalar subquery returned more than one row")
            first_row = sub_rows[0]
            if len(first_row) != 1:
                raise EngineError("Scalar subquery must return exactly one column")
            return next(iter(first_row.values()))

        case _:
            raise ValueError(f"Unsupported expression type: {type(expr).__name__}")


def evaluate_where(
    expression: Expression,
    ctx: EvalContext | None = None,
) -> Callable[[RowDict], bool]:
    """Compile a WHERE/HAVING expression into a row predicate."""
    c = ctx or EvalContext()

    def predicate(row: RowDict) -> bool:
        return to_bool(evaluate_expression(expression, row, c))

    return predicate


def resolve_column_name(expr: Expression) -> str | None:
    """Extract a readable name from a SELECT expression for header display."""
    match expr:
        case ColumnExpression(column_name, _):
            return column_name
        case AggregateExpression(func, col, distinct):
            arg = '*' if col is None else (f'{col.table}.{col.column_name}' if col.table else col.column_name)
            prefix = 'DISTINCT ' if distinct else ''
            return f'{func.value}({prefix}{arg})'
        case BinaryExpression(left, op, right):
            ln = resolve_column_name(left)
            rn = resolve_column_name(right)
            return f"{ln or '?'}{op.value}{rn or '?'}"
        case LiteralExpression(value):
            return str(value) if value is not None else 'NULL'
    return None


def resolve_column(
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
            return resolve_column(outer_row, column_name, table)
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
        return resolve_column(outer_row, column_name, None)

    raise ColumnNotFoundError(f'Column does not exist: {column_name}')


def is_number(value: object) -> bool:
    """Return whether value is treated as a numeric value (includes bool 0/1)."""
    return isinstance(value, (int, float, Decimal, bool))


def to_decimal(value: object) -> Decimal:
    """Convert numeric value (including bool 0/1) to Decimal."""
    if isinstance(value, bool):
        return Decimal(1) if value else Decimal(0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeMismatchError(f'Expected NUMBER value, got {type(value).__name__}')


def to_bool(value: object) -> bool:
    """Collapse SQL three-valued logic to Python bool (UNKNOWN -> False)."""
    return _to_tristate(value) is True


# ======================================================================
# Internal helpers
# ======================================================================

def _eval_in_values(val: object, candidates: Iterable[object], not_in: bool) -> bool | None:
    """Shared SQL three-valued logic for IN / NOT IN against lists and subqueries."""
    has_null = False
    for cand in candidates:
        cmp_res = _compare(val, cand, ComparisonOp.EQ)
        if cmp_res is True:
            return False if not_in else True
        if cmp_res is None:
            has_null = True
    if has_null:
        return None
    return True if not_in else False


def _tristate_and(lval: bool | None, rval: bool | None) -> bool | None:
    if lval is False or rval is False:
        return False
    if lval is True and rval is True:
        return True
    return None


def _like_match(value: str, pattern: str) -> bool:
    regex_parts = [
        '.*' if ch == '%' else '.' if ch == '_' else re.escape(ch)
        for ch in pattern
    ]
    return re.fullmatch(''.join(regex_parts), value, re.DOTALL) is not None


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


def _apply_arithmetic(left: object, op: ArithmeticOp, right: object) -> DataValue:
    """Apply arithmetic operation, treating NULL as NULL propagation."""
    if left is None or right is None:
        return None
    if not is_number(left) or not is_number(right):
        raise TypeMismatchError(
            f"Arithmetic requires NUMBER types, got {type(left).__name__} and {type(right).__name__}")
    lv, rv = to_decimal(left), to_decimal(right)
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

    if is_number(left) and is_number(right):
        l_val = to_decimal(left)
        r_val = to_decimal(right)
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
