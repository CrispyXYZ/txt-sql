from collections.abc import Callable
from decimal import Decimal

from .ast import (
    Expression, ColumnExpression, NullCheckExpression,
    ConditionExpression, LogicalExpression, LogicalOp, ComparisonOp,
)
from .exceptions import ColumnNotFoundError, TypeMismatchError
from .types import Types, RowDict


def evaluate_where(expression: Expression, table_defs: dict[str, Types]) -> Callable[[RowDict], bool]:
    """
    Compile an AST expression into a row predicate.
    :param expression: AST expression tree
    :param table_defs: {col_name: Types}
    :return: callable (row_dict) -> bool
    """

    def eval_expr(expr: Expression) -> Callable[[RowDict], bool]:
        match expr:
            case ColumnExpression(column_name):
                def _column(row: RowDict) -> bool:
                    if column_name not in row:
                        raise ColumnNotFoundError(f'Column does not exist: {column_name}')
                    return _to_bool(row[column_name])
                return _column

            case NullCheckExpression(column_expr, is_null):
                col_name = column_expr.column_name

                def _null_check(row: RowDict) -> bool:
                    if col_name not in row:
                        raise ColumnNotFoundError(f'Column does not exist: {col_name}')
                    return (row[col_name] is None) == is_null
                return _null_check

            case ConditionExpression(col_expr, op, literal_expr):
                col_name = col_expr.column_name
                literal_value = literal_expr.value

                if col_name not in table_defs:
                    raise ColumnNotFoundError(f'Column does not exist: {col_name}')

                col_type = table_defs[col_name]
                if not _check_type_compatibility(literal_value, col_type):
                    raise TypeMismatchError(
                        f'Type mismatch in WHERE clause: column "{col_name}" '
                        f'of type {col_type.value} is incompatible with '
                        f'{type(literal_value).__name__} value'
                    )

                def _condition(row: RowDict) -> bool:
                    if col_name not in row:
                        raise ColumnNotFoundError(f'Column does not exist: {col_name}')
                    return _compare(row[col_name], literal_value, op)
                return _condition

            case LogicalExpression(left, op, right):
                eval_left = eval_expr(left)
                eval_right = eval_expr(right)

                def _logical(row: RowDict) -> bool:
                    left_val = eval_left(row)
                    right_val = eval_right(row)
                    return left_val and right_val if op == LogicalOp.AND else left_val or right_val
                return _logical

            case _:
                raise ValueError(f"Unsupported expression type: {type(expr).__name__}")

    return eval_expr(expression)


def _to_bool(value: object) -> bool:
    """Convert arbitrary value to boolean for WHERE filtering."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return value != 0
    if isinstance(value, str):
        return bool(value)
    return bool(value)


def _check_type_compatibility(value: object, expected_type: Types) -> bool:
    """Check if a literal value is compatible with a column type."""
    if value is None:
        return True
    match expected_type:
        case Types.STRING:
            return isinstance(value, (str, bool))
        case Types.NUMBER:
            return isinstance(value, (int, float, Decimal))


def _compare(left: object, right: object, op: ComparisonOp) -> bool:
    """Execute comparison, handling NULL and type coercion."""
    # SQL standard: any comparison with NULL is UNKNOWN → False
    if left is None or right is None:
        return False

    # Coerce numeric types
    if isinstance(left, (int, float, Decimal)) and isinstance(right, (int, float, Decimal)):
        l_val = Decimal(left)
        r_val = Decimal(right)
    elif isinstance(left, str) and isinstance(right, (str, bool)):
        l_val = str(left)
        r_val = str(right) if isinstance(right, bool) else right
    else:
        if op == ComparisonOp.EQ:
            return False
        elif op == ComparisonOp.NE:
            return True
        else:
            raise TypeMismatchError(
                f"Cannot compare values of different types: "
                f"{type(left).__name__} and {type(right).__name__}"
            )

    match op:
        case ComparisonOp.EQ:
            return l_val == r_val
        case ComparisonOp.NE:
            return l_val != r_val
        case ComparisonOp.GT:
            return l_val > r_val
        case ComparisonOp.LT:
            return l_val < r_val
        case ComparisonOp.GE:
            return l_val >= r_val
        case ComparisonOp.LE:
            return l_val <= r_val
