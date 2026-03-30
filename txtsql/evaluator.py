from decimal import Decimal
from typing import Any

from .parser import (
    Expression, LiteralExpression, ColumnExpression, NullCheckExpression,
    ConditionExpression, LogicalExpression, LogicalOp, ComparisonOp
)
from .storage import RowDict
from .types import Types


def evaluate_where(expression: Expression, table_defs: dict[str, Types]) -> callable[[RowDict], bool]:
    """
    将表达式 AST 求值为布尔函数
    :param expression: 表达式 AST
    :param table_defs: 表结构定义 {col_name: Types}
    :return: 函数(row_dict) -> bool
    """

    def eval_expr(expr: Expression) -> callable[[RowDict], bool]:
        """内部递归求值函数"""
        match expr:
            case LiteralExpression(value):
                def _literal(row: RowDict) -> bool:
                    return _to_bool(value)
                return _literal

            case ColumnExpression(column_name):
                def _column(row: RowDict) -> bool:
                    if column_name not in row:
                        raise ValueError(f'Column does not exist: {column_name}')
                    return _to_bool(row[column_name])
                return _column

            case NullCheckExpression(column_expr, is_null):
                eval_col = eval_expr(column_expr)

                def _null_check(row: RowDict) -> bool:
                    value = eval_col(row)
                    return (value is None) == is_null
                return _null_check

            case ConditionExpression(col_expr, op, literal_expr):
                col_name = col_expr.column_name
                literal_value = literal_expr.value

                if col_name not in table_defs:
                    raise ValueError(f'Column does not exist: {col_name}')

                col_type = table_defs[col_name]
                if not _check_type_compatibility(literal_value, col_type):
                    literal_type = type(literal_value).__name__
                    raise ValueError(
                        f'Type mismatch in WHERE clause: attempting to compare '
                        f'column "{col_name}" of type {col_type.value} with value '
                        f'"{literal_value}" of incompatible type {literal_type}'
                    )

                def _condition(row: RowDict) -> bool:
                    if col_name not in row:
                        raise ValueError(f'Column does not exist: {col_name}')
                    col_value = row[col_name]
                    return _compare(col_value, literal_value, op)
                return _condition

            case LogicalExpression(left, op, right):
                eval_left = eval_expr(left)
                eval_right = eval_expr(right)

                def _logical(row: RowDict) -> bool:
                    left_val = eval_left(row)
                    right_val = eval_right(row)
                    if op == LogicalOp.AND:
                        return left_val and right_val
                    else:
                        return left_val or right_val
                return _logical

            case _:
                raise ValueError(f"Unsupported expression type: {type(expr).__name__}")

        return eval_expr(expression)

    return eval_expr(expression)


def _to_bool(value: Any) -> bool:
    """基础类型转布尔值"""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return value != 0
    if isinstance(value, (str, bytes)):
        return bool(value)
    return bool(value)


def _check_type_compatibility(value: Any, expected_type: Types) -> bool:
    """检查字面值类型是否与列类型兼容"""
    if value is None:
        return True
    match expected_type:
        case Types.STRING:
            return isinstance(value, (str, bool))
        case Types.NUMBER:
            return isinstance(value, (int, float, Decimal))
        case Types.BINARY:
            return isinstance(value, bytes)
    return False


def _compare(left: Any, right: Any, op: ComparisonOp) -> bool:
    """执行比较运算，处理 NULL 和类型转换"""
    if left is None or right is None:
        if op == ComparisonOp.EQ:
            return left is None and right is None
        elif op == ComparisonOp.NE:
            return not (left is None and right is None)
        else:
            return False

    if isinstance(left, (int, float, Decimal)) and isinstance(right, (int, float, Decimal)):
        l_val = Decimal(left)
        r_val = Decimal(right)
    elif isinstance(left, str) and isinstance(right, (str, bool)):
        l_val = str(left)
        r_val = str(right) if isinstance(right, bool) else right
    elif isinstance(left, bytes) and isinstance(right, bytes):
        l_val = left
        r_val = right
    else:
        if op == ComparisonOp.EQ:
            return False
        elif op == ComparisonOp.NE:
            return True
        else:
            raise ValueError(
                f"Cannot compare values of different types: {type(left).__name__} and {type(right).__name__}"
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

    return False
