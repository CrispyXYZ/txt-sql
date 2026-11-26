"""AST node definitions for TxtSQL statements and expressions."""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

type LiteralValue = str | Decimal | None | bool


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class LogicalOp(Enum):
    AND = 'and'
    OR = 'or'


class AggFunc(Enum):
    COUNT = 'COUNT'
    SUM = 'SUM'
    AVG = 'AVG'
    MIN = 'MIN'
    MAX = 'MAX'


class ComparisonOp(Enum):
    EQ = '='
    NE = '<>'
    GT = '>'
    LT = '<'
    GE = '>='
    LE = '<='


class ArithmeticOp(Enum):
    ADD = '+'
    SUB = '-'
    MUL = '*'
    DIV = '/'


class JoinType(Enum):
    INNER = 'INNER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    FULL = 'FULL'
    CROSS = 'CROSS'


# ---------------------------------------------------------------------------
# Expression nodes
# ---------------------------------------------------------------------------

class Expression:
    """Base class for all expressions."""
    pass


@dataclass
class LiteralExpression(Expression):
    """Literal: 1, 'hello', NULL, TRUE, FALSE."""
    value: LiteralValue


@dataclass
class ColumnExpression(Expression):
    """Column reference: col or table.col."""
    column_name: str
    table: str | None = None  # None = no table prefix


@dataclass
class BinaryExpression(Expression):
    """Arithmetic: left op right (e.g. a + b, 2 * price)."""
    left: Expression
    op: ArithmeticOp
    right: Expression


@dataclass
class NullCheckExpression(Expression):
    """NULL check: expr IS [NOT] NULL."""
    expr: Expression
    is_null: bool  # True for IS NULL, False for IS NOT NULL


@dataclass
class ConditionExpression(Expression):
    """Comparison: left op right (e.g. col > 5, a + b = c)."""
    left: Expression
    op: ComparisonOp
    right: Expression


@dataclass
class LogicalExpression(Expression):
    """Logical combination: left AND/OR right."""
    left: Expression
    op: LogicalOp
    right: Expression


@dataclass
class AggregateExpression(Expression):
    """Aggregate function call: COUNT(*), COUNT(DISTINCT col), SUM(col), SUM(t.col)."""
    func: AggFunc
    column: ColumnExpression | None  # None for COUNT(*)
    distinct: bool = False


@dataclass
class NotExpression(Expression):
    """Logical negation: NOT expr."""
    expr: Expression


@dataclass
class InListExpression(Expression):
    """expr IN (e1, e2, ...) or expr NOT IN (e1, e2, ...)."""
    expr: Expression
    items: list[Expression]
    not_in: bool = False


@dataclass
class BetweenExpression(Expression):
    """expr [NOT] BETWEEN low AND high."""
    expr: Expression
    low: Expression
    high: Expression
    negated: bool = False


@dataclass
class LikeExpression(Expression):
    """expr [NOT] LIKE pattern."""
    expr: Expression
    pattern: Expression
    negated: bool = False


@dataclass
class ScalarSubqueryExpression(Expression):
    """(SELECT ...) used as a scalar value expression."""
    subquery: 'SelectStatement | UnionStatement'


@dataclass
class StarExpression(Expression):
    """* or table.* wildcard in SELECT list."""
    table: str | None = None


@dataclass
class InExpression(Expression):
    """expr IN (subquery) or expr NOT IN (subquery)."""
    expr: Expression
    subquery: 'SelectStatement | UnionStatement'
    not_in: bool = False


@dataclass
class AnyAllExpression(Expression):
    """expr op ANY|ALL (subquery)."""
    expr: Expression
    op: ComparisonOp
    quantifier: str  # 'ANY' or 'ALL'
    subquery: 'SelectStatement | UnionStatement'


@dataclass
class ExistsExpression(Expression):
    """[NOT] EXISTS (subquery)."""
    subquery: 'SelectStatement | UnionStatement'
    negated: bool = False


# ---------------------------------------------------------------------------
# Clauses
# ---------------------------------------------------------------------------

@dataclass
class WhereClause:
    """WHERE clause wrapper."""
    expression: Expression


# ---------------------------------------------------------------------------
# Select / aggregate helpers
# ---------------------------------------------------------------------------

@dataclass(slots=True, frozen=True)
class AggregateColumn:
    """A single aggregate function call in a SELECT list."""
    func: AggFunc
    column: str | None  # None for COUNT(*)
    alias: str


@dataclass
class SelectExpression:
    """A single item in the SELECT list."""
    expr: Expression
    alias: str | None  # None = no alias; resolved name = column_name or alias


@dataclass(slots=True, frozen=True)
class TableSource:
    """A table reference in the FROM clause."""
    name: str          # table name
    alias: str | None  # None means use table name as alias


@dataclass(slots=True, frozen=True)
class JoinClause:
    join_type: JoinType
    source: TableSource
    condition: Expression | None = None  # ON condition
    using_columns: tuple[str, ...] | None = None  # USING (col, ...)
    natural: bool = False


# ---------------------------------------------------------------------------
# Top-level statements
# ---------------------------------------------------------------------------

@dataclass(slots=True, frozen=True)
class DropTable:
    table_name: str
    if_exists: bool = False


@dataclass(slots=True, frozen=True)
class CreateTable:
    table_name: str
    columns: list[tuple[str, str]]
    if_not_exists: bool = False


@dataclass(slots=True, frozen=True)
class InsertValues:
    table_name: str
    columns: list[str] | None
    values: list[list[LiteralValue]]


@dataclass(slots=True, frozen=True)
class InsertSelect:
    """INSERT INTO table [(cols)] SELECT ..."""
    table_name: str
    columns: list[str] | None
    query: 'SelectStatement | UnionStatement'


@dataclass(slots=True, frozen=True)
class DeleteStatement:
    table_name: str
    where_clause: WhereClause | None


@dataclass(slots=True, frozen=True)
class SelectStatement:
    sources: list[TableSource]
    joins: list[JoinClause]
    columns: list[SelectExpression] | None  # None means SELECT *
    aggregates: list[AggregateColumn] = field(default_factory=list)
    distinct: bool = False
    where_clause: WhereClause | None = None
    group_by: list[ColumnExpression] | None = None
    having: WhereClause | None = None
    order_by: list[tuple[ColumnExpression, bool]] | None = None  # (column, desc)
    limit: int | None = None
    offset: int = 0
    output_file: str | None = None  # INTO OUTFILE path; None for normal SELECT


@dataclass(slots=True, frozen=True)
class UnionStatement:
    """SELECT ... UNION [ALL] SELECT ..."""
    branches: list[tuple[SelectStatement, bool]]  # [(first, False), (next, is_all), ...]
    order_by: list[tuple[ColumnExpression, bool]] | None = None
    limit: int | None = None
    offset: int = 0
    output_file: str | None = None


@dataclass(slots=True, frozen=True)
class UpdateStatement:
    table_name: str
    set_clauses: list[tuple[str, Expression]]  # [(column, expr), ...]
    where_clause: WhereClause | None


@dataclass(slots=True, frozen=True)
class ImportStatement:
    """IMPORT table_name FROM 'file.xlsx';"""
    table_name: str
    file_path: str
    columns: list[tuple[str, str]] | None  # None = auto-infer types from data


@dataclass(slots=True, frozen=True)
class ShowTables:
    """SHOW TABLES;"""
    pass


@dataclass(slots=True, frozen=True)
class DescribeTable:
    """DESCRIBE table_name;"""
    table_name: str


@dataclass(slots=True, frozen=True)
class CreateIndex:
    """CREATE INDEX [IF NOT EXISTS] index_name ON table_name (column_name);"""
    index_name: str
    table_name: str
    column_name: str
    if_not_exists: bool = False


@dataclass(slots=True, frozen=True)
class DropIndex:
    """DROP INDEX [IF EXISTS] index_name;"""
    index_name: str
    if_exists: bool = False


@dataclass(slots=True, frozen=True)
class ShowIndexes:
    """SHOW INDEXES [FROM table_name];"""
    table_name: str | None = None


@dataclass(slots=True, frozen=True)
class ExplainStatement:
    """EXPLAIN [ANALYZE] <SELECT | UNION>;"""
    query: 'SelectStatement | UnionStatement'
    analyze: bool = False


@dataclass(slots=True, frozen=True)
class CheckTable:
    """CHECK TABLE table_name;"""
    table_name: str


@dataclass(slots=True, frozen=True)
class BeginTransaction:
    """BEGIN [TRANSACTION];"""
    pass


@dataclass(slots=True, frozen=True)
class CommitTransaction:
    """COMMIT [TRANSACTION];"""
    pass


@dataclass(slots=True, frozen=True)
class RollbackTransaction:
    """ROLLBACK [TRANSACTION];"""
    pass


# ---------------------------------------------------------------------------
# Shared AST traversal helpers
# ---------------------------------------------------------------------------

def contains_aggregate(expr: Expression) -> bool:
    """Return True if expr contains an AggregateExpression outside nested subqueries."""
    match expr:
        case AggregateExpression():
            return True
        case BinaryExpression(left, _, right) | ConditionExpression(left, _, right) | LogicalExpression(left, _, right):
            return contains_aggregate(left) or contains_aggregate(right)
        case NullCheckExpression(inner, _) | NotExpression(inner):
            return contains_aggregate(inner)
        case InExpression(inner, _, _) | AnyAllExpression(inner, _, _, _):
            return contains_aggregate(inner)
        case InListExpression(inner, items, _):
            return contains_aggregate(inner) or any(contains_aggregate(item) for item in items)
        case BetweenExpression(inner, low, high, _):
            return contains_aggregate(inner) or contains_aggregate(low) or contains_aggregate(high)
        case LikeExpression(inner, pattern, _):
            return contains_aggregate(inner) or contains_aggregate(pattern)
        case _:
            return False


def collect_non_agg_columns(expr: Expression) -> list[ColumnExpression]:
    """Collect ColumnExpression nodes outside aggregate calls and nested subqueries."""
    match expr:
        case AggregateExpression():
            return []
        case ColumnExpression():
            return [expr]
        case BinaryExpression(left, _, right) | ConditionExpression(left, _, right) | LogicalExpression(left, _, right):
            return collect_non_agg_columns(left) + collect_non_agg_columns(right)
        case NullCheckExpression(inner, _) | NotExpression(inner):
            return collect_non_agg_columns(inner)
        case InExpression(inner, _, _) | AnyAllExpression(inner, _, _, _):
            return collect_non_agg_columns(inner)
        case InListExpression(inner, items, _):
            cols = collect_non_agg_columns(inner)
            for item in items:
                cols.extend(collect_non_agg_columns(item))
            return cols
        case BetweenExpression(inner, low, high, _):
            return (
                collect_non_agg_columns(inner)
                + collect_non_agg_columns(low)
                + collect_non_agg_columns(high)
            )
        case LikeExpression(inner, pattern, _):
            return collect_non_agg_columns(inner) + collect_non_agg_columns(pattern)
        case _:
            return []
