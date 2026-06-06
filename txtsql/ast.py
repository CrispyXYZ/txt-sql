"""AST node definitions for TxtSQL statements and expressions."""

from dataclasses import dataclass
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
    """Column reference: name, score."""
    column_name: str


@dataclass
class NullCheckExpression(Expression):
    """NULL check: column IS [NOT] NULL."""
    column: ColumnExpression
    is_null: bool  # True for IS NULL, False for IS NOT NULL


@dataclass
class ConditionExpression(Expression):
    """Comparison: column op literal (e.g. age > 18)."""
    column: ColumnExpression
    op: ComparisonOp
    literal: LiteralExpression


@dataclass
class LogicalExpression(Expression):
    """Logical combination: left AND/OR right."""
    left: Expression
    op: LogicalOp
    right: Expression


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


# ---------------------------------------------------------------------------
# Top-level statements
# ---------------------------------------------------------------------------

@dataclass(slots=True, frozen=True)
class DropTable:
    table_name: str


@dataclass(slots=True, frozen=True)
class CreateTable:
    table_name: str
    columns: list[tuple[str, str]]


@dataclass(slots=True, frozen=True)
class InsertValues:
    table_name: str
    columns: list[str] | None
    values: list[list[LiteralValue]]


@dataclass(slots=True, frozen=True)
class DeleteStatement:
    table_name: str
    where_clause: WhereClause | None


@dataclass(slots=True, frozen=True)
class SelectStatement:
    table_name: str
    columns: list[str] | None  # None means SELECT *
    aggregates: list[AggregateColumn]
    distinct: bool
    where_clause: WhereClause | None
    group_by: list[str] | None
    having: WhereClause | None
    order_by: list[tuple[str, bool]] | None  # (column, desc)
    limit: int | None
    offset: int
    output_file: str | None  # INTO OUTFILE path; None for normal SELECT


@dataclass(slots=True, frozen=True)
class UpdateStatement:
    table_name: str
    set_clauses: list[tuple[str, LiteralValue]]  # [(column, value), ...]
    where_clause: WhereClause | None


@dataclass(slots=True, frozen=True)
class ImportStatement:
    """IMPORT table_name FROM 'file.xlsx';"""
    table_name: str
    file_path: str
    columns: list[tuple[str, str]] | None  # None = auto-infer types from data
