"""Structured execution result objects for CLI, GUI, and Python callers."""

from dataclasses import dataclass, field
from enum import StrEnum

from .types import RowDict


class StatementKind(StrEnum):
    SELECT = 'SELECT'
    UNION = 'UNION'
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    DDL = 'DDL'
    SYSTEM = 'SYSTEM'
    IMPORT = 'IMPORT'
    EXPORT = 'EXPORT'


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Unified structured result returned by Database.execute()."""
    kind: StatementKind
    columns: tuple[str, ...] = ()
    rows: list[RowDict] = field(default_factory=list)
    affected_rows: int | None = None
    message: str = 'Query OK.'

    @property
    def is_tabular(self) -> bool:
        """Whether this result represents tabular data (even if 0 rows)."""
        return self.kind in (StatementKind.SELECT, StatementKind.UNION, StatementKind.SYSTEM)

    def to_legacy(self) -> list[RowDict] | int | None:
        """Convert to the legacy execute_sql() return value for backward compatibility."""
        if self.is_tabular:
            return self.rows
        if self.kind in (
            StatementKind.UPDATE,
            StatementKind.DELETE,
            StatementKind.IMPORT,
            StatementKind.EXPORT,
        ):
            return self.affected_rows or 0
        return None
