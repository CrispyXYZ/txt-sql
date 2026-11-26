"""Top-level orchestration: Database session object and execute_sql helper."""

import os
from pathlib import Path

from .executor import QueryExecutor
from .lexer import Lexer
from .parser import Parser
from .result import QueryResult
from .storage import StorageEngine, Table
from .types import RowDict


class Database:
    """Directory-scoped TxtSQL database session (ready for CLI, GUI, and tests)."""

    def __init__(
        self,
        data_dir: str | os.PathLike[str] = '.',
        enable_optimizations: bool = True,
    ) -> None:
        self.storage = StorageEngine(data_dir)
        self.executor = QueryExecutor(self.storage, enable_optimizations=enable_optimizations)

    @property
    def data_dir(self) -> Path:
        return self.storage.data_dir

    def execute(self, sql: str) -> QueryResult:
        """Lex, parse, and execute a SQL statement, returning a structured QueryResult."""
        tokens = Lexer(sql).tokenize()
        statement = Parser(tokens).parse()
        return self.executor.execute(statement, tokens=tokens)

    def list_tables(self) -> list[tuple[str, int, dict[str, str]]]:
        """Return metadata for all tables in this database directory."""
        return self.storage.list_tables()

    def get_table(self, name: str) -> Table | None:
        """Return a Table handle for the named table, or None if missing."""
        return self.storage.get_table(name)


def execute_sql(sql: str, data_dir: str | os.PathLike[str] = '.') -> list[RowDict] | int | None:
    """Execute a SQL statement and return the legacy list/int/None representation."""
    return Database(data_dir).execute(sql).to_legacy()
