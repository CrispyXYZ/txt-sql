from __future__ import annotations

import csv
import logging
import os
import tempfile
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

from .exceptions import (
    ColumnNotFoundError,
    EngineError,
    TableAlreadyExistsError,
    TableNotFoundError,
    TypeMismatchError,
)
from .index import ColumnIndex, IndexMeta
from .types import Types, DataValue, RowDict

_log = logging.getLogger(__name__)

METADATA_FILENAME = 'metadata.txt'
INDEXES_FILENAME = 'indexes.txt'
_NULL_SENTINEL = '\\N'


# Sentinel object used internally to mean "leave this column unchanged" during UPDATE
class _Unchanged:
    pass


_UNCHANGED = _Unchanged()


# ============================================================================
# Serialization helpers
# ============================================================================

def _coerce_number(value: object) -> Decimal:
    """Validate and convert a Python value to Decimal for a NUMBER column."""
    if isinstance(value, bool):
        return Decimal(1) if value else Decimal(0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeMismatchError(f'Expected NUMBER value, got {type(value).__name__}')


def _data_to_string(value: object, type_def: Types) -> str:
    """Convert data value to string for TSV storage.

    None (SQL NULL) -> '\\N' sentinel; backslashes in strings are escaped as '\\\\'
    so empty string '' and literal '\\N' round-trip without ambiguity.
    """
    if value is None:
        return _NULL_SENTINEL
    match type_def:
        case Types.NUMBER:
            return str(_coerce_number(value))
        case Types.STRING:
            if not isinstance(value, str):
                raise TypeMismatchError(f'Expected STRING value, got {type(value).__name__}')
            return value.replace('\\', '\\\\')


def _string_to_data(string: str, type_def: Types) -> DataValue:
    """Convert TSV field string to typed data value. '\\N' -> None (SQL NULL)."""
    if string == _NULL_SENTINEL:
        return None
    match type_def:
        case Types.NUMBER:
            # Backward-compatible with legacy TSV files where empty fields denoted NULL.
            if string == '':
                return None
            return Decimal(string)
        case Types.STRING:
            return string.replace('\\\\', '\\')


# ============================================================================
# CSV dialect: tab-separated, all fields quoted so \t \n " \N round-trip cleanly
# ============================================================================

_CSV_WRITE_KWARGS = dict(delimiter='\t', quoting=csv.QUOTE_ALL, lineterminator='\n')
_CSV_READ_KWARGS = dict(delimiter='\t')


# ============================================================================
# StorageEngine (directory-scoped)
# ============================================================================

class StorageEngine:
    """Directory-scoped TSV storage manager for metadata.txt and <table>.txt files."""

    def __init__(self, data_dir: str | os.PathLike[str] = '.') -> None:
        self.data_dir = Path(data_dir)
        self.metadata_path = self.data_dir / METADATA_FILENAME
        self.indexes_path = self.data_dir / INDEXES_FILENAME
        self._row_cache: dict[str, tuple[tuple[int, int] | None, list[RowDict]]] = {}
        self._index_cache: dict[str, ColumnIndex] = {}

    # -- Cache & file-change awareness ---------------------------------

    @staticmethod
    def _file_fingerprint(path: Path) -> tuple[int, int] | None:
        try:
            st = path.stat()
            return (st.st_mtime_ns, st.st_size)
        except FileNotFoundError:
            return None

    def invalidate_table(self, table_name: str) -> None:
        """Invalidate row cache and all secondary indexes cached for a table."""
        self._row_cache.pop(table_name, None)
        stale = [
            idx_name
            for idx_name, idx_obj in self._index_cache.items()
            if idx_obj.meta.table_name == table_name
        ]
        for idx_name in stale:
            self._index_cache.pop(idx_name, None)

    def invalidate_all(self) -> None:
        """Clear all cached rows and secondary indexes."""
        self._row_cache.clear()
        self._index_cache.clear()

    def get_table_rows(self, table: Table) -> list[RowDict]:
        """Return parsed rows for table, automatically refreshing if the TSV file changed."""
        fp = self._file_fingerprint(table.file_path)
        cached = self._row_cache.get(table.name)
        if cached is not None and cached[0] == fp:
            return cached[1]
        # File changed or not cached yet: invalidate any stale indexes on this table too.
        self.invalidate_table(table.name)
        rows = [table._row_to_dict(raw) for raw in table._read_raw_rows()]
        self._row_cache[table.name] = (fp, rows)
        return rows

    def count_cached_rows(self, table: Table) -> int | None:
        """Return cached row count if fresh, or None if cache miss/stale."""
        fp = self._file_fingerprint(table.file_path)
        cached = self._row_cache.get(table.name)
        if cached is not None and cached[0] == fp:
            return len(cached[1])
        return None

    def create_table(self, name: str, defs: dict[str, Types]) -> Table:
        """Create a table in this engine's data directory and return it."""
        _log.debug(f'Creating table: {name} in {self.data_dir}')
        if not defs:
            raise EngineError('Table must have at least one column')

        self.data_dir.mkdir(parents=True, exist_ok=True)

        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as metadata:
                reader = csv.reader(metadata, delimiter='\t')
                if any(row and row[0] == name for row in reader):
                    raise TableAlreadyExistsError(f'Table already exists: {name}')
        except FileNotFoundError:
            pass

        with open(self.metadata_path, 'a', encoding='utf-8') as metadata:
            row = [name, str(len(defs))] + [str(x) for pair in defs.items() for x in pair]
            _log.debug(f'Writing metadata: {row}')
            metadata.write('\t'.join(row) + '\n')

        table = Table(name, defs, data_dir=self.data_dir, storage=self)
        table.create_if_not_exists()
        self.invalidate_table(name)
        return table

    def get_table(self, name: str) -> Table | None:
        """Return Table object or None if table doesn't exist."""
        _log.debug(f'Getting table: {name} from {self.data_dir}')
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as metadata:
                reader = csv.reader(metadata, delimiter='\t')
                row = next((row for row in reader if row and row[0] == name), None)
                if row is None:
                    return None
                count = int(row[1])
                defs = {row[i]: Types(row[i + 1]) for i in range(2, 2 + count * 2, 2)}
                return Table(name, defs, data_dir=self.data_dir, storage=self)
        except FileNotFoundError:
            _log.warning('%s not found. Returning None.', self.metadata_path)
            return None

    def list_tables(self) -> list[tuple[str, int, dict[str, str]]]:
        """Return all tables with their column counts and definitions.
        Returns: list of (table_name, col_count, {col_name: col_type_str})
        """
        result: list[tuple[str, int, dict[str, str]]] = []
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as metadata:
                reader = csv.reader(metadata, delimiter='\t')
                for row in reader:
                    if not row:
                        continue
                    name = row[0]
                    count = int(row[1])
                    defs = {row[i]: row[i + 1] for i in range(2, len(row), 2)}
                    result.append((name, count, defs))
        except FileNotFoundError:
            pass
        return result

    def drop_table(self, name: str) -> None:
        """Drop table and erase metadata atomically."""
        _log.debug(f'Dropping table: {name} from {self.data_dir}')
        data_path = self.data_dir / f'{name}.txt'

        existing_rows: list[list[str]] = []
        in_metadata = False
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t')
                for row in reader:
                    if not row:
                        continue
                    if row[0] == name:
                        in_metadata = True
                    else:
                        existing_rows.append(row)
        except FileNotFoundError:
            pass

        file_exists = data_path.exists()
        if not in_metadata and not file_exists:
            raise TableNotFoundError(f'Table does not exist: {name}')

        if file_exists:
            data_path.unlink()

        if self.metadata_path.exists() or in_metadata:
            fd, tmp_path = tempfile.mkstemp(dir=self.data_dir, prefix='.metadata-')
            try:
                with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                    writer.writerows(existing_rows)
                os.replace(tmp_path, self.metadata_path)
            except Exception:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise

        self._drop_indexes_for_table(name)
        self.invalidate_table(name)

    # -- Secondary index metadata & cache ------------------------------

    def _read_index_rows(self) -> list[IndexMeta]:
        try:
            with open(self.indexes_path, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f, delimiter='\t')
                return [
                    IndexMeta(name=row[0], table_name=row[1], column_name=row[2])
                    for row in reader
                    if len(row) >= 3
                ]
        except FileNotFoundError:
            return []

    def _write_index_rows(self, metas: list[IndexMeta]) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self.data_dir, prefix='.indexes-')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                writer.writerows([[m.name, m.table_name, m.column_name] for m in metas])
            os.replace(tmp_path, self.indexes_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def _drop_indexes_for_table(self, table_name: str) -> None:
        metas = self._read_index_rows()
        if not metas:
            return
        kept = [m for m in metas if m.table_name != table_name]
        if len(kept) != len(metas):
            self._write_index_rows(kept)

    def create_index(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        if_not_exists: bool = False,
    ) -> IndexMeta:
        """Create and build a secondary index on table_name(column_name)."""
        table = self.get_table(table_name)
        if table is None:
            raise TableNotFoundError(f'Table does not exist: {table_name}')
        if column_name not in table.defs:
            raise ColumnNotFoundError(f'Column does not exist: {column_name}')

        metas = self._read_index_rows()
        existing = next((m for m in metas if m.name == index_name), None)
        if existing is not None:
            if if_not_exists:
                return existing
            raise EngineError(f'Index already exists: {index_name}')

        meta = IndexMeta(name=index_name, table_name=table_name, column_name=column_name)
        metas.append(meta)
        self._write_index_rows(metas)
        self.get_built_index(table, meta)
        return meta

    def drop_index(self, index_name: str, if_exists: bool = False) -> None:
        """Drop a secondary index from indexes.txt and cache."""
        metas = self._read_index_rows()
        kept = [m for m in metas if m.name != index_name]
        if len(kept) == len(metas):
            if if_exists:
                return
            raise EngineError(f'Index does not exist: {index_name}')
        self._write_index_rows(kept)
        self._index_cache.pop(index_name, None)

    def list_indexes(self, table_name: str | None = None) -> list[IndexMeta]:
        """List all persisted indexes (optionally filtered by table_name)."""
        metas = self._read_index_rows()
        if table_name is not None:
            return [m for m in metas if m.table_name == table_name]
        return metas

    def get_table_indexes(self, table: Table) -> dict[str, tuple[IndexMeta, ColumnIndex]]:
        """Return {column_name: (IndexMeta, ColumnIndex)} for all indexes on table."""
        metas = self.list_indexes(table.name)
        if not metas:
            return {}
        result: dict[str, tuple[IndexMeta, ColumnIndex]] = {}
        for meta in metas:
            if meta.column_name not in table.defs:
                continue
            col_idx = self.get_built_index(table, meta)
            # First index created on a column takes precedence.
            result.setdefault(meta.column_name, (meta, col_idx))
        return result

    def get_built_index(self, table: Table, meta: IndexMeta) -> ColumnIndex:
        """Return a fresh ColumnIndex, rebuilding automatically if the table TSV changed."""
        rows = self.get_table_rows(table)
        cached = self._index_cache.get(meta.name)
        if cached is not None:
            return cached
        built = ColumnIndex(meta, table.defs[meta.column_name], rows)
        self._index_cache[meta.name] = built
        return built


# ============================================================================
# Table class
# ============================================================================

class Table:
    """A table stored as a TSV file in a target data directory."""

    name: str
    data_dir: Path
    file_path: Path
    filename: str
    defs: dict[str, Types]

    def __init__(
        self,
        name: str,
        defs: dict[str, Types],
        data_dir: str | os.PathLike[str] = '.',
        storage: StorageEngine | None = None,
    ) -> None:
        self.name = name
        self.data_dir = Path(data_dir)
        self.file_path = self.data_dir / f'{self.name}.txt'
        self.filename = str(self.file_path)
        self.defs = defs
        self._storage = storage

    # -- Public helpers ------------------------------------------------

    def column_names(self) -> list[str]:
        """Return ordered list of column names."""
        return list(self.defs.keys())

    def column_types(self) -> dict[str, Types]:
        """Return a copy of {column_name: type} mapping."""
        return dict(self.defs)

    def count_rows(self) -> int:
        """Count records in the table (safe with multi-line strings)."""
        if self._storage is not None:
            cached_count = self._storage.count_cached_rows(self)
            if cached_count is not None:
                return cached_count
        try:
            with open(self.file_path, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f, **_CSV_READ_KWARGS)
                return sum(1 for row in reader if row)
        except FileNotFoundError:
            return 0

    # -- File management -----------------------------------------------

    def create_if_not_exists(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            _log.debug(f'Creating file: {self.file_path}')
            with open(self.file_path, 'w', encoding='utf-8'):
                pass

    # -- CRUD ----------------------------------------------------------

    def insert_rows(self, rows: list[RowDict]) -> None:
        """Validate and append multiple rows atomically at the batch level."""
        prepared = [
            [_data_to_string(values.get(key), value) for key, value in self.defs.items()]
            for values in rows
        ]
        with open(self.file_path, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, **_CSV_WRITE_KWARGS)
            writer.writerows(prepared)
        if self._storage is not None:
            self._storage.invalidate_table(self.name)

    def insert_values(self, values: RowDict) -> None:
        """Insert a single row. Use None for SQL NULL."""
        _log.debug(f'Inserting into {self.name}: {values}')
        self.insert_rows([values])

    def update(self, values: RowDict, where: Callable[[RowDict], bool] | None = None) -> int:
        """
        Update rows matching the where predicate.
        :param values: Dictionary of columns to update. Use None for SQL NULL.
        :param where: Row predicate. None means update all rows.
        :return: Number of matched/updated rows.
        """
        _log.debug(f'Updating {self.name}: {values}')

        def_count: int = len(self.defs)

        updated_values: list[str | _Unchanged] = [
            _data_to_string(values[key], value) if key in values else _UNCHANGED
            for key, value in self.defs.items()
        ]
        _log.debug(f'Computed values: {updated_values}')

        if all(value is _UNCHANGED for value in updated_values):
            _log.warning(f'No values to update: {values}')
            return 0

        table_values = self._read_raw_rows()
        updated_count = 0

        for i, row in enumerate(table_values):
            if where is None or where(self._row_to_dict(row)):
                table_values[i] = [
                    row[j] if updated_values[j] is _UNCHANGED else updated_values[j]
                    for j in range(def_count)
                ]
                updated_count += 1

        self._write_raw_rows(table_values)
        return updated_count

    def update_rows(
        self,
        updater: Callable[[RowDict], RowDict],
        where: Callable[[RowDict], bool] | None = None,
    ) -> int:
        """Update matching rows via a per-row updater callback, validating all rows before atomic write."""
        col_defs = list(self.defs.items())
        table_values = self._read_raw_rows()
        updated_count = 0

        for i, raw_row in enumerate(table_values):
            row_dict = self._row_to_dict(raw_row)
            if where is None or where(row_dict):
                new_vals = updater(row_dict)
                table_values[i] = [
                    raw_row[j] if col_name not in new_vals else _data_to_string(new_vals[col_name], col_type)
                    for j, (col_name, col_type) in enumerate(col_defs)
                ]
                updated_count += 1

        self._write_raw_rows(table_values)
        return updated_count

    def delete(self, where: Callable[[RowDict], bool] | None = None) -> int:
        """
        Delete rows matching the where predicate.
        :param where: Row predicate. None truncates all rows.
        :return: Number of deleted rows.
        """
        _log.debug(f'Deleting from {self.name}')

        if where is None:
            deleted_count = self.count_rows()
            self._write_raw_rows([])
            return deleted_count

        table_values = self._read_raw_rows()
        deleted_count = 0
        kept: list[list[str]] = []

        for row in table_values:
            if where(self._row_to_dict(row)):
                _log.debug(f'Deleting: {row}')
                deleted_count += 1
            else:
                kept.append(row)

        self._write_raw_rows(kept)
        return deleted_count

    def read_rows(self) -> list[RowDict]:
        """Read all rows from the table as typed RowDicts."""
        if self._storage is not None:
            return [dict(r) for r in self._storage.get_table_rows(self)]
        return [self._row_to_dict(row) for row in self._read_raw_rows()]

    def select(self) -> list[RowDict]:
        """Backward-compatible alias for read_rows()."""
        return self.read_rows()

    # -- Internal helpers ----------------------------------------------

    def _read_raw_rows(self) -> list[list[str]]:
        """Read all rows from the TSV file as raw string lists."""
        try:
            with open(self.file_path, 'r', encoding='utf-8', newline='') as file:
                reader = csv.reader(file, **_CSV_READ_KWARGS)
                return [row for row in reader if row]
        except FileNotFoundError:
            return []

    def _write_raw_rows(self, rows: list[list[str]]) -> None:
        """Atomically write raw rows to the TSV file using a temp file + rename."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self.data_dir, prefix=f'.{self.name}-')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file, **_CSV_WRITE_KWARGS)
                writer.writerows(rows)
            os.replace(tmp_path, self.file_path)
            if self._storage is not None:
                self._storage.invalidate_table(self.name)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def _row_to_dict(self, row: list[str]) -> RowDict:
        """Convert a single raw row (list of strings) to a typed RowDict."""
        return {key: _string_to_data(val, typ) for val, (key, typ) in zip(row, self.defs.items())}


# ============================================================================
# Backward-compatible module-level functions (operating on current working dir)
# ============================================================================

def create_table(name: str, defs: dict[str, Types]) -> Table:
    return StorageEngine().create_table(name, defs)


def get_table(name: str) -> Table | None:
    return StorageEngine().get_table(name)


def list_tables() -> list[tuple[str, int, dict[str, str]]]:
    return StorageEngine().list_tables()


def drop_table(name: str) -> None:
    StorageEngine().drop_table(name)
