from __future__ import annotations

import csv
import logging
import os
import tempfile
from collections.abc import Callable
from decimal import Decimal

from .exceptions import EngineError, TableAlreadyExistsError, TableNotFoundError, TypeMismatchError
from .types import Types, DataValue, RowDict

_log = logging.getLogger(__name__)

METADATA_FILENAME = 'metadata.txt'
_NULL_SENTINEL = '\\N'


# Sentinel object used internally to mean "leave this column unchanged" during UPDATE
class _Unchanged:
    pass


_UNCHANGED = _Unchanged()


# ============================================================================
# Module-level functions
# ============================================================================

def create_table(name: str, defs: dict[str, Types]) -> Table:
    """Create a table and return it."""
    _log.debug(f'Creating table: {name}')
    if not defs:
        raise EngineError('Table must have at least one column')

    try:
        with open(METADATA_FILENAME, 'r', encoding='utf-8') as metadata:
            reader = csv.reader(metadata, delimiter='\t')
            if any(row and row[0] == name for row in reader):
                raise TableAlreadyExistsError(f'Table already exists: {name}')
    except FileNotFoundError:
        pass

    with open(METADATA_FILENAME, 'a', encoding='utf-8') as metadata:
        row = [name, str(len(defs))] + [str(x) for pair in defs.items() for x in pair]
        _log.debug(f'Writing metadata: {row}')
        metadata.write('\t'.join(row) + '\n')

    table = Table(name, defs)
    table.create_if_not_exists()
    return table


def get_table(name: str) -> Table | None:
    """Return Table object or None if table doesn't exist."""
    _log.debug(f'Getting table: {name}')
    try:
        with open(METADATA_FILENAME, 'r', encoding='utf-8') as metadata:
            reader = csv.reader(metadata, delimiter='\t')
            row = next((row for row in reader if row and row[0] == name), None)
            if row is None:
                return None
            count = int(row[1])
            return Table(name, {row[i]: Types(row[i + 1]) for i in range(2, 2 + count * 2, 2)})
    except FileNotFoundError:
        _log.warning('metadata.txt file not found. Returning None.')
        return None


def list_tables() -> list[tuple[str, int, dict[str, str]]]:
    """Return all tables with their definitions.
    Returns: list of (table_name, col_count, {col_name: col_type_str})
    """
    result: list[tuple[str, int, dict[str, str]]] = []
    try:
        with open(METADATA_FILENAME, 'r', encoding='utf-8') as metadata:
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


def drop_table(name: str) -> None:
    """Drop table and erase metadata atomically."""
    _log.debug(f'Dropping table: {name}')
    data_filename = f'{name}.txt'

    existing_rows: list[list[str]] = []
    in_metadata = False
    try:
        with open(METADATA_FILENAME, 'r', encoding='utf-8') as f:
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

    file_exists = os.path.exists(data_filename)
    if not in_metadata and not file_exists:
        raise TableNotFoundError(f'Table does not exist: {name}')

    if file_exists:
        os.remove(data_filename)

    if os.path.exists(METADATA_FILENAME) or in_metadata:
        fd, tmp_path = tempfile.mkstemp(dir='.', prefix='.metadata-')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f, delimiter='\t', lineterminator='\n')
                writer.writerows(existing_rows)
            os.replace(tmp_path, METADATA_FILENAME)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise


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


def _string_to_number(string: str) -> Decimal:
    return Decimal(string)


def _number_to_string(number: Decimal) -> str:
    return str(number)


def _data_to_string(value: object, type_def: Types) -> str:
    """Convert data value to string for TSV storage.

    None (SQL NULL) -> '\\N' sentinel; backslashes in strings are escaped as '\\\\'
    so empty string '' and literal '\\N' round-trip without ambiguity.
    """
    if value is None:
        return _NULL_SENTINEL
    match type_def:
        case Types.NUMBER:
            return _number_to_string(_coerce_number(value))
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
            return _string_to_number(string)
        case Types.STRING:
            return string.replace('\\\\', '\\')


# ============================================================================
# CSV dialect: tab-separated, all fields quoted so \t \n " \N round-trip cleanly
# ============================================================================

_CSV_WRITE_KWARGS = dict(delimiter='\t', quoting=csv.QUOTE_ALL, lineterminator='\n')
_CSV_READ_KWARGS = dict(delimiter='\t')


# ============================================================================
# Table class
# ============================================================================

class Table:
    """A table stored as a TSV file."""

    name: str
    filename: str
    defs: dict[str, Types]

    def __init__(self, name: str, defs: dict[str, Types]) -> None:
        self.name = name
        self.filename = f'{self.name}.txt'
        self.defs = defs

    # -- Public helpers ------------------------------------------------

    def column_names(self) -> list[str]:
        """Return ordered list of column names."""
        return list(self.defs.keys())

    def column_types(self) -> dict[str, Types]:
        """Return a copy of {column_name: type} mapping."""
        return dict(self.defs)

    def count_rows(self) -> int:
        """Count records in the table (safe with multi-line strings)."""
        try:
            with open(self.filename, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f, **_CSV_READ_KWARGS)
                return sum(1 for row in reader if row)
        except FileNotFoundError:
            return 0

    # -- File management -----------------------------------------------

    def create_if_not_exists(self) -> None:
        if not os.path.exists(self.filename):
            _log.debug(f'Creating file: {self.filename}')
            with open(self.filename, 'w', encoding='utf-8'):
                pass

    # -- CRUD ----------------------------------------------------------

    def insert_rows(self, rows: list[RowDict]) -> None:
        """Validate and append multiple rows atomically at the batch level."""
        prepared = [
            [_data_to_string(values.get(key), value) for key, value in self.defs.items()]
            for values in rows
        ]
        with open(self.filename, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, **_CSV_WRITE_KWARGS)
            writer.writerows(prepared)

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

    def select(
            self,
            columns: list[str] | None = None,
            where: Callable[[RowDict], bool] | None = None,
            order_by: list[tuple[str, bool]] | None = None,
            distinct: bool = False,
            limit: int | None = None,
            offset: int = 0,
    ) -> list[RowDict]:
        """
        Read rows from the table with optional filtering, projection, sorting, and pagination.
        Aggregate logic (GROUP BY, aggregations, HAVING) is handled in executor.py.
        """
        rows = self._read_rows()

        # WHERE
        if where is not None:
            rows = [r for r in rows if where(r)]

        # Column projection
        if columns is not None:
            rows = self._project(rows, columns)

        # DISTINCT
        if distinct:
            rows = self._dedup(rows)

        # ORDER BY
        if order_by:
            rows = self._order(rows, order_by)

        # OFFSET / LIMIT
        if offset > 0:
            rows = rows[offset:]
        if limit is not None:
            rows = rows[:limit]

        return rows

    # -- Internal helpers ----------------------------------------------

    def _read_raw_rows(self) -> list[list[str]]:
        """Read all rows from the TSV file as raw string lists."""
        try:
            with open(self.filename, 'r', encoding='utf-8', newline='') as file:
                reader = csv.reader(file, **_CSV_READ_KWARGS)
                return [row for row in reader if row]
        except FileNotFoundError:
            return []

    def _write_raw_rows(self, rows: list[list[str]]) -> None:
        """Atomically write raw rows to the TSV file using a temp file + rename."""
        fd, tmp_path = tempfile.mkstemp(dir='.', prefix=f'.{self.name}-')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file, **_CSV_WRITE_KWARGS)
                writer.writerows(rows)
            os.replace(tmp_path, self.filename)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def _read_rows(self) -> list[RowDict]:
        """Read raw rows and convert to list[RowDict]."""
        return [self._row_to_dict(row) for row in self._read_raw_rows()]

    def _row_to_dict(self, row: list[str]) -> RowDict:
        """Convert a single raw row (list of strings) to a typed RowDict."""
        return {key: _string_to_data(val, typ) for val, (key, typ) in zip(row, self.defs.items())}

    @staticmethod
    def _project(rows: list[RowDict], columns: list[str]) -> list[RowDict]:
        """Project rows down to the given columns."""
        return [{col: r[col] for col in columns if col in r} for r in rows]

    @staticmethod
    def _dedup(rows: list[RowDict]) -> list[RowDict]:
        """Remove duplicate rows in a type-safe manner."""
        seen = set()
        result: list[RowDict] = []
        for r in rows:
            key = tuple(
                (k, (0, None) if v is None else (1, v) if isinstance(v, Decimal) else (2, v))
                for k, v in r.items()
            )
            if key not in seen:
                seen.add(key)
                result.append(r)
        return result

    @staticmethod
    def _order(rows: list[RowDict], order_by: list[tuple[str, bool]]) -> list[RowDict]:
        """Sort rows by given columns. NULLs sort last regardless of direction."""
        result = list(rows)
        for col, desc in reversed(order_by):
            null_rows = [r for r in result if r.get(col) is None]
            non_null_rows = [r for r in result if r.get(col) is not None]
            non_null_rows.sort(key=lambda r, _c=col: r[_c], reverse=desc)
            result = non_null_rows + null_rows
        return result
