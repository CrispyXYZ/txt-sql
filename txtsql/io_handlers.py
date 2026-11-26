"""External file import/export handlers (.xlsx, .csv, .tsv)."""

import csv
import os
from pathlib import Path

import openpyxl

from .evaluator import is_number, to_decimal
from .exceptions import EngineError
from .types import RowDict, Types


def export_rows(rows: list[RowDict], headers: list[str], filepath: str | os.PathLike[str]) -> int:
    """Export query rows to .xlsx, .csv, or .tsv determined by file extension."""
    path = Path(filepath)
    ext = path.suffix.lstrip('.').lower()
    if ext == 'xlsx':
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(headers)
        for row in rows:
            ws.append([_cell_value(row.get(h)) for h in headers])
        wb.save(path)
    else:
        delim = '\t' if ext == 'tsv' else ','
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=delim, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(headers)
            for row in rows:
                writer.writerow([_cell_value(row.get(h)) for h in headers])
    return len(rows)


def read_excel_table(
    file_path: str | os.PathLike[str],
    explicit_columns: list[tuple[str, str]] | None = None,
) -> tuple[dict[str, Types], list[RowDict]]:
    """Read an .xlsx worksheet into (column_defs, typed_rows)."""
    try:
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    except FileNotFoundError as exc:
        raise EngineError(f'File not found: {file_path}') from exc

    ws = wb.active
    raw_rows: list[tuple] = []
    empty_streak = 0
    for row in ws.iter_rows(values_only=True):
        if any(cell is not None for cell in row):
            raw_rows.append(row)
            empty_streak = 0
        elif raw_rows:
            empty_streak += 1
            if empty_streak > 10:
                break
    wb.close()

    if not raw_rows:
        raise EngineError('Excel file is empty')

    headers = [str(cell) if cell is not None else '' for cell in raw_rows[0]]
    col_defs = (
        {name: Types(t) for name, t in explicit_columns}
        if explicit_columns
        else _infer_types(raw_rows[1:], headers)
    )
    if not col_defs:
        raise EngineError('No valid columns found in Excel file')

    prepared_rows: list[RowDict] = []
    for row in raw_rows[1:]:
        values: RowDict = {}
        for i, h in enumerate(headers):
            if not h or h not in col_defs:
                continue
            raw_val = row[i] if i < len(row) else None
            if raw_val is None:
                values[h] = None
            elif col_defs[h] == Types.STRING:
                values[h] = str(raw_val)
            else:
                values[h] = to_decimal(raw_val)
        prepared_rows.append(values)

    return col_defs, prepared_rows


def _cell_value(value: object) -> object:
    return '' if value is None else value


def _infer_types(rows: list[tuple], headers: list[str]) -> dict[str, Types]:
    sample = rows[:100]
    defs: dict[str, Types] = {}
    for i, h in enumerate(headers):
        if not h:
            continue
        col_vals = [row[i] if i < len(row) else None for row in sample]
        non_null = [v for v in col_vals if v is not None]
        if not non_null:
            defs[h] = Types.STRING
        elif all(is_number(v) for v in non_null):
            defs[h] = Types.NUMBER
        else:
            defs[h] = Types.STRING
    return defs
