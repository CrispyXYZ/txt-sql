"""Interactive CLI loop and table renderer."""

import logging
import os
import traceback
from collections.abc import Mapping, Sequence
from typing import Any

from .engine import Database
from .exceptions import TxtSqlError
from .result import StatementKind


def _format_cell(value: Any) -> str:
    return '' if value is None else str(value)


def print_table(rows: Sequence[Mapping[str, Any]]) -> None:
    """Print a list of dictionaries as a formatted ASCII table with borders."""
    if not rows:
        print("Empty set.")
        return

    headers = list(rows[0].keys())

    # Calculate column widths
    widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            val = _format_cell(row.get(h))
            widths[h] = max(widths[h], len(val))

    separator = "+" + "+".join("-" * (widths[h] + 2) for h in headers) + "+"

    print()
    print(separator)
    header_str = "|" + "|".join(f" {h.ljust(widths[h])} " for h in headers) + "|"
    print(header_str)
    print(separator)

    for row in rows:
        row_str = "|" + "|".join(f" {_format_cell(row.get(h)).ljust(widths[h])} " for h in headers) + "|"
        print(row_str)

    print(separator)


def run_cli(data_dir: str | os.PathLike[str] = '.') -> None:
    """Start the interactive TxtSQL REPL."""
    log_level = os.environ.get('TXT_SQL_LOG_LEVEL', 'WARNING')
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.WARNING))

    db = Database(data_dir)

    print("Welcome to TxtSql CLI. Type your SQL statement and press Enter.")
    print("Type 'exit' or 'quit' to exit.")

    buffer = []          # accumulate lines of the current statement
    prompt = "txtsql> "

    while True:
        try:
            line = input(prompt)
        except EOFError:
            print()
            if buffer:
                buffer.clear()
                prompt = "txtsql> "
                continue
            break
        except KeyboardInterrupt:
            print()
            if buffer:
                buffer.clear()
                prompt = "txtsql> "
                continue
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            traceback.print_exc()
            if buffer:
                buffer.clear()
                prompt = "txtsql> "
            continue

        stripped = line.strip()

        # Skip empty lines when buffer is empty
        if not buffer and not stripped:
            continue

        # Exit command only when buffer is empty
        if not buffer and stripped.lower() in ('exit', 'quit'):
            break

        # Add the line to buffer (preserve original formatting)
        buffer.append(line)

        # Reconstruct current statement
        statement = "\n".join(buffer).rstrip()

        # Check if the statement ends with a semicolon -> complete
        if statement.endswith(";"):
            try:
                result = db.execute(statement.rstrip(";").strip())
            except TxtSqlError as e:
                print(f"Error: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
                traceback.print_exc()
            else:
                if result.is_tabular:
                    print_table(result.rows)
                elif result.kind in (
                    StatementKind.INSERT,
                    StatementKind.UPDATE,
                    StatementKind.DELETE,
                    StatementKind.IMPORT,
                    StatementKind.EXPORT,
                ):
                    print(f"{result.affected_rows} rows affected.")
                else:
                    print("Query OK.")
            finally:
                buffer.clear()
                prompt = "txtsql> "
        else:
            # Statement not complete – change prompt for continuation
            prompt = "... "