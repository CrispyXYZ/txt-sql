"""Interactive CLI loop and table renderer."""

import logging
import os
import traceback
from collections.abc import Mapping, Sequence
from typing import Any

from .engine import execute_sql
from .exceptions import TxtSqlError


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
            val = str(row.get(h, ''))
            widths[h] = max(widths[h], len(val))

    separator = "+" + "+".join("-" * (widths[h] + 2) for h in headers) + "+"

    print()
    print(separator)
    header_str = "|" + "|".join(f" {h.ljust(widths[h])} " for h in headers) + "|"
    print(header_str)
    print(separator)

    for row in rows:
        row_str = "|" + "|".join(f" {str(row.get(h, '')).ljust(widths[h])} " for h in headers) + "|"
        print(row_str)

    print(separator)


def run_cli() -> None:
    """Start the interactive TxtSQL REPL."""
    log_level = os.environ.get('TXT_SQL_LOG_LEVEL', 'WARNING')
    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.WARNING))

    print("Welcome to TxtSql CLI. Type your SQL statement and press Enter.")
    print("Type 'exit' or 'quit' to exit.")

    while True:
        try:
            line = input("txtsql> ")
            if line.strip().lower() in ('exit', 'quit'):
                break
            if not line.strip():
                continue

            result = execute_sql(line.strip())

            if isinstance(result, list):
                print_table(result)
            elif isinstance(result, int):
                print(f"{result} rows affected.")
            elif result is not None:
                print(result)
            else:
                print("Query OK.")

        except (EOFError, KeyboardInterrupt):
            print()
            break
        except TxtSqlError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    run_cli()
