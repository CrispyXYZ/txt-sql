"""Pure Python controller and helpers for the TxtSql Tkinter GUI (headless-testable)."""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from .engine import Database
from .exceptions import EngineError, TxtSqlError
from .io_handlers import export_rows
from .result import QueryResult, StatementKind
from .types import RowDict


NULL_DISPLAY = '<NULL>'
_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_LINE_COMMENT_RE = re.compile(r'--[^\n]*')


@dataclass(frozen=True, slots=True)
class TableMeta:
    """Metadata summary for one table in the sidebar tree."""
    name: str
    row_count: int
    columns: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class ExecutionEntry:
    """Record of one executed SQL statement."""
    sql: str
    timestamp: str
    elapsed_ms: float
    ok: bool
    result: QueryResult | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class BatchExecutionReport:
    """Aggregated outcome of running one or more SQL statements."""
    entries: list[ExecutionEntry]
    last_tabular: QueryResult | None
    schema_changed: bool
    total_elapsed_ms: float
    error_count: int


DEMO_TEMPLATES: dict[str, str] = {
    '1. 单表增删改查 (CRUD)': (
        "-- 单表演示：建表、插入（含空串、NULL、负数、TRUE/FALSE）、表达式更新与查询\n"
        "DROP TABLE IF EXISTS demo_students;\n"
        "CREATE TABLE demo_students (\n"
        "    id NUMBER,\n"
        "    name STRING,\n"
        "    score NUMBER,\n"
        "    passed NUMBER,\n"
        "    remark STRING\n"
        ");\n"
        "INSERT INTO demo_students VALUES\n"
        "    (1, '张三', 92.5, TRUE, '表现优秀'),\n"
        "    (2, '李四', 58, FALSE, ''),\n"
        "    (3, '王五', NULL, FALSE, NULL),\n"
        "    (4, 'O''Brien', -1, FALSE, '缺考标记');\n"
        "UPDATE demo_students SET score = score + 5, passed = TRUE WHERE id = 2;\n"
        "SELECT id, name, score, passed, remark FROM demo_students ORDER BY id ASC;"
    ),
    '2. 分组统计与聚合分析': (
        "-- 聚合演示：GROUP BY、HAVING COUNT(*)、COUNT(DISTINCT)、聚合算术表达式\n"
        "DROP TABLE IF EXISTS demo_sales;\n"
        "CREATE TABLE demo_sales (id NUMBER, dept STRING, product STRING, amount NUMBER);\n"
        "INSERT INTO demo_sales VALUES\n"
        "    (1, '华东区', '笔记本', 6000),\n"
        "    (2, '华东区', '笔记本', 6500),\n"
        "    (3, '华东区', '显示器', 2200),\n"
        "    (4, '华北区', '键盘', 500),\n"
        "    (5, '华北区', '鼠标', NULL),\n"
        "    (6, '华南区', '服务器', 18000);\n"
        "SELECT dept, COUNT(*) AS orders, COUNT(DISTINCT product) AS products,\n"
        "       SUM(amount) AS total, MAX(amount) - MIN(amount) AS span\n"
        "FROM demo_sales\n"
        "GROUP BY dept\n"
        "HAVING COUNT(*) > 1\n"
        "ORDER BY total DESC;"
    ),
    '3. 多表连接查询 (JOIN)': (
        "-- 多表演示：INNER JOIN、LEFT JOIN、USING 合并列与 SELECT table.*\n"
        "DROP TABLE IF EXISTS demo_emp;\n"
        "DROP TABLE IF EXISTS demo_dept;\n"
        "CREATE TABLE demo_dept (id NUMBER, dname STRING);\n"
        "INSERT INTO demo_dept VALUES (10, '研发部'), (20, '市场部'), (30, '财务部');\n"
        "CREATE TABLE demo_emp (id NUMBER, name STRING, dept_id NUMBER, salary NUMBER);\n"
        "INSERT INTO demo_emp VALUES\n"
        "    (1, '张明', 10, 12000),\n"
        "    (2, '陈芳', 20, 9500),\n"
        "    (3, '刘强', NULL, 8000);\n"
        "SELECT e.id, e.name, d.dname, e.salary\n"
        "FROM demo_emp e\n"
        "LEFT JOIN demo_dept d ON e.dept_id = d.id\n"
        "ORDER BY e.id ASC;"
    ),
    '4. 子查询与集合运算 (UNION)': (
        "-- 高级查询演示：IN、BETWEEN、LIKE、关联 EXISTS、标量子查询与 UNION\n"
        "DROP TABLE IF EXISTS demo_a;\n"
        "DROP TABLE IF EXISTS demo_b;\n"
        "CREATE TABLE demo_a (id NUMBER, name STRING, score NUMBER);\n"
        "INSERT INTO demo_a VALUES (1, 'Alice', 85), (2, 'Bob', 92), (3, 'Alex', 70);\n"
        "CREATE TABLE demo_b (id NUMBER, name STRING);\n"
        "INSERT INTO demo_b VALUES (1, 'Alice'), (4, 'Bella');\n"
        "SELECT id, name FROM demo_a\n"
        "WHERE score BETWEEN 80 AND 100\n"
        "  AND EXISTS (SELECT * FROM demo_b WHERE demo_b.id = demo_a.id)\n"
        "UNION\n"
        "SELECT id, name FROM demo_b\n"
        "WHERE name LIKE 'B%'\n"
        "ORDER BY id ASC;"
    ),
}


def split_sql_statements(script: str) -> list[str]:
    """Split a SQL script by semicolons outside string literals and -- line comments."""
    statements: list[str] = []
    n = len(script)
    i = 0
    start = 0
    in_string = False
    in_comment = False

    while i < n:
        ch = script[i]
        if in_comment:
            if ch == '\n':
                in_comment = False
            i += 1
            continue
        if in_string:
            if ch == "'":
                if i + 1 < n and script[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue

        if ch == "'":
            in_string = True
        elif ch == '-' and i + 1 < n and script[i + 1] == '-':
            in_comment = True
            i += 2
            continue
        elif ch == ';':
            chunk = script[start:i].strip()
            if _has_executable_sql(chunk):
                statements.append(chunk)
            start = i + 1
        i += 1

    tail = script[start:].strip()
    if _has_executable_sql(tail):
        statements.append(tail)
    return statements


def _has_executable_sql(chunk: str) -> bool:
    """Return True if chunk contains characters outside whitespace and -- comments."""
    stripped = _LINE_COMMENT_RE.sub('', chunk).strip()
    return bool(stripped)


def format_cell_display(value: object) -> str:
    """Format a cell value for full detail display (None -> '<NULL>', '' -> '')."""
    if value is None:
        return NULL_DISPLAY
    return str(value)


def format_cell_single_line(value: object, max_len: int = 120) -> str:
    """Format a cell value for single-line Treeview display."""
    if value is None:
        return NULL_DISPLAY
    text = str(value).replace('\r\n', '\n').replace('\n', '↵ ').replace('\t', '  ')
    if len(text) > max_len:
        return text[:max_len - 1] + '…'
    return text


def build_create_table_sql(
    table_name: str,
    columns: list[tuple[str, str]],
    if_not_exists: bool = False,
) -> str:
    """Validate inputs from the Create Table dialog and produce a CREATE TABLE SQL string."""
    tname = table_name.strip()
    if not _IDENT_RE.match(tname):
        raise ValueError(f'表名 "{tname}" 不合法，需以字母或下划线开头，仅含字母/数字/下划线。')
    if not columns:
        raise ValueError('至少需要定义 1 个列。')

    seen: set[str] = set()
    col_defs: list[str] = []
    for raw_name, raw_type in columns:
        cname = raw_name.strip()
        ctype = raw_type.strip().upper()
        if not _IDENT_RE.match(cname):
            raise ValueError(f'列名 "{cname}" 不合法，需以字母或下划线开头，仅含字母/数字/下划线。')
        if cname in seen:
            raise ValueError(f'列名 "{cname}" 重复。')
        if ctype not in ('STRING', 'NUMBER'):
            raise ValueError(f'列 "{cname}" 的类型 "{ctype}" 无效，仅支持 STRING 或 NUMBER。')
        seen.add(cname)
        col_defs.append(f'{cname} {ctype}')

    clause = 'IF NOT EXISTS ' if if_not_exists else ''
    return f'CREATE TABLE {clause}{tname} ({", ".join(col_defs)});'


def sort_rows_for_view(rows: list[RowDict], column: str, descending: bool = False) -> list[RowDict]:
    """Sort rows in memory for GUI column-header sorting (NULLs always placed last)."""
    null_rows = [r for r in rows if r.get(column) is None]
    non_null = [r for r in rows if r.get(column) is not None]
    try:
        non_null.sort(key=lambda r: r[column], reverse=descending)
    except TypeError:
        non_null.sort(key=lambda r: str(r[column]), reverse=descending)
    return non_null + null_rows


class GuiController:
    """Stateful controller backing the Tkinter GUI without depending on tkinter itself."""

    def __init__(self, data_dir: str | os.PathLike[str] = '.') -> None:
        self.db = Database(Path(data_dir).resolve())
        self.history: list[ExecutionEntry] = []
        self.last_tabular_result: QueryResult | None = None

    @property
    def data_dir(self) -> Path:
        return self.db.data_dir

    def switch_directory(self, data_dir: str | os.PathLike[str]) -> Path:
        """Switch to a different database directory cleanly without os.chdir()."""
        self.db = Database(Path(data_dir).resolve())
        self.last_tabular_result = None
        return self.db.data_dir

    def get_metadata(self, filter_text: str = '') -> list[TableMeta]:
        """Load table list with row counts and column definitions, optionally filtered."""
        needle = filter_text.strip().lower()
        metas: list[TableMeta] = []
        for name, _, defs in self.db.list_tables():
            if needle and needle not in name.lower():
                continue
            table = self.db.get_table(name)
            row_count = table.count_rows() if table else 0
            cols = tuple((col_name, col_type) for col_name, col_type in defs.items())
            metas.append(TableMeta(name=name, row_count=row_count, columns=cols))
        return metas

    def execute_script(self, script: str) -> BatchExecutionReport:
        """Execute one or more semicolon-separated SQL statements sequentially."""
        statements = split_sql_statements(script)
        entries: list[ExecutionEntry] = []
        last_tabular: QueryResult | None = None
        schema_changed = False
        error_count = 0
        batch_start = time.perf_counter()

        for sql in statements:
            ts = datetime.now().strftime('%H:%M:%S')
            t0 = time.perf_counter()
            try:
                res = self.db.execute(sql)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                entry = ExecutionEntry(sql=sql, timestamp=ts, elapsed_ms=elapsed_ms, ok=True, result=res)
                entries.append(entry)
                self.history.append(entry)
                if res.is_tabular:
                    last_tabular = res
                    self.last_tabular_result = res
                if res.kind in (
                    StatementKind.DDL,
                    StatementKind.INSERT,
                    StatementKind.UPDATE,
                    StatementKind.DELETE,
                    StatementKind.IMPORT,
                ):
                    schema_changed = True
            except TxtSqlError as exc:
                elapsed_ms = (time.perf_counter() - t0) * 1000
                entry = ExecutionEntry(sql=sql, timestamp=ts, elapsed_ms=elapsed_ms, ok=False, error=str(exc))
                entries.append(entry)
                self.history.append(entry)
                error_count += 1
                break
            except Exception as exc:
                elapsed_ms = (time.perf_counter() - t0) * 1000
                entry = ExecutionEntry(
                    sql=sql, timestamp=ts, elapsed_ms=elapsed_ms, ok=False, error=f'系统错误: {exc}')
                entries.append(entry)
                self.history.append(entry)
                error_count += 1
                break

        total_ms = (time.perf_counter() - batch_start) * 1000
        return BatchExecutionReport(
            entries=entries,
            last_tabular=last_tabular,
            schema_changed=schema_changed,
            total_elapsed_ms=total_ms,
            error_count=error_count,
        )

    def export_last_result(self, filepath: str | os.PathLike[str]) -> int:
        """Export the most recent tabular QueryResult to .xlsx, .csv, or .tsv."""
        if self.last_tabular_result is None or not self.last_tabular_result.columns:
            raise EngineError('当前没有可导出的查询结果')
        return export_rows(
            self.last_tabular_result.rows,
            list(self.last_tabular_result.columns),
            filepath,
        )

    def import_excel(self, table_name: str, file_path: str) -> ExecutionEntry:
        """Import an Excel (.xlsx) worksheet into a new table."""
        tname = table_name.strip()
        if not _IDENT_RE.match(tname):
            raise ValueError(f'表名 "{tname}" 不合法，需以字母或下划线开头，仅含字母/数字/下划线。')
        escaped_path = file_path.replace("'", "''")
        sql = f"IMPORT {tname} FROM '{escaped_path}';"
        report = self.execute_script(sql)
        return report.entries[0]
