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

SQL_KEYWORDS: tuple[str, ...] = (
    'CREATE', 'DROP', 'TABLE', 'INDEX', 'INDEXES', 'IF', 'NOT', 'EXISTS', 'INSERT', 'INTO', 'VALUES',
    'SELECT', 'DISTINCT', 'FROM', 'WHERE', 'AND', 'OR', 'IS', 'NULL', 'TRUE', 'FALSE',
    'IN', 'BETWEEN', 'LIKE', 'ANY', 'ALL', 'UNION', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
    'FULL', 'OUTER', 'CROSS', 'NATURAL', 'ON', 'USING', 'AS', 'GROUP', 'BY', 'HAVING',
    'ORDER', 'ASC', 'DESC', 'LIMIT', 'OFFSET', 'UPDATE', 'SET', 'DELETE', 'IMPORT',
    'OUTFILE', 'SHOW', 'TABLES', 'DESCRIBE', 'EXPLAIN', 'ANALYZE', 'CHECK',
    'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
    'STRING', 'NUMBER', 'VARCHAR', 'DECIMAL', 'INT',
)
AGG_FUNCTIONS: tuple[str, ...] = ('COUNT', 'SUM', 'AVG', 'MIN', 'MAX')
_SQL_KEYWORD_SET = frozenset(SQL_KEYWORDS)
_TABLE_CONTEXT_KW = frozenset({'FROM', 'JOIN', 'INTO', 'UPDATE', 'DESCRIBE', 'TABLE', 'IMPORT'})
_IDENT_TOKEN_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')


@dataclass(frozen=True, slots=True)
class TableMeta:
    """Metadata summary for one table in the sidebar tree."""
    name: str
    row_count: int
    columns: tuple[tuple[str, str], ...]
    indexes: tuple[tuple[str, str], ...] = ()


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
    '5. 索引、优化与执行计划剖析 (EXPLAIN ANALYZE)': (
        "-- 白盒执行剖析演示：二级索引 + 谓词下推 + Hash Join\n"
        "DROP TABLE IF EXISTS demo_orders;\n"
        "DROP TABLE IF EXISTS demo_users;\n"
        "CREATE TABLE demo_users (id NUMBER, name STRING, city STRING);\n"
        "INSERT INTO demo_users VALUES\n"
        "    (1, '张三', 'Harbin'), (2, '李四', 'Beijing'), (3, '王五', 'Harbin');\n"
        "CREATE TABLE demo_orders (oid NUMBER, uid NUMBER, amount NUMBER);\n"
        "INSERT INTO demo_orders VALUES\n"
        "    (101, 1, 299), (102, 1, 520), (103, 2, 180), (104, 3, 660);\n"
        "CREATE INDEX idx_users_city ON demo_users (city);\n"
        "CREATE INDEX idx_orders_uid ON demo_orders (uid);\n"
        "EXPLAIN ANALYZE SELECT u.name, o.oid, o.amount\n"
        "FROM demo_users u JOIN demo_orders o ON u.id = o.uid\n"
        "WHERE u.city = 'Harbin' AND o.amount >= 300\n"
        "ORDER BY o.amount DESC;"
    ),
}


@dataclass(frozen=True)
class StatementSpan:
    """Span of an executable SQL statement inside a script."""
    start: int
    end: int
    sql: str


@dataclass(frozen=True)
class CompletionItem:
    """Single SQL code-completion suggestion."""
    label: str
    insert_text: str
    kind: str  # 'column' | 'table' | 'keyword' | 'function'
    detail: str = ''


@dataclass(frozen=True)
class CompletionResult:
    """Completion candidates together with the character range to replace."""
    replace_start: int
    replace_end: int
    prefix: str
    items: tuple[CompletionItem, ...]


def _trim_span_start(script: str, start: int, end: int) -> int:
    pos = start
    while pos < end:
        ch = script[pos]
        if ch.isspace():
            pos += 1
        elif ch == '-' and pos + 1 < end and script[pos + 1] == '-':
            nl = script.find('\n', pos + 2, end)
            if nl == -1:
                return end
            pos = nl + 1
        else:
            break
    return pos


def find_statement_spans(script: str) -> list[StatementSpan]:
    """Locate all executable SQL statements with character spans [start, end)."""
    spans: list[StatementSpan] = []
    n = len(script)
    i = 0
    raw_start = 0
    in_string = False
    in_comment = False

    def _emit(chunk_start: int, chunk_end: int, include_end: int) -> None:
        real_start = _trim_span_start(script, chunk_start, chunk_end)
        body = script[real_start:chunk_end].strip()
        if _has_executable_sql(body):
            spans.append(StatementSpan(start=real_start, end=include_end, sql=body))

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
            _emit(raw_start, i, i + 1)
            raw_start = i + 1
        i += 1

    trimmed_end = len(script.rstrip())
    if raw_start < trimmed_end:
        _emit(raw_start, trimmed_end, trimmed_end)
    return spans


def split_sql_statements(script: str) -> list[str]:
    """Split a SQL script by semicolons outside string literals and -- line comments."""
    return [span.sql for span in find_statement_spans(script)]


def find_active_statement(script: str, cursor_offset: int) -> StatementSpan | None:
    """Return the StatementSpan under cursor_offset (or sharing its lines), if any."""
    spans = find_statement_spans(script)
    if not spans:
        return None

    offset = max(0, min(cursor_offset, len(script)))
    for span in spans:
        if span.start <= offset < span.end:
            return span
    for span in spans:
        if offset == span.end:
            return span

    cursor_line = script.count('\n', 0, offset)
    same_line_spans: list[StatementSpan] = []
    for span in spans:
        start_line = script.count('\n', 0, span.start)
        end_probe = max(span.start, span.end - 1)
        end_line = script.count('\n', 0, end_probe)
        if start_line <= cursor_line <= end_line:
            same_line_spans.append(span)

    if not same_line_spans:
        return None
    if len(same_line_spans) == 1:
        return same_line_spans[0]

    before = [s for s in same_line_spans if s.end <= offset]
    if before:
        return before[-1]
    return min(same_line_spans, key=lambda s: abs(s.start - offset))


def _is_inside_string_or_comment(script: str, cursor_offset: int) -> bool:
    """Return True if cursor_offset lies inside a string literal or -- line comment."""
    limit = max(0, min(cursor_offset, len(script)))
    i = 0
    in_string = False
    in_comment = False
    while i < limit:
        ch = script[i]
        if in_comment:
            if ch == '\n':
                in_comment = False
            i += 1
            continue
        if in_string:
            if ch == "'":
                if i + 1 < limit and script[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
        elif ch == '-' and i + 1 < limit and script[i + 1] == '-':
            in_comment = True
            i += 2
            continue
        i += 1
    return in_string or in_comment


def _mask_strings_and_comments(sql: str) -> str:
    """Replace string literals and -- line comments with spaces while preserving length."""
    chars = list(sql)
    n = len(chars)
    i = 0
    in_string = False
    in_comment = False
    while i < n:
        ch = chars[i]
        if in_comment:
            if ch == '\n':
                in_comment = False
            else:
                chars[i] = ' '
            i += 1
            continue
        if in_string:
            chars[i] = ' '
            if ch == "'":
                if i + 1 < n and chars[i + 1] == "'":
                    chars[i + 1] = ' '
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            chars[i] = ' '
            in_string = True
        elif ch == '-' and i + 1 < n and chars[i + 1] == '-':
            chars[i] = ' '
            chars[i + 1] = ' '
            in_comment = True
            i += 2
            continue
        i += 1
    return ''.join(chars)


_ALIAS_SCAN_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]*|,')


def _extract_table_aliases(sql: str, known_tables: dict[str, TableMeta]) -> tuple[dict[str, str], list[str]]:
    """Extract (alias_lower -> table_name, referenced_tables) from a SQL statement."""
    masked = _mask_strings_and_comments(sql)
    tokens = _ALIAS_SCAN_RE.findall(masked)
    alias_map: dict[str, str] = {}
    referenced: list[str] = []
    seen_tables: set[str] = set()

    def _record(tbl_token: str, alias_token: str | None = None) -> None:
        meta = known_tables.get(tbl_token.lower())
        if meta is None:
            return
        name = meta.name
        if name not in seen_tables:
            seen_tables.add(name)
            referenced.append(name)
        alias_map[name.lower()] = name
        if alias_token and alias_token != ',' and alias_token.upper() not in _SQL_KEYWORD_SET:
            alias_map[alias_token.lower()] = name

    i = 0
    n = len(tokens)
    while i < n:
        upper = tokens[i].upper()
        if upper in ('FROM', 'JOIN'):
            i += 1
            while i < n:
                tbl = tokens[i]
                if tbl == ',' or tbl.upper() in _SQL_KEYWORD_SET:
                    break
                i += 1
                alias = None
                if i < n and tokens[i].upper() == 'AS':
                    i += 1
                    if i < n and tokens[i] != ',' and tokens[i].upper() not in _SQL_KEYWORD_SET:
                        alias = tokens[i]
                        i += 1
                elif i < n and tokens[i] != ',' and tokens[i].upper() not in _SQL_KEYWORD_SET:
                    alias = tokens[i]
                    i += 1
                _record(tbl, alias)
                if i < n and tokens[i] == ',':
                    i += 1
                    continue
                break
            continue
        if upper in ('INTO', 'UPDATE', 'DESCRIBE', 'TABLE', 'IMPORT'):
            idx = i + 1
            if upper == 'TABLE':
                if idx < n and tokens[idx].upper() == 'IF':
                    idx += 1
                    if idx < n and tokens[idx].upper() == 'NOT':
                        idx += 1
                    if idx < n and tokens[idx].upper() == 'EXISTS':
                        idx += 1
            if idx < n and tokens[idx] != ',' and tokens[idx].upper() not in _SQL_KEYWORD_SET:
                alias = None
                if upper == 'UPDATE' and idx + 1 < n:
                    nxt = tokens[idx + 1]
                    if nxt.upper() == 'AS' and idx + 2 < n and tokens[idx + 2].upper() not in _SQL_KEYWORD_SET:
                        alias = tokens[idx + 2]
                    elif nxt != ',' and nxt.upper() not in _SQL_KEYWORD_SET:
                        alias = nxt
                _record(tokens[idx], alias)
        i += 1

    return alias_map, referenced


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
        self.last_plan_result: QueryResult | None = None

    @property
    def data_dir(self) -> Path:
        return self.db.data_dir

    def switch_directory(self, data_dir: str | os.PathLike[str]) -> Path:
        """Switch to a different database directory cleanly without os.chdir()."""
        self.db = Database(Path(data_dir).resolve())
        self.last_tabular_result = None
        self.last_plan_result = None
        return self.db.data_dir

    def get_completions(self, script: str, cursor_offset: int, manual: bool = False) -> CompletionResult | None:
        """Compute context-aware SQL completions for the current cursor offset."""
        offset = max(0, min(cursor_offset, len(script)))
        if _is_inside_string_or_comment(script, offset):
            return None

        metas = self.get_metadata()
        known_tables: dict[str, TableMeta] = {m.name.lower(): m for m in metas}

        active = find_active_statement(script, offset)
        if active is not None:
            stmt_sql = active.sql
            stmt_before_cursor = script[active.start:offset] if offset >= active.start else ''
        else:
            stmt_sql = script[:offset]
            stmt_before_cursor = stmt_sql

        alias_map, referenced_tables = _extract_table_aliases(stmt_sql, known_tables)
        before_cursor = script[:offset]

        # 1. Qualified column completion: table.col or alias.col
        qual_match = re.search(r'([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z0-9_]*)$', before_cursor)
        if qual_match:
            qualifier = qual_match.group(1).lower()
            prefix = qual_match.group(2)
            replace_start = offset - len(prefix)
            target_name = alias_map.get(qualifier)
            if target_name is None and qualifier in known_tables:
                target_name = known_tables[qualifier].name
            if target_name is None:
                return None
            meta = known_tables[target_name.lower()]
            prefix_lower = prefix.lower()
            items = [
                CompletionItem(
                    label=col_name,
                    insert_text=col_name,
                    kind='column',
                    detail=f'{meta.name} ({col_type})',
                )
                for col_name, col_type in meta.columns
                if col_name.lower().startswith(prefix_lower)
            ]
            if not items:
                return None
            return CompletionResult(
                replace_start=replace_start,
                replace_end=offset,
                prefix=prefix,
                items=tuple(items[:20]),
            )

        # 2. Identifier prefix
        id_match = re.search(r'([A-Za-z_][A-Za-z0-9_]*)$', before_cursor)
        prefix = id_match.group(1) if id_match else ''
        replace_start = offset - len(prefix)

        if not prefix and not manual:
            return None

        prefix_lower = prefix.lower()
        masked_before = _mask_strings_and_comments(
            script[active.start:replace_start] if active and replace_start >= active.start else stmt_before_cursor[:max(0, len(stmt_before_cursor) - len(prefix))]
        )
        prev_tokens = _IDENT_TOKEN_RE.findall(masked_before)
        prev_kw = prev_tokens[-1].upper() if prev_tokens else ''

        candidates: list[CompletionItem] = []
        seen: set[tuple[str, str]] = set()

        def _add(item: CompletionItem) -> None:
            key = (item.label.lower(), item.kind)
            if key not in seen and item.label.lower().startswith(prefix_lower):
                seen.add(key)
                candidates.append(item)

        if prev_kw in _TABLE_CONTEXT_KW:
            for meta in metas:
                _add(CompletionItem(
                    label=meta.name,
                    insert_text=meta.name,
                    kind='table',
                    detail=f'表 ({meta.row_count} 行)',
                ))
        else:
            # Priority: active table columns > agg functions > keywords > tables > other columns
            ref_set = {name.lower() for name in referenced_tables}
            for tbl_name in referenced_tables:
                meta = known_tables.get(tbl_name.lower())
                if meta:
                    for col_name, col_type in meta.columns:
                        _add(CompletionItem(
                            label=col_name,
                            insert_text=col_name,
                            kind='column',
                            detail=f'{meta.name} ({col_type})',
                        ))

            for fn in AGG_FUNCTIONS:
                _add(CompletionItem(
                    label=fn,
                    insert_text=fn,
                    kind='function',
                    detail='聚合函数',
                ))

            for kw in SQL_KEYWORDS:
                if kw not in AGG_FUNCTIONS:
                    _add(CompletionItem(
                        label=kw,
                        insert_text=kw,
                        kind='keyword',
                        detail='关键字',
                    ))

            for meta in metas:
                _add(CompletionItem(
                    label=meta.name,
                    insert_text=meta.name,
                    kind='table',
                    detail=f'表 ({meta.row_count} 行)',
                ))

            for meta in metas:
                if meta.name.lower() in ref_set:
                    continue
                for col_name, col_type in meta.columns:
                    _add(CompletionItem(
                        label=col_name,
                        insert_text=col_name,
                        kind='column',
                        detail=f'{meta.name}.{col_name} ({col_type})',
                    ))

        if not candidates:
            return None
        if not manual and len(candidates) == 1 and candidates[0].label.lower() == prefix_lower:
            return None

        return CompletionResult(
            replace_start=replace_start,
            replace_end=offset,
            prefix=prefix,
            items=tuple(candidates[:20]),
        )

    def get_metadata(self, filter_text: str = '') -> list[TableMeta]:
        """Load table list with row counts, column definitions, and secondary indexes."""
        needle = filter_text.strip().lower()
        metas: list[TableMeta] = []
        all_indexes = self.db.storage.list_indexes()
        for name, _, defs in self.db.list_tables():
            if needle and needle not in name.lower():
                continue
            table = self.db.get_table(name)
            row_count = table.count_rows() if table else 0
            cols = tuple((col_name, col_type) for col_name, col_type in defs.items())
            idxs = tuple((m.name, m.column_name) for m in all_indexes if m.table_name == name)
            metas.append(TableMeta(name=name, row_count=row_count, columns=cols, indexes=idxs))
        return metas

    def explain_sql(self, sql: str, analyze: bool = True) -> BatchExecutionReport:
        """Run EXPLAIN [ANALYZE] against a single SQL statement."""
        cleaned = sql.strip().rstrip(';').strip()
        if not cleaned:
            return BatchExecutionReport(entries=[], last_tabular=None, schema_changed=False, total_elapsed_ms=0.0, error_count=0)
        if not re.match(r'^EXPLAIN\b', cleaned, re.IGNORECASE):
            prefix = 'EXPLAIN ANALYZE ' if analyze else 'EXPLAIN '
            cleaned = prefix + cleaned
        return self.execute_script(cleaned + ';')

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
                if res.kind == StatementKind.EXPLAIN and res.plan is not None:
                    self.last_plan_result = res
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
