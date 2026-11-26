# TxtSql

A lightweight DBMS that stores data in tab-separated-value (TSV) files with a simple SQL interface.

## Architecture

```
SQL string → Lexer → Tokens → Parser → AST → Optimizer → Executor → Storage (TSV + Indexes)
                                                ↓           ↓
                                          Plan / Trace   Evaluator (3VL)
```

| Module | Role |
|---|---|
| `lexer.py` | Tokenises SQL text into `Token` stream |
| `parser.py` | Recursive-descent parser producing AST nodes (`ast.py`) |
| `index.py` | In-memory hybrid Hash + sorted-array (`bisect`) secondary indexes (`ColumnIndex`) |
| `optimizer.py` | Predicate pushdown (outer-join safe), implicit comma-join rewrite, equi-join key extraction |
| `plan.py` | `PlanNode` tree, runtime operator metrics, token & AST serialization (`EXPLAIN [ANALYZE]`) |
| `evaluator.py` | Evaluates expressions & compiles SQL three-valued-logic predicates (`EvalContext`) |
| `executor.py` | `QueryExecutor` runs statements (Index Scan, Hash Join, `EXPLAIN`, transactions, `CHECK TABLE`) |
| `io_handlers.py` | External `.xlsx` / `.csv` / `.tsv` import and export |
| `storage.py` | Directory-scoped `StorageEngine` & `Table` with `(mtime_ns, size)` cache, `indexes.txt`, and snapshots |
| `result.py` | Structured `QueryResult` (including `plan`, `tokens_info`, `ast_tree`) and `StatementKind` |
| `engine.py` | `Database(data_dir, enable_optimizations=True)` orchestrator & `execute_sql()` |
| `cli.py` | Interactive REPL with ASCII table output |
| `gui_controller.py` | Headless GUI controller (script spans, context autocomplete, history, metadata, formatting) |
| `gui.py` | Tkinter desktop GUI client (`txtsql-gui`) with active-statement highlight & plan visualizer |

## Requirements

- Python >= 3.12
- `openpyxl` (for `IMPORT` from `.xlsx` files)

```bash
pip install openpyxl
```

## Usage

CLI (interactive REPL):

```bash
python -m txtsql [--dir ./data]
# or: txtsql
```

GUI (Tkinter desktop client):

```bash
python -m txtsql --gui [--dir ./data]
# or: txtsql-gui
```

Python API (directory-scoped session & structured results):

```python
from txtsql import Database

db = Database("./my_data")
res = db.execute("SELECT id, name FROM employees ORDER BY id ASC;")
print(res.columns, res.rows)
```

```
Welcome to TxtSql CLI. Type your SQL statement and press Enter.
txtsql> CREATE TABLE employees (id NUMBER, name STRING, salary NUMBER)
txtsql> INSERT INTO employees VALUES (1, 'Alice', 75000)
txtsql> INSERT INTO employees VALUES (2, 'Bob', 62000)
txtsql> SELECT * FROM employees ORDER BY salary DESC
+----+-------+--------+
| id | name  | salary |
+----+-------+--------+
| 1  | Alice | 75000  |
| 2  | Bob   | 62000  |
+----+-------+--------+
```

## SQL Reference

### CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] table_name (col1 TYPE, col2 TYPE, ...);
```
Types: `STRING`, `NUMBER` (`INT` / `DECIMAL` / `VARCHAR` accepted as aliases)

### INSERT

```sql
INSERT INTO table_name VALUES (val1, val2, ...);
INSERT INTO table_name (col1, col2) VALUES (val1, val2);
INSERT INTO table_name VALUES (1, 'a'), (2, 'b');
INSERT INTO dst (id, name) SELECT id, name FROM src WHERE active = TRUE;
```
Values: `'string'`, `42`, `3.14`, `-5`, `NULL`, `TRUE`, `FALSE`

### SELECT

```sql
SELECT [DISTINCT] *, table.*, col1, col2, ... FROM table_name
    [WHERE condition]
    [GROUP BY col, ...]
    [HAVING aggregate_condition]
    [ORDER BY col [ASC|DESC], ...]
    [LIMIT n] [OFFSET n];

SELECT COUNT(*) AS cnt, COUNT(DISTINCT dept) AS depts FROM emp;
SELECT COUNT(*), dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept HAVING COUNT(*) > 1;
```

### Subqueries & UNION

```sql
SELECT * FROM a WHERE id IN (SELECT id FROM b);
SELECT * FROM a WHERE EXISTS (SELECT * FROM b WHERE b.id = a.id);
SELECT * FROM a WHERE salary > (SELECT AVG(salary) FROM a);
SELECT * FROM a WHERE score > ANY (SELECT min_score FROM thresholds);
SELECT id, name FROM a UNION SELECT id, name FROM b ORDER BY id;
SELECT id, name FROM a UNION ALL SELECT id, name FROM b;
```

### JOIN

```sql
SELECT * FROM a JOIN b ON a.id = b.id;
SELECT * FROM a LEFT JOIN b USING (id);
SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id;
SELECT * FROM a FULL OUTER JOIN b USING (id);
SELECT * FROM a CROSS JOIN b;
SELECT * FROM a NATURAL LEFT JOIN b;
SELECT * FROM a, b WHERE a.id = b.id;   -- comma is a cross join + WHERE
```

- `JOIN` / `INNER JOIN` / `LEFT [OUTER] JOIN` / `RIGHT [OUTER] JOIN` / `FULL [OUTER] JOIN`
- `CROSS JOIN` (also written as a comma-separated `FROM` list)
- `NATURAL [INNER | LEFT | RIGHT | FULL] JOIN` matches all common column names
- `JOIN ... USING (col1, col2)` matches the listed columns and merges them in `SELECT *`
- `SELECT *` uses unqualified column names; when a name is duplicated across tables, the later columns keep their table qualifier

### UPDATE

```sql
UPDATE table_name SET col1 = expr1, col2 = expr2 [WHERE condition];
```

### DELETE

```sql
DELETE FROM table_name [WHERE condition];
```

### DROP TABLE

```sql
DROP TABLE [IF EXISTS] table_name;
```

### Secondary Indexes

```sql
CREATE INDEX [IF NOT EXISTS] idx_name ON table_name (column_name);
DROP INDEX [IF EXISTS] idx_name;
SHOW INDEXES [FROM table_name];
```
- Hybrid in-memory Hash (`=` / `IN`) + sorted array `bisect` (`>` / `>=` / `<` / `<=` / `BETWEEN`) secondary index
- Index metadata persisted in `indexes.txt`; automatically invalidated and rebuilt when `.txt` files are updated via SQL or edited externally
- `DROP TABLE` automatically cascades and removes associated indexes

### EXPLAIN & EXPLAIN ANALYZE

```sql
EXPLAIN SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.uid WHERE u.city = 'Harbin';
EXPLAIN ANALYZE SELECT * FROM scores WHERE grade BETWEEN 80 AND 100;
```
- `EXPLAIN` prints the logical/physical plan tree highlighting `IndexScan`, `HashJoin`, and `Predicate Pushdown`
- `EXPLAIN ANALYZE` executes the query and reports per-operator `RowsIn`, `RowsOut`, `Selectivity`, and `Time(ms)`

### Integrity Audit & Snapshot Transactions

```sql
CHECK TABLE table_name;

BEGIN [TRANSACTION];
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
ROLLBACK [TRANSACTION];   -- or COMMIT [TRANSACTION];
```
- `CHECK TABLE` scans the TSV file and reports exact 1-based line numbers for column-count mismatches or invalid `NUMBER` values
- `BEGIN` / `COMMIT` / `ROLLBACK` provide directory-level atomic snapshot rollback across DML, DDL, and index changes

### SHOW TABLES / DESCRIBE (system commands)

```sql
SHOW TABLES;
DESCRIBE table_name;
```
- `SHOW TABLES` lists all tables with column names and row counts
- `DESCRIBE` shows column definitions and row count for a specific table

### IMPORT (Excel)

```sql
IMPORT table_name FROM 'path/to/file.xlsx';
IMPORT table_name (col1 STRING, col2 NUMBER) FROM 'path/to/file.xlsx';
```
- First row must be column headers
- Types are auto-inferred (NUMBER if all values are numeric, otherwise STRING)
- Optional column definitions override auto-inference

### EXPORT (query results to file)

```sql
SELECT * FROM table_name INTO OUTFILE 'path/to/data.xlsx';
SELECT dept, AVG(salary) AS avg FROM emp GROUP BY dept INTO OUTFILE 'report.csv';
```
- Format determined by file extension: `.xlsx`, `.csv`, `.tsv`
- Headers are automatically included in the output
- Returns number of rows exported

### WHERE / HAVING expressions

```sql
WHERE salary > 50000 AND NOT (name = 'Bob')
WHERE dept IN ('Sales', 'Eng') OR dept != 'HR'
WHERE salary BETWEEN 5000 AND 10000 AND name LIKE 'A%'
WHERE score IS NULL
WHERE (a > 1 AND b < 2) OR c = 3
WHERE price * qty > 100
```

- `=`, `<>`, `!=`, `>`, `<`, `>=`, `<=`
- Arithmetic operators `+`, `-`, `*`, `/` (with `NULL` propagation)
- `AND`, `OR`, `NOT`, parentheses (SQL three-valued logic)
- `IS NULL`, `IS NOT NULL`, `[NOT] IN (list | subquery)`, `[NOT] BETWEEN ... AND ...`, `[NOT] LIKE` (`%`, `_`)
- `ANY`, `ALL`, `EXISTS`, `NOT EXISTS`, and scalar `(SELECT ...)` subqueries
- `TRUE` / `FALSE` literals evaluate to `NUMBER` `1` / `0`
- Escaped single quotes (`''`) inside string literals and `--` single-line SQL comments

## Storage Format

Tables are stored as TSV files in the working directory:

```
metadata.txt          — table registry (name, column count, col_name, type, ...)
indexes.txt           — secondary index registry (index_name, table_name, column_name)
<table_name>.txt      — data rows (tab-separated, QUOTE_ALL quoting)
```

- `NULL` values are stored using the `\N` sentinel; literal backslashes in strings are escaped (`\\`), so empty strings `''` and `NULL` are strictly distinguished
- All writes (including `metadata.txt`, `indexes.txt`, and transaction rollback restores) are atomic (temp file + rename)
- Row and index caches track `(st_mtime_ns, st_size)` file fingerprints to automatically detect external edits
- TSV quoting protects strings containing tabs, newlines, or quotes

## Query Optimization & Benchmarks

TxtSql includes an automated optimizer pipeline (`Database(data_dir, enable_optimizations=True)`) featuring Hash Join (`O(M + N)`), outer-join-safe Predicate Pushdown, implicit comma-join rewriting, and hybrid Hash/`bisect` Index Scan.

Run the benchmark suite locally:

```bash
.venv/bin/python benchmarks/run_benchmark.py
```

Measured local results (with result-equivalence assertions against `enable_optimizations=False`):

| Scenario | Scale | Unoptimized (ms) | Optimized (ms) | Speedup | Mechanism |
|---|---|---|---|---|---|
| Equi-Join (`JOIN`) | 1,000 × 1,000 rows | 2220.82 | 3.65 | **608.1x** | Hash Join (`O(M+N)`) |
| Equi-Join (`JOIN`) | 2,000 × 2,000 rows | 9154.34 | 8.92 | **1026.4x** | Hash Join (`O(M+N)`) |
| Point Lookup (`=`) | 10,000 rows | 15.73 | 0.16 | **98.2x** | Hash Secondary Index (`IndexScan`) |
| Range Query (`BETWEEN`) | 10,000 rows | 24.88 | 0.23 | **106.9x** | Sorted Array `bisect` |
| Comma Join + Filters | 1,200 × 1,200 rows | 7294.73 | 2.83 | **2580.8x** | Index + Pushdown + Implicit Rewrite + Hash Join |

## Limitations

- Single-session directory-level snapshot transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) without multi-client concurrency locks or WAL recovery
- Secondary indexes are rebuilt in memory from TSV files on first query or file modification rather than stored as on-disk B+ trees
- `DISTINCT` applies to the SELECT result row; `ORDER BY` supports column names, qualified names, and SELECT aliases (not arbitrary expressions)

## License

MIT
