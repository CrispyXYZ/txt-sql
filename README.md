# TxtSql

A lightweight DBMS that stores data in tab-separated-value (TSV) files with a simple SQL interface.

## Architecture

```
SQL string → Lexer → Tokens → Parser → AST → Executor → Storage (TSV files)
                                  ↓
                            Evaluator (WHERE / HAVING)
```

| Module | Role |
|---|---|
| `lexer.py` | Tokenises SQL text into `Token` stream |
| `parser.py` | Recursive-descent parser producing AST nodes (`ast.py`) |
| `evaluator.py` | Evaluates expressions & compiles predicates with `EvalContext` |
| `executor.py` | `QueryExecutor` runs statements against `StorageEngine` |
| `io_handlers.py` | External `.xlsx` / `.csv` / `.tsv` import and export |
| `storage.py` | Directory-scoped `StorageEngine` & `Table` with atomic TSV writes |
| `result.py` | Structured `QueryResult` and `StatementKind` |
| `engine.py` | `Database(data_dir)` session orchestrator & `execute_sql()` |
| `cli.py` | Interactive REPL with ASCII table output |

## Requirements

- Python >= 3.12
- `openpyxl` (for `IMPORT` from `.xlsx` files)

```bash
pip install openpyxl
```

## Usage

```bash
python -m txtsql
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
<table_name>.txt      — data rows (tab-separated, QUOTE_ALL quoting)
```

- `NULL` values are stored using the `\N` sentinel; literal backslashes in strings are escaped (`\\`), so empty strings `''` and `NULL` are strictly distinguished
- All writes (including `metadata.txt` updates on `DROP TABLE`) are atomic (temp file + rename)
- TSV quoting protects strings containing tabs, newlines, or quotes

## Limitations

- No transactions
- No indexes — all queries scan the full table
- `DISTINCT` applies to the SELECT result row; `ORDER BY` supports column names, qualified names, and SELECT aliases (not arbitrary expressions)

## License

MIT
