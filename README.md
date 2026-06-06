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
| `evaluator.py` | Compiles WHERE/HAVING expressions into row predicates |
| `executor.py` | Executes statements — coordinates storage and evaluator |
| `storage.py` | TSV-file CRUD: `Table` class with atomic writes |
| `engine.py` | Orchestrator: `execute_sql(sql)` runs the full pipeline |
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
CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...);
```
Types: `STRING`, `NUMBER`

### INSERT

```sql
INSERT INTO table_name VALUES (val1, val2, ...);
INSERT INTO table_name (col1, col2) VALUES (val1, val2);
INSERT INTO table_name VALUES (1, 'a'), (2, 'b');
```
Values: `'string'`, `42`, `3.14`, `NULL`, `TRUE`, `FALSE`

### SELECT

```sql
SELECT [DISTINCT] col1, col2, ... FROM table_name
    [WHERE condition]
    [GROUP BY col, ...]
    [HAVING aggregate_condition]
    [ORDER BY col [ASC|DESC], ...]
    [LIMIT n] [OFFSET n];

SELECT COUNT(*) AS cnt FROM table_name;
SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept;
```

### UPDATE

```sql
UPDATE table_name SET col1 = val1, col2 = val2 [WHERE condition];
```

### DELETE

```sql
DELETE FROM table_name [WHERE condition];
```

### DROP TABLE

```sql
DROP TABLE table_name;
```

### IMPORT (Excel)

```sql
IMPORT table_name FROM 'path/to/file.xlsx';
IMPORT table_name (col1 STRING, col2 NUMBER) FROM 'path/to/file.xlsx';
```
- First row must be column headers
- Types are auto-inferred (NUMBER if all values are numeric, otherwise STRING)
- Optional column definitions override auto-inference

### WHERE / HAVING expressions

```sql
WHERE salary > 50000 AND name <> 'Bob'
WHERE dept = 'Sales' OR dept = 'Eng'
WHERE score IS NULL
WHERE (a > 1 AND b < 2) OR c = 3
```

- `=`, `<>`, `>`, `<`, `>=`, `<=`
- `AND`, `OR`, parentheses
- `IS NULL`, `IS NOT NULL`
- `NULL` comparisons always return false (SQL standard)

## Storage Format

Tables are stored as TSV files in the working directory:

```
metadata.txt          — table registry (name, column count, col_name, type, ...)
<table_name>.txt      — data rows (tab-separated, QUOTE_NONNUMERIC quoting)
```

- NULL values are stored as empty fields
- All writes are atomic (temp file + rename)
- TSV quoting protects strings containing tabs or newlines

## Limitations

- No joins, subqueries, or transactions
- No indexes — all queries scan the full table
- Strings containing literal `\N` are safe (NULL uses empty field, not `\N` sentinel)
- AS keyword is mandatory for aggregate aliases: `COUNT(*) AS cnt`
- DISTINCT only applies to non-aggregate queries
- No escaped quotes inside string literals

## License

MIT
