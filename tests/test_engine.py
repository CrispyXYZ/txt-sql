"""End-to-end tests for the TxtSQL engine covering all CRUD operations."""
import os
import tempfile
from decimal import Decimal

import pytest

from txtsql.engine import execute_sql
from txtsql.exceptions import (
    ColumnNotFoundError, EngineError, SqlSyntaxError,
    TableAlreadyExistsError, TableNotFoundError, TypeMismatchError,
)


@pytest.fixture(autouse=True)
def isolated_dir(tmp_path, monkeypatch):
    """Run each test in its own temporary directory so metadata files don't interfere."""
    monkeypatch.chdir(tmp_path)


# ---------------------------------------------------------------------------
# CREATE / DROP
# ---------------------------------------------------------------------------

class TestCreateDrop:
    def test_create_and_drop(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING)')
        execute_sql('DROP TABLE t')

    def test_create_duplicate_raises(self):
        execute_sql('CREATE TABLE t (id NUMBER)')
        with pytest.raises(TableAlreadyExistsError):
            execute_sql('CREATE TABLE t (id NUMBER)')

    def test_create_multiple_types(self):
        execute_sql('CREATE TABLE t (n NUMBER, s STRING)')
        execute_sql('DROP TABLE t')


# ---------------------------------------------------------------------------
# INSERT
# ---------------------------------------------------------------------------

class TestInsert:
    def setup_method(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING, score NUMBER)')

    def teardown_method(self):
        try:
            execute_sql('DROP TABLE t')
        except Exception:
            pass

    def test_insert_all_columns(self):
        execute_sql("INSERT INTO t VALUES (1, 'Alice', 95)")
        rows = execute_sql('SELECT * FROM t')
        assert len(rows) == 1
        assert rows[0]['id'] == Decimal('1')
        assert rows[0]['name'] == 'Alice'
        assert rows[0]['score'] == Decimal('95')

    def test_insert_named_columns(self):
        execute_sql("INSERT INTO t (id, name) VALUES (2, 'Bob')")
        rows = execute_sql('SELECT * FROM t')
        assert rows[0]['id'] == Decimal('2')
        assert rows[0]['name'] == 'Bob'
        assert rows[0]['score'] is None  # unspecified -> NULL

    def test_insert_multiple_rows(self):
        execute_sql("INSERT INTO t VALUES (1, 'A', 10), (2, 'B', 20)")
        rows = execute_sql('SELECT * FROM t ORDER BY id ASC')
        assert len(rows) == 2
        assert rows[0]['name'] == 'A'
        assert rows[1]['name'] == 'B'

    def test_insert_null_value(self):
        execute_sql('INSERT INTO t VALUES (1, NULL, 50)')
        rows = execute_sql('SELECT * FROM t')
        assert rows[0]['name'] is None

    def test_insert_decimal_number(self):
        execute_sql('INSERT INTO t VALUES (1, NULL, 3.14)')
        rows = execute_sql('SELECT * FROM t')
        assert rows[0]['score'] == Decimal('3.14')


# ---------------------------------------------------------------------------
# SELECT
# ---------------------------------------------------------------------------

class TestSelect:
    def setup_method(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING, score NUMBER)')
        execute_sql("INSERT INTO t VALUES (1, 'Alice', 95)")
        execute_sql("INSERT INTO t VALUES (2, 'Bob', 80)")
        execute_sql("INSERT INTO t VALUES (3, 'Charlie', 95)")
        execute_sql("INSERT INTO t VALUES (4, 'Dave', NULL)")

    def teardown_method(self):
        try:
            execute_sql('DROP TABLE t')
        except Exception:
            pass

    def test_select_all(self):
        rows = execute_sql('SELECT * FROM t')
        assert len(rows) == 4

    def test_select_columns(self):
        rows = execute_sql('SELECT id, name FROM t ORDER BY id ASC')
        assert list(rows[0].keys()) == ['id', 'name']

    def test_select_where_gt(self):
        rows = execute_sql('SELECT * FROM t WHERE score > 85')
        assert all(r['score'] > 85 for r in rows)
        assert len(rows) == 2

    def test_select_where_eq_string(self):
        rows = execute_sql("SELECT * FROM t WHERE name = 'Alice'")
        assert len(rows) == 1
        assert rows[0]['name'] == 'Alice'

    def test_select_where_and(self):
        rows = execute_sql("SELECT * FROM t WHERE score >= 80 AND score < 95")
        assert len(rows) == 1
        assert rows[0]['name'] == 'Bob'

    def test_select_where_or(self):
        rows = execute_sql("SELECT * FROM t WHERE name = 'Alice' OR name = 'Bob'")
        assert len(rows) == 2

    def test_select_where_is_null(self):
        rows = execute_sql('SELECT * FROM t WHERE score IS NULL')
        assert len(rows) == 1
        assert rows[0]['name'] == 'Dave'

    def test_select_where_is_not_null(self):
        rows = execute_sql('SELECT * FROM t WHERE score IS NOT NULL')
        assert len(rows) == 3

    def test_select_distinct(self):
        rows = execute_sql('SELECT DISTINCT score FROM t WHERE score IS NOT NULL')
        scores = {r['score'] for r in rows}
        assert scores == {Decimal('80'), Decimal('95')}

    def test_select_order_by_asc(self):
        rows = execute_sql('SELECT * FROM t WHERE score IS NOT NULL ORDER BY score ASC')
        scores = [r['score'] for r in rows]
        assert scores == sorted(scores)

    def test_select_order_by_desc(self):
        rows = execute_sql('SELECT * FROM t WHERE score IS NOT NULL ORDER BY score DESC')
        scores = [r['score'] for r in rows]
        assert scores == sorted(scores, reverse=True)

    def test_select_order_by_null_last_asc(self):
        rows = execute_sql('SELECT * FROM t ORDER BY score ASC')
        assert rows[-1]['score'] is None  # NULL last in ASC

    def test_select_order_by_null_last_desc(self):
        rows = execute_sql('SELECT * FROM t ORDER BY score DESC')
        assert rows[-1]['score'] is None  # NULL last in DESC

    def test_select_limit(self):
        rows = execute_sql('SELECT * FROM t ORDER BY id ASC LIMIT 2')
        assert len(rows) == 2
        assert rows[0]['id'] == Decimal('1')

    def test_select_offset(self):
        rows = execute_sql('SELECT * FROM t ORDER BY id ASC LIMIT 10 OFFSET 2')
        assert rows[0]['id'] == Decimal('3')

    def test_select_decimal_comparison(self):
        execute_sql('DROP TABLE t')
        execute_sql('CREATE TABLE prices (id NUMBER, price NUMBER)')
        execute_sql('INSERT INTO prices VALUES (1, 3.14)')
        execute_sql('INSERT INTO prices VALUES (2, 9.99)')
        rows = execute_sql('SELECT * FROM prices WHERE price > 5.0')
        assert len(rows) == 1
        assert rows[0]['price'] == Decimal('9.99')
        execute_sql('DROP TABLE prices')


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------

class TestUpdate:
    def setup_method(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING, score NUMBER)')
        execute_sql("INSERT INTO t VALUES (1, 'Alice', 95)")
        execute_sql("INSERT INTO t VALUES (2, 'Bob', 80)")
        execute_sql("INSERT INTO t VALUES (3, 'Charlie', 70)")

    def teardown_method(self):
        try:
            execute_sql('DROP TABLE t')
        except Exception:
            pass

    def test_update_with_where(self):
        n = execute_sql("UPDATE t SET score = 100 WHERE name = 'Alice'")
        assert n == 1
        rows = execute_sql("SELECT * FROM t WHERE name = 'Alice'")
        assert rows[0]['score'] == Decimal('100')

    def test_update_all_rows(self):
        n = execute_sql('UPDATE t SET score = 0')
        assert n == 3
        rows = execute_sql('SELECT * FROM t')
        assert all(r['score'] == Decimal('0') for r in rows)

    def test_update_multiple_columns(self):
        n = execute_sql("UPDATE t SET name = 'Eve', score = 88 WHERE id = 2")
        assert n == 1
        rows = execute_sql('SELECT * FROM t WHERE id = 2')
        assert rows[0]['name'] == 'Eve'
        assert rows[0]['score'] == Decimal('88')

    def test_update_set_null(self):
        n = execute_sql('UPDATE t SET score = NULL WHERE id = 1')
        assert n == 1
        rows = execute_sql('SELECT * FROM t WHERE id = 1')
        assert rows[0]['score'] is None

    def test_update_where_no_match(self):
        n = execute_sql('UPDATE t SET score = 0 WHERE id = 999')
        assert n == 0


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------

class TestDelete:
    def setup_method(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING)')
        execute_sql("INSERT INTO t VALUES (1, 'Alice')")
        execute_sql("INSERT INTO t VALUES (2, 'Bob')")
        execute_sql("INSERT INTO t VALUES (3, 'Charlie')")

    def teardown_method(self):
        try:
            execute_sql('DROP TABLE t')
        except Exception:
            pass

    def test_delete_with_where(self):
        n = execute_sql('DELETE FROM t WHERE id = 2')
        assert n == 1
        rows = execute_sql('SELECT * FROM t')
        assert len(rows) == 2
        ids = {r['id'] for r in rows}
        assert Decimal('2') not in ids

    def test_delete_all(self):
        n = execute_sql('DELETE FROM t')
        assert n == 3
        rows = execute_sql('SELECT * FROM t')
        assert len(rows) == 0

    def test_delete_where_no_match(self):
        n = execute_sql('DELETE FROM t WHERE id = 999')
        assert n == 0
        rows = execute_sql('SELECT * FROM t')
        assert len(rows) == 3

    def test_delete_with_complex_where(self):
        n = execute_sql("DELETE FROM t WHERE id > 1 AND name <> 'Charlie'")
        assert n == 1
        rows = execute_sql('SELECT * FROM t ORDER BY id ASC')
        assert len(rows) == 2
        assert rows[0]['name'] == 'Alice'
        assert rows[1]['name'] == 'Charlie'


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_data_persists(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING)')
        execute_sql("INSERT INTO t VALUES (1, 'Alice')")
        execute_sql("INSERT INTO t VALUES (2, 'Bob')")

        rows = execute_sql('SELECT * FROM t ORDER BY id ASC')
        assert len(rows) == 2
        assert rows[0]['name'] == 'Alice'
        assert rows[1]['name'] == 'Bob'
        execute_sql('DROP TABLE t')

    def test_null_persists(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING)')
        execute_sql('INSERT INTO t VALUES (1, NULL)')
        rows = execute_sql('SELECT * FROM t')
        assert rows[0]['name'] is None
        execute_sql('DROP TABLE t')


# ---------------------------------------------------------------------------
# Aggregate queries
# ---------------------------------------------------------------------------

class TestAggregate:
    def setup_method(self):
        execute_sql('CREATE TABLE emp (id NUMBER, dept STRING, salary NUMBER)')
        execute_sql("INSERT INTO emp VALUES (1, 'Sales', 5000)")
        execute_sql("INSERT INTO emp VALUES (2, 'Sales', 6000)")
        execute_sql("INSERT INTO emp VALUES (3, 'Eng', 8000)")
        execute_sql("INSERT INTO emp VALUES (4, 'Eng', 9000)")
        execute_sql("INSERT INTO emp VALUES (5, 'Eng', NULL)")

    def teardown_method(self):
        try:
            execute_sql('DROP TABLE emp')
        except Exception:
            pass

    def test_count_star(self):
        rows = execute_sql('SELECT COUNT(*) AS cnt FROM emp')
        assert len(rows) == 1
        assert rows[0]['cnt'] == Decimal('5')

    def test_count_column_excludes_null(self):
        rows = execute_sql('SELECT COUNT(salary) AS cnt FROM emp')
        assert rows[0]['cnt'] == Decimal('4')

    def test_sum(self):
        rows = execute_sql('SELECT SUM(salary) AS total FROM emp')
        assert rows[0]['total'] == Decimal('28000')

    def test_avg(self):
        rows = execute_sql('SELECT AVG(salary) AS avg_sal FROM emp')
        assert rows[0]['avg_sal'] == Decimal('7000')

    def test_min(self):
        rows = execute_sql('SELECT MIN(salary) AS min_sal FROM emp')
        assert rows[0]['min_sal'] == Decimal('5000')

    def test_max(self):
        rows = execute_sql('SELECT MAX(salary) AS max_sal FROM emp')
        assert rows[0]['max_sal'] == Decimal('9000')

    def test_agg_with_where(self):
        rows = execute_sql("SELECT COUNT(*) AS cnt FROM emp WHERE dept = 'Eng'")
        assert rows[0]['cnt'] == Decimal('3')

    def test_group_by(self):
        rows = execute_sql('SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept ORDER BY dept ASC')
        assert len(rows) == 2
        eng = next(r for r in rows if r['dept'] == 'Eng')
        sales = next(r for r in rows if r['dept'] == 'Sales')
        assert eng['cnt'] == Decimal('3')
        assert sales['cnt'] == Decimal('2')

    def test_group_by_sum(self):
        rows = execute_sql('SELECT dept, SUM(salary) AS total FROM emp GROUP BY dept ORDER BY dept ASC')
        eng = next(r for r in rows if r['dept'] == 'Eng')
        sales = next(r for r in rows if r['dept'] == 'Sales')
        assert eng['total'] == Decimal('17000')
        assert sales['total'] == Decimal('11000')

    def test_group_by_avg(self):
        rows = execute_sql('SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept ORDER BY dept ASC')
        eng = next(r for r in rows if r['dept'] == 'Eng')
        sales = next(r for r in rows if r['dept'] == 'Sales')
        assert eng['avg_sal'] == Decimal('8500')
        assert sales['avg_sal'] == Decimal('5500')

    def test_group_by_min_max(self):
        rows = execute_sql('SELECT dept, MIN(salary) AS lo, MAX(salary) AS hi FROM emp GROUP BY dept ORDER BY dept ASC')
        eng = next(r for r in rows if r['dept'] == 'Eng')
        sales = next(r for r in rows if r['dept'] == 'Sales')
        assert eng['lo'] == Decimal('8000')
        assert eng['hi'] == Decimal('9000')
        assert sales['lo'] == Decimal('5000')
        assert sales['hi'] == Decimal('6000')

    def test_having(self):
        rows = execute_sql('SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept HAVING cnt > 2')
        assert len(rows) == 1
        assert rows[0]['dept'] == 'Eng'

    def test_agg_null_returns_none_for_empty_group(self):
        # SUM/AVG/MIN/MAX on a group with all-NULL values should return None
        execute_sql('CREATE TABLE nulltest (x NUMBER)')
        execute_sql('INSERT INTO nulltest VALUES (NULL)')
        try:
            rows = execute_sql('SELECT SUM(x) AS s FROM nulltest')
            assert rows[0]['s'] is None
            rows = execute_sql('SELECT AVG(x) AS a FROM nulltest')
            assert rows[0]['a'] is None
            rows = execute_sql('SELECT MIN(x) AS lo FROM nulltest')
            assert rows[0]['lo'] is None
            rows = execute_sql('SELECT MAX(x) AS hi FROM nulltest')
            assert rows[0]['hi'] is None
        finally:
            execute_sql('DROP TABLE nulltest')


# ---------------------------------------------------------------------------
# NULL comparison semantics (standard SQL)
# ---------------------------------------------------------------------------

class TestNullComparison:
    def setup_method(self):
        execute_sql('CREATE TABLE t (id NUMBER, score NUMBER)')
        execute_sql('INSERT INTO t VALUES (1, 100)')
        execute_sql('INSERT INTO t VALUES (2, NULL)')
        execute_sql('INSERT INTO t VALUES (3, 200)')

    def teardown_method(self):
        try:
            execute_sql('DROP TABLE t')
        except Exception:
            pass

    def test_null_eq_null_returns_no_rows(self):
        rows = execute_sql('SELECT * FROM t WHERE score = NULL')
        assert len(rows) == 0

    def test_null_ne_null_returns_no_rows(self):
        rows = execute_sql('SELECT * FROM t WHERE score <> NULL')
        assert len(rows) == 0

    def test_null_eq_non_null_returns_no_rows(self):
        rows = execute_sql('SELECT * FROM t WHERE score = 100')
        assert len(rows) == 1

    def test_is_null_finds_null_rows(self):
        rows = execute_sql('SELECT * FROM t WHERE score IS NULL')
        assert len(rows) == 1
        assert rows[0]['id'] == Decimal('2')

    def test_is_not_null_excludes_null_rows(self):
        rows = execute_sql('SELECT * FROM t WHERE score IS NOT NULL')
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# Aggregate validation
# ---------------------------------------------------------------------------

class TestAggregateValidation:
    def setup_method(self):
        execute_sql('CREATE TABLE emp (id NUMBER, dept STRING, name STRING, salary NUMBER)')

    def teardown_method(self):
        try:
            execute_sql('DROP TABLE emp')
        except Exception:
            pass

    def test_agg_with_plain_col_no_group_by_raises_error(self):
        with pytest.raises(SqlSyntaxError, match='GROUP BY'):
            execute_sql('SELECT dept, COUNT(*) AS cnt FROM emp')

    def test_agg_with_non_grouped_col_raises_error(self):
        with pytest.raises(SqlSyntaxError, match='GROUP BY'):
            execute_sql('SELECT dept, name, COUNT(*) AS cnt FROM emp GROUP BY dept')

    def test_having_without_group_by(self):
        execute_sql("INSERT INTO emp VALUES (1, 'Sales', 'Alice', 5000)")
        execute_sql("INSERT INTO emp VALUES (2, 'Eng', 'Bob', 8000)")
        rows = execute_sql('SELECT COUNT(*) AS cnt FROM emp HAVING cnt > 1')
        assert len(rows) == 1
        assert rows[0]['cnt'] == Decimal('2')

    def test_where_group_by_having_combined(self):
        execute_sql("INSERT INTO emp VALUES (1, 'Sales', 'Alice', 5000)")
        execute_sql("INSERT INTO emp VALUES (2, 'Sales', 'Bob', 6000)")
        execute_sql("INSERT INTO emp VALUES (3, 'Eng', 'Charlie', 8000)")
        rows = execute_sql(
            "SELECT dept, COUNT(*) AS cnt FROM emp WHERE salary > 5000 "
            "GROUP BY dept HAVING cnt > 0 ORDER BY dept ASC"
        )
        assert len(rows) == 2
        sales = next(r for r in rows if r['dept'] == 'Sales')
        eng = next(r for r in rows if r['dept'] == 'Eng')
        assert sales['cnt'] == Decimal('1')  # only Bob
        assert eng['cnt'] == Decimal('1')    # only Charlie


# ---------------------------------------------------------------------------
# Arithmetic expressions
# ---------------------------------------------------------------------------

class TestArithmeticExpressions:
    def setup_method(self):
        execute_sql('CREATE TABLE t (id NUMBER, price NUMBER, qty NUMBER)')
        execute_sql('INSERT INTO t VALUES (1, 10, 3)')
        execute_sql('INSERT INTO t VALUES (2, 20, 5)')

    def teardown_method(self):
        try: execute_sql('DROP TABLE t')
        except Exception: pass

    def test_select_literal_arithmetic(self):
        rows = execute_sql('SELECT 2 + 3 AS five FROM t;')
        assert len(rows) == 2  # one row per table row
        assert rows[0]['five'] == Decimal('5')

    def test_select_column_arithmetic(self):
        rows = execute_sql('SELECT price * qty AS total FROM t WHERE id = 1;')
        assert rows[0]['total'] == Decimal('30')

    def test_select_add_subtract(self):
        rows = execute_sql('SELECT price + qty AS s, price - qty AS d FROM t WHERE id = 2;')
        assert rows[0]['s'] == Decimal('25')
        assert rows[0]['d'] == Decimal('15')

    def test_select_divide(self):
        rows = execute_sql('SELECT price / qty AS ratio FROM t WHERE id = 1;')
        assert rows[0]['ratio'] == Decimal('10') / Decimal('3')

    def test_where_compare_two_columns(self):
        # Compare column expression vs literal (old behavior still works)
        rows = execute_sql('SELECT * FROM t WHERE price > 15;')
        assert len(rows) == 1
        assert rows[0]['id'] == Decimal('2')

    def test_where_column_vs_column(self):
        rows = execute_sql('SELECT * FROM t WHERE price > qty;')
        assert len(rows) == 2  # 10>3 and 20>5 are both true

    def test_where_arithmetic(self):
        rows = execute_sql('SELECT * FROM t WHERE price * qty > 40;')
        assert len(rows) == 1
        assert rows[0]['id'] == Decimal('2')  # 20*5=100 > 40

    def test_select_with_as_alias(self):
        rows = execute_sql('SELECT price * qty AS total FROM t ORDER BY total ASC;')
        assert rows[0]['total'] == Decimal('30')
        assert rows[1]['total'] == Decimal('100')


# ---------------------------------------------------------------------------
# Multi-table queries (Cartesian product)
# ---------------------------------------------------------------------------

class TestMultiTable:
    def setup_method(self):
        execute_sql('CREATE TABLE a (id NUMBER, val STRING);')
        execute_sql('INSERT INTO a VALUES (1, \'Alice\');')
        execute_sql('INSERT INTO a VALUES (2, \'Bob\');')
        execute_sql('CREATE TABLE b (id NUMBER, score NUMBER);')
        execute_sql('INSERT INTO b VALUES (1, 100);')
        execute_sql('INSERT INTO b VALUES (2, 200);')

    def teardown_method(self):
        try: execute_sql('DROP TABLE a')
        except Exception: pass
        try: execute_sql('DROP TABLE b')
        except Exception: pass

    def test_cartesian_product_size(self):
        rows = execute_sql('SELECT * FROM a, b;')
        assert len(rows) == 4  # 2 × 2

    def test_dot_notation(self):
        rows = execute_sql('SELECT a.id, a.val, b.score FROM a, b WHERE a.id = b.id;')
        assert len(rows) == 2
        for r in rows:
            assert r['id'] == r['score'] / 100

    def test_table_alias_with_as(self):
        rows = execute_sql('SELECT x.id, y.score FROM a AS x, b AS y WHERE x.id * 100 = y.score;')
        assert len(rows) == 2

    def test_table_alias_without_as(self):
        rows = execute_sql('SELECT x.id, y.score FROM a x, b y WHERE x.id * 100 = y.score;')
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# JOIN
# ---------------------------------------------------------------------------

class TestJoin:
    def setup_method(self):
        execute_sql('CREATE TABLE emp (id NUMBER, name STRING, dept_id NUMBER);')
        execute_sql("INSERT INTO emp VALUES (1, 'Alice', 1);")
        execute_sql("INSERT INTO emp VALUES (2, 'Bob', 2);")
        execute_sql("INSERT INTO emp VALUES (3, 'Charlie', NULL);")
        execute_sql('CREATE TABLE dept (id NUMBER, dname STRING);')
        execute_sql("INSERT INTO dept VALUES (1, 'Engineering');")
        execute_sql("INSERT INTO dept VALUES (3, 'HR');")

    def teardown_method(self):
        try: execute_sql('DROP TABLE emp')
        except Exception: pass
        try: execute_sql('DROP TABLE dept')
        except Exception: pass

    def test_inner_join(self):
        rows = execute_sql(
            'SELECT emp.name, dept.dname FROM emp INNER JOIN dept ON emp.dept_id = dept.id;')
        assert len(rows) == 1
        assert rows[0]['name'] == 'Alice'

    def test_left_join(self):
        rows = execute_sql(
            'SELECT emp.name, dept.dname FROM emp LEFT JOIN dept ON emp.dept_id = dept.id ORDER BY emp.id ASC;')
        assert len(rows) == 3
        assert rows[0]['dname'] == 'Engineering'
        assert rows[1]['dname'] is None
        assert rows[2]['dname'] is None

    def test_inner_join_with_where(self):
        rows = execute_sql(
            'SELECT emp.name FROM emp INNER JOIN dept ON emp.dept_id = dept.id WHERE emp.id = 1;')
        assert len(rows) == 1
        assert rows[0]['name'] == 'Alice'

    def test_cross_join(self):
        rows = execute_sql('SELECT * FROM emp CROSS JOIN dept;')
        assert len(rows) == 6
        assert list(rows[0].keys()) == ['id', 'name', 'dept_id', 'dept.id', 'dname']

    def test_left_outer_join(self):
        rows = execute_sql('SELECT * FROM emp LEFT OUTER JOIN dept ON emp.dept_id = dept.id;')
        assert len(rows) == 3
        assert rows[2]['dname'] is None

    def test_right_join(self):
        rows = execute_sql(
            'SELECT * FROM emp RIGHT JOIN dept ON emp.dept_id = dept.id ORDER BY dname;')
        assert len(rows) == 2
        assert rows[0]['name'] == 'Alice'
        assert rows[0]['dname'] == 'Engineering'
        assert rows[1]['name'] is None
        assert rows[1]['dname'] == 'HR'

    def test_right_outer_join(self):
        rows = execute_sql('SELECT * FROM emp RIGHT OUTER JOIN dept ON emp.dept_id = dept.id;')
        assert len(rows) == 2

    def test_full_join(self):
        rows = execute_sql('SELECT * FROM emp FULL JOIN dept ON emp.dept_id = dept.id;')
        assert len(rows) == 4
        names = {r['name'] for r in rows}
        dnames = {r['dname'] for r in rows}
        assert names == {'Alice', 'Bob', 'Charlie', None}
        assert dnames == {'Engineering', 'HR', None}

    def test_full_outer_join(self):
        rows = execute_sql('SELECT * FROM emp FULL OUTER JOIN dept ON emp.dept_id = dept.id;')
        assert len(rows) == 4


# ---------------------------------------------------------------------------
# Unqualified columns in multi-table queries
# ---------------------------------------------------------------------------

class TestUnqualifiedJoinColumns:
    def setup_method(self):
        execute_sql('CREATE TABLE weather (city STRING, temp_lo NUMBER, temp_hi NUMBER, prcp NUMBER, date STRING);')
        execute_sql("INSERT INTO weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');")
        execute_sql("INSERT INTO weather VALUES ('Hayward', 37, 54, NULL, '1994-11-29');")
        execute_sql('CREATE TABLE cities (name STRING, location STRING);')
        execute_sql("INSERT INTO cities VALUES ('San Francisco', '(-194.0, 53.0)');")

    def teardown_method(self):
        try: execute_sql('DROP TABLE weather')
        except Exception: pass
        try: execute_sql('DROP TABLE cities')
        except Exception: pass

    def test_join_with_unqualified_columns(self):
        rows = execute_sql('SELECT * FROM weather JOIN cities ON city = name;')
        assert len(rows) == 1
        assert list(rows[0].keys()) == ['city', 'temp_lo', 'temp_hi', 'prcp', 'date', 'name', 'location']
        assert rows[0]['city'] == 'San Francisco'
        assert rows[0]['location'] == '(-194.0, 53.0)'

    def test_comma_join_with_unqualified_columns(self):
        rows = execute_sql('SELECT * FROM weather, cities WHERE city = name;')
        assert len(rows) == 1
        assert rows[0]['city'] == 'San Francisco'
        assert rows[0]['location'] == '(-194.0, 53.0)'

    def test_comma_join_is_cross_join_with_standard_star(self):
        rows = execute_sql('SELECT * FROM weather, cities;')
        assert len(rows) == 2
        assert list(rows[0].keys()) == ['city', 'temp_lo', 'temp_hi', 'prcp', 'date', 'name', 'location']


# ---------------------------------------------------------------------------
# USING / NATURAL joins
# ---------------------------------------------------------------------------

class TestUsingAndNaturalJoins:
    def setup_method(self):
        execute_sql('CREATE TABLE a (id NUMBER, x STRING);')
        execute_sql("INSERT INTO a VALUES (1, 'a1'), (2, 'a2');")
        execute_sql('CREATE TABLE b (id NUMBER, y STRING);')
        execute_sql("INSERT INTO b VALUES (1, 'b1'), (3, 'b3');")

    def teardown_method(self):
        try: execute_sql('DROP TABLE a')
        except Exception: pass
        try: execute_sql('DROP TABLE b')
        except Exception: pass

    def test_join_using_inner(self):
        rows = execute_sql('SELECT * FROM a JOIN b USING (id);')
        assert len(rows) == 1
        assert list(rows[0].keys()) == ['id', 'x', 'y']
        assert rows[0] == {'id': Decimal('1'), 'x': 'a1', 'y': 'b1'}

    def test_join_using_left(self):
        rows = execute_sql('SELECT * FROM a LEFT JOIN b USING (id);')
        assert len(rows) == 2
        assert rows[1]['x'] == 'a2'
        assert rows[1]['y'] is None

    def test_join_using_right(self):
        rows = execute_sql('SELECT * FROM a RIGHT JOIN b USING (id);')
        assert len(rows) == 2
        assert rows[1]['x'] is None
        assert rows[1]['y'] == 'b3'

    def test_join_using_full(self):
        rows = execute_sql('SELECT * FROM a FULL JOIN b USING (id);')
        assert len(rows) == 3
        assert rows[1] == {'id': Decimal('2'), 'x': 'a2', 'y': None}
        assert rows[2] == {'id': Decimal('3'), 'x': None, 'y': 'b3'}

    def test_natural_join(self):
        rows = execute_sql('SELECT * FROM a NATURAL JOIN b;')
        assert len(rows) == 1
        assert list(rows[0].keys()) == ['id', 'x', 'y']
        assert rows[0]['y'] == 'b1'

    def test_natural_left_join(self):
        rows = execute_sql('SELECT * FROM a NATURAL LEFT JOIN b;')
        assert len(rows) == 2
        assert rows[1]['y'] is None

    def test_natural_full_join(self):
        rows = execute_sql('SELECT * FROM a NATURAL FULL JOIN b;')
        assert len(rows) == 3
        assert rows[2]['x'] is None
        assert rows[2]['y'] == 'b3'


# ---------------------------------------------------------------------------
# Subqueries
# ---------------------------------------------------------------------------

class TestSubquery:
    def setup_method(self):
        execute_sql('CREATE TABLE t1 (id NUMBER, val STRING);')
        execute_sql("INSERT INTO t1 VALUES (1, 'A');")
        execute_sql("INSERT INTO t1 VALUES (2, 'B');")
        execute_sql("INSERT INTO t1 VALUES (3, 'C');")
        execute_sql('CREATE TABLE t2 (id NUMBER);')
        execute_sql('INSERT INTO t2 VALUES (1);')
        execute_sql('INSERT INTO t2 VALUES (3);')

    def teardown_method(self):
        try: execute_sql('DROP TABLE t1')
        except Exception: pass
        try: execute_sql('DROP TABLE t2')
        except Exception: pass

    def test_in_subquery(self):
        rows = execute_sql('SELECT * FROM t1 WHERE id IN (SELECT id FROM t2);')
        assert len(rows) == 2
        vals = {r['val'] for r in rows}
        assert vals == {'A', 'C'}

    def test_not_in_subquery(self):
        rows = execute_sql('SELECT * FROM t1 WHERE id NOT IN (SELECT id FROM t2);')
        assert len(rows) == 1
        assert rows[0]['val'] == 'B'

    def test_any_subquery(self):
        execute_sql('CREATE TABLE threshold (min_score NUMBER);')
        execute_sql('INSERT INTO threshold VALUES (2);')
        rows = execute_sql('SELECT * FROM t1 WHERE id > ANY (SELECT id FROM t2);')
        assert len(rows) == 2  # id=2,3 are > some t2 values
        execute_sql('DROP TABLE threshold;')

    def test_all_subquery(self):
        rows = execute_sql('SELECT * FROM t1 WHERE id > ALL (SELECT id FROM t2);')
        assert len(rows) == 0  # no id > all of (1,3)


# ---------------------------------------------------------------------------
# Lexer & Parser boundary / syntax validation
# ---------------------------------------------------------------------------

class TestLexerParserBoundaries:
    def setup_method(self):
        execute_sql('CREATE TABLE t (id NUMBER, name STRING, score NUMBER);')
        execute_sql("INSERT INTO t VALUES (1, 'Alice', 10), (2, 'Bob', 20);")

    def teardown_method(self):
        try: execute_sql('DROP TABLE t')
        except Exception: pass

    def test_trailing_garbage_raises_syntax_error(self):
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT * FROM t WHERE id = 1 GARBAGE;')
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT * FROM t a b;')

    def test_trailing_second_statement_raises_syntax_error(self):
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT * FROM t; SELECT * FROM t;')

    def test_outfile_after_semicolon_raises_syntax_error(self):
        with pytest.raises(SqlSyntaxError):
            execute_sql("SELECT * FROM t; INTO OUTFILE 'x.csv'")

    def test_unterminated_string_raises_syntax_error(self):
        with pytest.raises(SqlSyntaxError, match='Unterminated'):
            execute_sql("SELECT * FROM t WHERE name = 'Alice")

    def test_escaped_single_quote_in_string(self):
        execute_sql("INSERT INTO t VALUES (3, 'O''Brien', 30);")
        rows = execute_sql("SELECT name FROM t WHERE name = 'O''Brien';")
        assert len(rows) == 1
        assert rows[0]['name'] == "O'Brien"

    def test_not_equal_bang_eq(self):
        rows = execute_sql("SELECT id FROM t WHERE name != 'Alice';")
        assert [r['id'] for r in rows] == [Decimal('2')]

    def test_sql_line_comments(self):
        rows = execute_sql(
            "-- leading comment\n"
            "SELECT id -- inline comment\n"
            "FROM t WHERE id = 1; -- trailing comment"
        )
        assert [r['id'] for r in rows] == [Decimal('1')]

    def test_type_aliases_varchar_int_decimal(self):
        execute_sql('CREATE TABLE t2 (a INT, b VARCHAR, c DECIMAL);')
        desc = execute_sql('DESCRIBE t2;')
        assert {'Column': 'a', 'Type': 'NUMBER'} in desc
        assert {'Column': 'b', 'Type': 'STRING'} in desc
        assert {'Column': 'c', 'Type': 'NUMBER'} in desc
        execute_sql('DROP TABLE t2;')

    def test_type_keyword_vs_literal_no_collision(self):
        # TYPE_STRING / TYPE_NUMBER must not collide with STRING / NUMBER literal tokens
        with pytest.raises(SqlSyntaxError):
            execute_sql('CREATE TABLE bad (a 123);')
        with pytest.raises(SqlSyntaxError):
            execute_sql("CREATE TABLE bad (a 'STRING');")

    def test_duplicate_column_in_create_raises(self):
        with pytest.raises(SqlSyntaxError, match='Duplicate'):
            execute_sql('CREATE TABLE bad (id NUMBER, id STRING);')

    def test_duplicate_column_in_insert_raises(self):
        with pytest.raises(SqlSyntaxError, match='Duplicate'):
            execute_sql('INSERT INTO t (id, id) VALUES (1, 2);')

    def test_negative_and_fractional_limit_offset_raise(self):
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT * FROM t LIMIT -1;')
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT * FROM t LIMIT 1.5;')
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT * FROM t OFFSET -2;')
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT * FROM t OFFSET 0.5;')

    def test_limit_zero_returns_empty(self):
        assert execute_sql('SELECT * FROM t LIMIT 0;') == []

    def test_negative_numbers_in_insert_update_where(self):
        execute_sql("INSERT INTO t VALUES (-1, 'Neg', -9.5);")
        n = execute_sql("UPDATE t SET score = -10.25 WHERE id = -1;")
        assert n == 1
        rows = execute_sql("SELECT score FROM t WHERE score < -10;")
        assert rows == [{'score': Decimal('-10.25')}]

    def test_aggregate_in_where_raises(self):
        with pytest.raises(SqlSyntaxError, match='WHERE'):
            execute_sql('SELECT * FROM t WHERE COUNT(*) > 1;')

    def test_select_star_with_group_by_raises(self):
        with pytest.raises(SqlSyntaxError, match='GROUP BY'):
            execute_sql('SELECT * FROM t GROUP BY name;')


# ---------------------------------------------------------------------------
# Storage: NULL vs '' distinction, escaping, atomicity, type validation
# ---------------------------------------------------------------------------

class TestStorageAndTypes:
    def setup_method(self):
        execute_sql('CREATE TABLE items (id NUMBER, note STRING, qty NUMBER);')

    def teardown_method(self):
        try: execute_sql('DROP TABLE items')
        except Exception: pass

    def test_empty_string_distinguished_from_null(self):
        execute_sql("INSERT INTO items VALUES (1, '', 10), (2, NULL, 20);")
        rows = execute_sql('SELECT * FROM items ORDER BY id ASC;')
        assert rows[0]['note'] == ''
        assert rows[1]['note'] is None

        only_empty = execute_sql("SELECT id FROM items WHERE note = '';")
        assert [r['id'] for r in only_empty] == [Decimal('1')]

        only_null = execute_sql('SELECT id FROM items WHERE note IS NULL;')
        assert [r['id'] for r in only_null] == [Decimal('2')]

    def test_special_characters_and_null_sentinel_roundtrip(self):
        special_vals = [
            '\\N',
            '\\\\N',
            'a\\b\\c',
            'tab\there',
            'line1\nline2',
            'quote "double" and \'single\'',
        ]
        for idx, val in enumerate(special_vals, start=1):
            escaped = val.replace("'", "''")
            execute_sql(f"INSERT INTO items VALUES ({idx}, '{escaped}', {idx});")

        rows = execute_sql('SELECT * FROM items ORDER BY id ASC;')
        assert [r['note'] for r in rows] == special_vals

        # Multiline string must not inflate count_rows()
        desc = execute_sql('DESCRIBE items;')
        row_entry = next(r for r in desc if r['Column'] == '__rows__')
        assert row_entry['Type'] == str(len(special_vals))

    def test_insert_and_update_type_mismatch_raises(self):
        execute_sql("INSERT INTO items VALUES (1, 'ok', 5);")
        with pytest.raises(TypeMismatchError):
            execute_sql("INSERT INTO items VALUES ('bad', 'ok', 5);")
        with pytest.raises(TypeMismatchError):
            execute_sql("INSERT INTO items VALUES (2, 123, 5);")
        with pytest.raises(TypeMismatchError):
            execute_sql("UPDATE items SET qty = 'bad' WHERE id = 1;")
        with pytest.raises(TypeMismatchError):
            execute_sql("UPDATE items SET note = 999 WHERE id = 1;")

    def test_multiline_insert_failure_is_atomic(self):
        with pytest.raises(TypeMismatchError):
            execute_sql("INSERT INTO items VALUES (1, 'ok', 5), ('bad', 'fail', 6);")
        assert execute_sql('SELECT * FROM items;') == []

    def test_drop_nonexistent_table_raises(self):
        with pytest.raises(TableNotFoundError):
            execute_sql('DROP TABLE missing_tbl;')

    def test_drop_and_recreate_table(self):
        execute_sql("INSERT INTO items VALUES (1, 'a', 1);")
        execute_sql('DROP TABLE items;')
        execute_sql('CREATE TABLE items (id NUMBER, label STRING);')
        execute_sql("INSERT INTO items VALUES (9, 'fresh');")
        assert execute_sql('SELECT * FROM items;') == [{'id': Decimal('9'), 'label': 'fresh'}]

    def test_legacy_empty_number_field_reads_as_null(self, tmp_path):
        # Legacy TSV files stored NULL as "" in NUMBER columns while STRING "" remains ''
        (tmp_path / 'metadata.txt').write_text('legacy\t2\tn\tNUMBER\ts\tSTRING\n', encoding='utf-8')
        (tmp_path / 'legacy.txt').write_text('""\t""\n"42"\t"ok"\n', encoding='utf-8')
        rows = execute_sql('SELECT * FROM legacy;')
        assert rows == [{'n': None, 's': ''}, {'n': Decimal('42'), 's': 'ok'}]


# ---------------------------------------------------------------------------
# TRUE / FALSE semantics (= 1 / 0 NUMBER literals, arithmetic allowed)
# ---------------------------------------------------------------------------

class TestBooleanLiterals:
    def setup_method(self):
        execute_sql('CREATE TABLE flags (id NUMBER, active NUMBER, label STRING);')

    def teardown_method(self):
        try: execute_sql('DROP TABLE flags')
        except Exception: pass

    def test_true_false_stored_as_number_and_queried(self):
        execute_sql("INSERT INTO flags VALUES (1, TRUE, 'yes'), (2, FALSE, 'no');")
        rows = execute_sql('SELECT * FROM flags ORDER BY id ASC;')
        assert rows[0]['active'] == Decimal('1')
        assert rows[1]['active'] == Decimal('0')

        t_rows = execute_sql('SELECT id FROM flags WHERE active = TRUE;')
        assert [r['id'] for r in t_rows] == [Decimal('1')]
        assert execute_sql('SELECT id FROM flags WHERE 1 = TRUE;') == [{'id': Decimal('1')}, {'id': Decimal('2')}]

    def test_true_false_arithmetic_and_boolean_expression(self):
        execute_sql("INSERT INTO flags VALUES (1, TRUE, 'a');")
        rows = execute_sql('SELECT TRUE + TRUE AS two, FALSE * 5 AS zero, (active = 1) = TRUE AS matched FROM flags;')
        assert rows[0]['two'] == Decimal('2')
        assert rows[0]['zero'] == Decimal('0')
        assert rows[0]['matched'] is True

    def test_true_false_rejected_for_string_column_and_comparison(self):
        with pytest.raises(TypeMismatchError):
            execute_sql("INSERT INTO flags VALUES (1, 1, TRUE);")
        execute_sql("INSERT INTO flags VALUES (1, 1, 'True');")
        with pytest.raises(TypeMismatchError):
            execute_sql("SELECT * FROM flags WHERE label = TRUE;")
        with pytest.raises(TypeMismatchError):
            execute_sql("SELECT * FROM flags WHERE TRUE = label;")


# ---------------------------------------------------------------------------
# SQL Three-Valued Logic (3VL) & Subquery edge cases (IN/ANY/ALL/EXISTS)
# ---------------------------------------------------------------------------

class TestThreeValuedLogicAndSubqueries:
    def setup_method(self):
        execute_sql('CREATE TABLE a (id NUMBER, v NUMBER, tag STRING);')
        execute_sql("INSERT INTO a VALUES (1, 10, 'x'), (2, 20, 'y'), (3, NULL, 'z');")
        execute_sql('CREATE TABLE b (id NUMBER, v NUMBER);')
        execute_sql('INSERT INTO b VALUES (1, 10), (2, NULL);')
        execute_sql('CREATE TABLE empty_t (id NUMBER);')

    def teardown_method(self):
        for name in ('a', 'b', 'empty_t'):
            try: execute_sql(f'DROP TABLE {name}')
            except Exception: pass

    def test_3vl_and_or_truth_table(self):
        # FALSE AND NULL -> FALSE; TRUE OR NULL -> TRUE; FALSE OR NULL -> UNKNOWN (filtered out)
        rows_or = execute_sql('SELECT id FROM a WHERE id = 1 OR v = 999 ORDER BY id ASC;')
        assert [r['id'] for r in rows_or] == [Decimal('1')]

        rows_and = execute_sql('SELECT id FROM a WHERE id = 999 AND v = 10;')
        assert rows_and == []

        rows_unknown_or = execute_sql('SELECT id FROM a WHERE v = 10 OR v = 999;')
        assert [r['id'] for r in rows_unknown_or] == [Decimal('1')]

    def test_not_in_with_null_in_subquery(self):
        # Standard SQL: 20 NOT IN (10, NULL) evaluates to UNKNOWN -> excluded!
        rows = execute_sql('SELECT id FROM a WHERE v NOT IN (SELECT v FROM b);')
        assert rows == []

    def test_in_and_not_in_with_empty_subquery(self):
        assert execute_sql('SELECT id FROM a WHERE v IN (SELECT id FROM empty_t);') == []
        # Even NULL v matches NOT IN (empty)
        rows = execute_sql('SELECT id FROM a WHERE v NOT IN (SELECT id FROM empty_t) ORDER BY id ASC;')
        assert [r['id'] for r in rows] == [Decimal('1'), Decimal('2'), Decimal('3')]

    def test_any_all_with_empty_and_null_subquery(self):
        assert execute_sql('SELECT id FROM a WHERE id > ANY (SELECT id FROM empty_t);') == []
        rows_all_empty = execute_sql('SELECT id FROM a WHERE id > ALL (SELECT id FROM empty_t) ORDER BY id ASC;')
        assert [r['id'] for r in rows_all_empty] == [Decimal('1'), Decimal('2'), Decimal('3')]

        # 20 > ANY (10, NULL) is TRUE; 5 > ANY (10, NULL) is UNKNOWN
        rows_any = execute_sql('SELECT id FROM a WHERE v > ANY (SELECT v FROM b);')
        assert [r['id'] for r in rows_any] == [Decimal('2')]

        # 20 > ALL (10, NULL) is UNKNOWN (excluded)
        rows_all = execute_sql('SELECT id FROM a WHERE v > ALL (SELECT v FROM b);')
        assert rows_all == []

    def test_multicolumn_subquery_raises(self):
        with pytest.raises(EngineError, match='one column'):
            execute_sql('SELECT * FROM a WHERE id IN (SELECT id, v FROM b);')

    def test_exists_and_not_exists_uncorrelated(self):
        assert len(execute_sql('SELECT * FROM a WHERE EXISTS (SELECT * FROM b);')) == 3
        assert execute_sql('SELECT * FROM a WHERE EXISTS (SELECT * FROM empty_t);') == []
        assert len(execute_sql('SELECT * FROM a WHERE NOT EXISTS (SELECT * FROM empty_t);')) == 3
        assert execute_sql('SELECT * FROM a WHERE NOT EXISTS (SELECT * FROM b);') == []

    def test_exists_and_not_exists_correlated(self):
        rows = execute_sql(
            'SELECT id FROM a WHERE EXISTS (SELECT * FROM b WHERE b.id = a.id) ORDER BY id ASC;')
        assert [r['id'] for r in rows] == [Decimal('1'), Decimal('2')]

        rows_not = execute_sql(
            'SELECT id FROM a WHERE NOT EXISTS (SELECT * FROM b WHERE b.id = a.id);')
        assert [r['id'] for r in rows_not] == [Decimal('3')]

    def test_delete_and_update_with_subquery(self):
        updated = execute_sql('UPDATE a SET v = 99 WHERE id IN (SELECT id FROM b);')
        assert updated == 2
        deleted = execute_sql('DELETE FROM a WHERE NOT EXISTS (SELECT * FROM b WHERE b.id = a.id);')
        assert deleted == 1
        assert [r['id'] for r in execute_sql('SELECT id FROM a ORDER BY id ASC;')] == [Decimal('1'), Decimal('2')]


# ---------------------------------------------------------------------------
# UNION / UNION ALL
# ---------------------------------------------------------------------------

class TestUnion:
    def setup_method(self):
        execute_sql('CREATE TABLE u1 (id NUMBER, name STRING);')
        execute_sql("INSERT INTO u1 VALUES (1, 'Alice'), (2, 'Bob');")
        execute_sql('CREATE TABLE u2 (uid NUMBER, uname STRING);')
        execute_sql("INSERT INTO u2 VALUES (2, 'Bob'), (3, 'Charlie');")

    def teardown_method(self):
        for name in ('u1', 'u2'):
            try: execute_sql(f'DROP TABLE {name}')
            except Exception: pass

    def test_union_dedup_and_column_names(self):
        rows = execute_sql('SELECT id, name FROM u1 UNION SELECT uid, uname FROM u2 ORDER BY id ASC;')
        assert rows == [
            {'id': Decimal('1'), 'name': 'Alice'},
            {'id': Decimal('2'), 'name': 'Bob'},
            {'id': Decimal('3'), 'name': 'Charlie'},
        ]

    def test_union_all_keeps_duplicates(self):
        rows = execute_sql('SELECT id, name FROM u1 UNION ALL SELECT uid, uname FROM u2;')
        assert len(rows) == 4

    def test_union_chained_with_limit_offset(self):
        rows = execute_sql(
            'SELECT id FROM u1 UNION ALL SELECT uid FROM u2 UNION SELECT id FROM u1 '
            'ORDER BY id DESC LIMIT 2 OFFSET 1;'
        )
        assert [r['id'] for r in rows] == [Decimal('2'), Decimal('1')]

    def test_union_column_count_mismatch_raises(self):
        with pytest.raises(EngineError, match='different number of columns'):
            execute_sql('SELECT id, name FROM u1 UNION SELECT uid FROM u2;')

    def test_union_inside_subquery(self):
        rows = execute_sql(
            'SELECT uid FROM u2 WHERE uid IN (SELECT id FROM u1 UNION SELECT uid FROM u2 WHERE uid = 3) ORDER BY uid ASC;')
        assert [r['uid'] for r in rows] == [Decimal('2'), Decimal('3')]


# ---------------------------------------------------------------------------
# Aggregation, ORDER BY, DISTINCT, and Multi-table / JOIN edge cases
# ---------------------------------------------------------------------------

class TestAdvancedQueryBoundaries:
    def setup_method(self):
        execute_sql('CREATE TABLE emp (id NUMBER, dept STRING, salary NUMBER);')
        execute_sql("INSERT INTO emp VALUES (3, 'Eng', 9000), (1, 'Sales', 5000), (2, 'Eng', 8000);")
        execute_sql('CREATE TABLE dept (id NUMBER, dname STRING);')
        execute_sql("INSERT INTO dept VALUES (1, 'Sales'), (2, 'Ops');")
        execute_sql('CREATE TABLE empty_t (id NUMBER, x NUMBER);')

    def teardown_method(self):
        for name in ('emp', 'dept', 'empty_t'):
            try: execute_sql(f'DROP TABLE {name}')
            except Exception: pass

    def test_order_by_qualified_column_really_sorts(self):
        # Rows were inserted in order 3, 1, 2; verify qualified ORDER BY actually sorts
        rows = execute_sql('SELECT emp.id FROM emp ORDER BY emp.id ASC;')
        assert [r['id'] for r in rows] == [Decimal('1'), Decimal('2'), Decimal('3')]
        rows_desc = execute_sql('SELECT emp.id FROM emp ORDER BY emp.id DESC;')
        assert [r['id'] for r in rows_desc] == [Decimal('3'), Decimal('2'), Decimal('1')]

    def test_order_by_non_projected_column(self):
        rows = execute_sql('SELECT dept FROM emp ORDER BY salary ASC;')
        assert [r['dept'] for r in rows] == ['Sales', 'Eng', 'Eng']

    def test_order_by_unknown_column_raises(self):
        with pytest.raises(ColumnNotFoundError):
            execute_sql('SELECT id FROM emp ORDER BY nonexistent ASC;')

    def test_group_by_without_aggregate(self):
        rows = execute_sql('SELECT dept FROM emp GROUP BY dept ORDER BY dept ASC;')
        assert rows == [{'dept': 'Eng'}, {'dept': 'Sales'}]

    def test_aggregate_without_alias_and_having_direct_agg(self):
        rows = execute_sql(
            'SELECT dept, COUNT(*), MAX(emp.salary) - MIN(emp.salary) AS span '
            'FROM emp GROUP BY emp.dept HAVING COUNT(*) > 1;'
        )
        assert len(rows) == 1
        assert rows[0]['dept'] == 'Eng'
        assert rows[0]['COUNT(*)'] == Decimal('2')
        assert rows[0]['span'] == Decimal('1000')

    def test_empty_table_aggregate_with_and_without_group_by(self):
        assert execute_sql('SELECT COUNT(*), SUM(x) FROM empty_t;') == [
            {'COUNT(*)': Decimal('0'), 'SUM(x)': None}
        ]
        assert execute_sql('SELECT id, COUNT(*) FROM empty_t GROUP BY id;') == []

    def test_sum_avg_on_string_column_raises(self):
        with pytest.raises(TypeMismatchError):
            execute_sql('SELECT SUM(dept) FROM emp;')
        with pytest.raises(TypeMismatchError):
            execute_sql('SELECT AVG(dept) FROM emp;')

    def test_min_max_on_string_column(self):
        rows = execute_sql('SELECT MIN(dept) AS lo, MAX(dept) AS hi FROM emp;')
        assert rows == [{'lo': 'Eng', 'hi': 'Sales'}]

    def test_distinct_preserves_none_vs_string_none(self):
        execute_sql('CREATE TABLE d (v STRING);')
        execute_sql("INSERT INTO d VALUES (NULL), ('None'), (NULL), ('None');")
        rows = execute_sql('SELECT DISTINCT v FROM d;')
        assert len(rows) == 2
        assert {r['v'] for r in rows} == {None, 'None'}
        execute_sql('DROP TABLE d;')

    def test_single_table_bogus_qualifier_raises(self):
        with pytest.raises(ColumnNotFoundError):
            execute_sql('SELECT bogus.id FROM emp;')

    def test_duplicate_table_without_alias_raises(self):
        with pytest.raises(EngineError, match='Not unique'):
            execute_sql('SELECT * FROM emp JOIN emp ON emp.id = emp.id;')

    def test_self_join_with_aliases(self):
        rows = execute_sql(
            'SELECT a.id AS id1, b.id AS id2 FROM emp a JOIN emp b ON a.dept = b.dept AND a.id < b.id;'
        )
        assert rows == [{'id1': Decimal('2'), 'id2': Decimal('3')}]

    def test_full_join_using_select_where_order_by_merged_column(self):
        rows = execute_sql(
            'SELECT id, dept, dname FROM emp FULL JOIN dept USING (id) WHERE id >= 2 ORDER BY id ASC;'
        )
        assert rows == [
            {'id': Decimal('2'), 'dept': 'Eng', 'dname': 'Ops'},
            {'id': Decimal('3'), 'dept': 'Eng', 'dname': None},
        ]

    def test_join_with_empty_table(self):
        assert execute_sql('SELECT * FROM emp INNER JOIN empty_t ON emp.id = empty_t.id;') == []
        left_rows = execute_sql('SELECT * FROM emp LEFT JOIN empty_t ON emp.id = empty_t.id;')
        assert len(left_rows) == 3
        right_rows = execute_sql('SELECT * FROM empty_t RIGHT JOIN dept ON empty_t.id = dept.id;')
        assert len(right_rows) == 2


# ---------------------------------------------------------------------------
# System commands, IMPORT (.xlsx), and INTO OUTFILE (.xlsx / .csv / .tsv)
# ---------------------------------------------------------------------------

class TestSystemCommandsImportExport:
    def test_show_tables_and_describe(self):
        execute_sql('CREATE TABLE alpha (id NUMBER, name STRING);')
        execute_sql("INSERT INTO alpha VALUES (1, 'a'), (2, 'b');")
        tables = execute_sql('SHOW TABLES;')
        assert tables == [{'Table': 'alpha', 'Columns': 'id, name', 'Rows': 2}]

        desc = execute_sql('DESCRIBE alpha;')
        assert desc == [
            {'Column': 'id', 'Type': 'NUMBER'},
            {'Column': 'name', 'Type': 'STRING'},
            {'Column': '__rows__', 'Type': '2'},
        ]
        execute_sql('DROP TABLE alpha;')

    def test_describe_missing_table_raises(self):
        with pytest.raises(TableNotFoundError):
            execute_sql('DESCRIBE no_such_table;')

    def test_import_xlsx_auto_infer_and_explicit_types(self, tmp_path):
        import openpyxl
        xlsx_path = tmp_path / 'sample.xlsx'
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(['id', 'name', 'empty_col'])
        ws.append([1, 'Alice', None])
        ws.append([2, None, None])
        wb.save(xlsx_path)

        cnt = execute_sql(f"IMPORT people FROM '{xlsx_path}';")
        assert cnt == 2
        desc = execute_sql('DESCRIBE people;')
        assert {'Column': 'id', 'Type': 'NUMBER'} in desc
        assert {'Column': 'name', 'Type': 'STRING'} in desc
        assert {'Column': 'empty_col', 'Type': 'STRING'} in desc

        rows = execute_sql('SELECT * FROM people ORDER BY id ASC;')
        assert rows[0] == {'id': Decimal('1'), 'name': 'Alice', 'empty_col': None}
        assert rows[1] == {'id': Decimal('2'), 'name': None, 'empty_col': None}

        # Explicit schema override
        cnt2 = execute_sql(f"IMPORT people_str (id STRING, name STRING, empty_col STRING) FROM '{xlsx_path}';")
        assert cnt2 == 2
        rows2 = execute_sql('SELECT id FROM people_str ORDER BY id ASC;')
        assert rows2 == [{'id': '1'}, {'id': '2'}]

    def test_import_error_cases(self, tmp_path):
        import openpyxl
        with pytest.raises(EngineError, match='File not found'):
            execute_sql("IMPORT t FROM 'nonexistent.xlsx';")

        empty_xlsx = tmp_path / 'empty.xlsx'
        openpyxl.Workbook().save(empty_xlsx)
        with pytest.raises(EngineError, match='empty'):
            execute_sql(f"IMPORT t FROM '{empty_xlsx}';")

        bad_xlsx = tmp_path / 'bad.xlsx'
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(['num'])
        ws.append(['not_a_number'])
        wb.save(bad_xlsx)
        with pytest.raises(TypeMismatchError):
            execute_sql(f"IMPORT t (num NUMBER) FROM '{bad_xlsx}';")
        # Failed import should not leave behind half-created table
        assert execute_sql('SHOW TABLES;') == []

    def test_export_csv_tsv_xlsx_and_empty_result(self, tmp_path):
        import openpyxl
        execute_sql('CREATE TABLE exp (id NUMBER, name STRING);')
        execute_sql("INSERT INTO exp VALUES (1, 'Alice'), (2, NULL);")

        csv_path = tmp_path / 'out.csv'
        tsv_path = tmp_path / 'out.tsv'
        xlsx_path = tmp_path / 'out.xlsx'
        empty_csv = tmp_path / 'empty.csv'

        assert execute_sql(f"SELECT * FROM exp ORDER BY id ASC INTO OUTFILE '{csv_path}';") == 2
        assert execute_sql(f"SELECT * FROM exp ORDER BY id ASC INTO OUTFILE '{tsv_path}';") == 2
        assert execute_sql(f"SELECT * FROM exp ORDER BY id ASC INTO OUTFILE '{xlsx_path}';") == 2
        assert execute_sql(f"SELECT * FROM exp WHERE id = 999 INTO OUTFILE '{empty_csv}';") == 0

        assert 'Alice' in csv_path.read_text(encoding='utf-8')
        assert '\t' in tsv_path.read_text(encoding='utf-8')
        assert empty_csv.read_text(encoding='utf-8').strip().replace('"', '') == 'id,name'

        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        sheet_rows = list(wb.active.iter_rows(values_only=True))
        wb.close()
        assert sheet_rows[0] == ('id', 'name')
        assert sheet_rows[1] == (1, 'Alice')
        assert sheet_rows[2][0] == 2 and sheet_rows[2][1] in (None, '')
        execute_sql('DROP TABLE exp;')


# ---------------------------------------------------------------------------
# B1 SQL Completeness: NOT, IN list, BETWEEN, LIKE, UPDATE expr, INSERT SELECT,
# IF [NOT] EXISTS, SELECT table.*, scalar subquery, COUNT(DISTINCT col)
# ---------------------------------------------------------------------------

class TestPredicatesNotInBetweenLike:
    def setup_method(self):
        execute_sql('CREATE TABLE items (id NUMBER, name STRING, price NUMBER);')
        execute_sql(
            "INSERT INTO items VALUES "
            "(1, 'apple', 10), "
            "(2, 'apricot', 20), "
            "(3, 'banana', 30), "
            "(4, 'a.b[1]', NULL), "
            "(5, 'line1\nline2', 50), "
            "(6, NULL, 60);"
        )

    def teardown_method(self):
        try: execute_sql('DROP TABLE items')
        except Exception: pass

    def test_logical_not_and_3vl(self):
        # NOT binds tighter than AND / OR; NOT UNKNOWN -> UNKNOWN
        rows = execute_sql('SELECT id FROM items WHERE NOT id = 1 AND price <= 30 ORDER BY id ASC;')
        assert [r['id'] for r in rows] == [Decimal('2'), Decimal('3')]

        # Double negation and NULL: NOT (price = 10) is UNKNOWN when price IS NULL
        rows2 = execute_sql('SELECT id FROM items WHERE NOT (price = 10) ORDER BY id ASC;')
        assert [r['id'] for r in rows2] == [Decimal('2'), Decimal('3'), Decimal('5'), Decimal('6')]

        rows3 = execute_sql('SELECT id FROM items WHERE NOT NOT (id = 1);')
        assert [r['id'] for r in rows3] == [Decimal('1')]

    def test_in_and_not_in_list_with_expressions_and_nulls(self):
        rows = execute_sql('SELECT id FROM items WHERE id IN (1, 1 + 2, 99) ORDER BY id ASC;')
        assert [r['id'] for r in rows] == [Decimal('1'), Decimal('3')]

        # 10 NOT IN (20, NULL) -> UNKNOWN (excluded)
        assert execute_sql('SELECT id FROM items WHERE price NOT IN (20, NULL);') == []

        # 20 NOT IN (20, NULL) -> FALSE (excluded); 10 NOT IN (20, 30) -> TRUE
        rows_not = execute_sql('SELECT id FROM items WHERE price NOT IN (20, 30) ORDER BY id ASC;')
        assert [r['id'] for r in rows_not] == [Decimal('1'), Decimal('5'), Decimal('6')]

        with pytest.raises(TypeMismatchError):
            execute_sql("SELECT id FROM items WHERE id IN ('bad');")

    def test_between_and_not_between_with_3vl(self):
        # Inclusive bounds + combined with logical AND
        rows = execute_sql('SELECT id FROM items WHERE price BETWEEN 10 AND 30 AND id >= 2 ORDER BY id ASC;')
        assert [r['id'] for r in rows] == [Decimal('2'), Decimal('3')]

        rows_str = execute_sql("SELECT id FROM items WHERE name BETWEEN 'apple' AND 'banana' ORDER BY id ASC;")
        assert [r['id'] for r in rows_str] == [Decimal('1'), Decimal('2'), Decimal('3')]

        # 10 BETWEEN NULL AND 5 -> UNKNOWN AND FALSE -> FALSE -> NOT BETWEEN is TRUE!
        rows_not = execute_sql('SELECT id FROM items WHERE price NOT BETWEEN NULL AND 15 ORDER BY id ASC;')
        assert [r['id'] for r in rows_not] == [Decimal('2'), Decimal('3'), Decimal('5'), Decimal('6')]

        # 20 BETWEEN NULL AND 50 -> UNKNOWN -> excluded
        assert execute_sql('SELECT id FROM items WHERE price BETWEEN NULL AND 50;') == []

    def test_like_and_not_like_wildcards_multiline_and_regex_chars(self):
        rows = execute_sql("SELECT id FROM items WHERE name LIKE 'ap%' ORDER BY id ASC;")
        assert [r['id'] for r in rows] == [Decimal('1'), Decimal('2')]

        rows_underscore = execute_sql("SELECT id FROM items WHERE name LIKE 'ap_le';")
        assert [r['id'] for r in rows_underscore] == [Decimal('1')]

        # Regex metacharacters (., [, ]) must be treated literally
        rows_lit = execute_sql("SELECT id FROM items WHERE name LIKE 'a.b[1]';")
        assert [r['id'] for r in rows_lit] == [Decimal('4')]

        # Multiline string matched by % under re.DOTALL
        rows_nl = execute_sql("SELECT id FROM items WHERE name LIKE 'line1%line2';")
        assert [r['id'] for r in rows_nl] == [Decimal('5')]

        # NOT LIKE excludes NULL names (3VL)
        rows_not = execute_sql("SELECT id FROM items WHERE name NOT LIKE 'ap%' ORDER BY id ASC;")
        assert [r['id'] for r in rows_not] == [Decimal('3'), Decimal('4'), Decimal('5')]

        with pytest.raises(TypeMismatchError):
            execute_sql("SELECT id FROM items WHERE id LIKE '1%';")


class TestDmlDdlEnhancementsAndAggregates:
    def test_create_if_not_exists_and_drop_if_exists(self):
        execute_sql('CREATE TABLE IF NOT EXISTS t (id NUMBER);')
        execute_sql('INSERT INTO t VALUES (1);')
        # Should not raise and should preserve existing data
        execute_sql('CREATE TABLE IF NOT EXISTS t (id NUMBER, extra STRING);')
        assert execute_sql('SELECT * FROM t;') == [{'id': Decimal('1')}]

        execute_sql('DROP TABLE IF EXISTS t;')
        execute_sql('DROP TABLE IF EXISTS t;')  # no-op when already gone

    def test_update_set_expressions_and_atomicity(self):
        execute_sql('CREATE TABLE emp (id NUMBER, salary NUMBER, bonus NUMBER, tag STRING);')
        execute_sql("INSERT INTO emp VALUES (1, 1000, 100, 'A'), (2, 2000, 200, 'B');")

        # Both columns evaluate against old row state; scalar subquery also works
        updated = execute_sql(
            'UPDATE emp SET salary = salary * 1.1, bonus = salary / 10 '
            'WHERE id = (SELECT MIN(id) FROM emp);'
        )
        assert updated == 1
        rows = execute_sql('SELECT * FROM emp ORDER BY id ASC;')
        assert rows[0]['salary'] == Decimal('1100.0')
        assert rows[0]['bonus'] == Decimal('100')  # computed from old salary=1000

        # Division by zero or type mismatch aborts whole UPDATE without partial write
        with pytest.raises(TypeMismatchError):
            execute_sql('UPDATE emp SET salary = salary / (id - 2);')
        assert execute_sql('SELECT salary FROM emp WHERE id = 1;') == [{'salary': Decimal('1100.0')}]

        # Literal type mismatch rejected even when WHERE matches 0 rows
        with pytest.raises(TypeMismatchError):
            execute_sql("UPDATE emp SET salary = 'bad' WHERE id = 999;")

        with pytest.raises(SqlSyntaxError):
            execute_sql('UPDATE emp SET salary = COUNT(*);')

        execute_sql('DROP TABLE emp;')

    def test_insert_into_select_and_self_insert(self):
        execute_sql('CREATE TABLE src (id NUMBER, name STRING);')
        execute_sql("INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob');")
        execute_sql('CREATE TABLE dst (id NUMBER, name STRING, note STRING);')

        execute_sql("INSERT INTO dst (id, name) SELECT id, name FROM src WHERE id >= 1;")
        assert execute_sql('SELECT * FROM dst ORDER BY id ASC;') == [
            {'id': Decimal('1'), 'name': 'Alice', 'note': None},
            {'id': Decimal('2'), 'name': 'Bob', 'note': None},
        ]

        # Self-insert must materialize first and not loop infinitely
        execute_sql('INSERT INTO src SELECT id + 10, name FROM src;')
        assert len(execute_sql('SELECT * FROM src;')) == 4

        # Empty source SELECT inserts 0 rows
        execute_sql('INSERT INTO dst (id, name) SELECT id, name FROM src WHERE id = 999;')
        assert len(execute_sql('SELECT * FROM dst;')) == 2

        # Column count mismatch & type mismatch
        with pytest.raises(EngineError, match='Column count'):
            execute_sql('INSERT INTO dst SELECT id, name FROM src;')
        with pytest.raises(TypeMismatchError):
            execute_sql('INSERT INTO dst (id, name) SELECT name, id FROM src;')
        assert len(execute_sql('SELECT * FROM dst;')) == 2

        execute_sql('DROP TABLE src;')
        execute_sql('DROP TABLE dst;')

    def test_select_table_star_and_wildcards(self):
        execute_sql('CREATE TABLE a (id NUMBER, val STRING);')
        execute_sql("INSERT INTO a VALUES (1, 'x');")
        execute_sql('CREATE TABLE b (id NUMBER, score NUMBER);')
        execute_sql('INSERT INTO b VALUES (1, 95);')

        rows = execute_sql('SELECT a.*, b.score FROM a JOIN b ON a.id = b.id;')
        assert rows == [{'id': Decimal('1'), 'val': 'x', 'score': Decimal('95')}]

        rows_both = execute_sql('SELECT a.*, b.* FROM a JOIN b ON a.id = b.id;')
        assert list(rows_both[0].keys()) == ['id', 'val', 'b.id', 'score']

        rows_single = execute_sql('SELECT t.*, id * 2 AS dbl FROM a t;')
        assert rows_single == [{'id': Decimal('1'), 'val': 'x', 'dbl': Decimal('2')}]

        with pytest.raises(TableNotFoundError):
            execute_sql('SELECT bogus.* FROM a;')
        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT a.* FROM a GROUP BY id;')

        execute_sql('DROP TABLE a;')
        execute_sql('DROP TABLE b;')

    def test_scalar_subquery_and_correlated_scalar_subquery(self):
        execute_sql('CREATE TABLE emp (id NUMBER, dept STRING, salary NUMBER);')
        execute_sql("INSERT INTO emp VALUES (1, 'Eng', 8000), (2, 'Eng', 10000), (3, 'Sales', 5000);")
        execute_sql('CREATE TABLE empty_t (x NUMBER);')

        # Scalar subquery in WHERE and SELECT
        rows = execute_sql(
            'SELECT id, salary - (SELECT AVG(salary) FROM emp) AS diff '
            'FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) ORDER BY id ASC;'
        )
        assert [r['id'] for r in rows] == [Decimal('1'), Decimal('2')]

        # Correlated scalar subquery: earn more than own dept average
        corr = execute_sql(
            'SELECT e.id FROM emp e '
            'WHERE e.salary > (SELECT AVG(salary) FROM emp e2 WHERE e2.dept = e.dept);'
        )
        assert corr == [{'id': Decimal('2')}]

        # 0 rows -> NULL
        empty_res = execute_sql('SELECT (SELECT x FROM empty_t) AS missing FROM emp LIMIT 1;')
        assert empty_res == [{'missing': None}]

        # >1 row -> EngineError
        with pytest.raises(EngineError, match='more than one row'):
            execute_sql('SELECT * FROM emp WHERE salary > (SELECT salary FROM emp);')

        # >1 column -> EngineError
        with pytest.raises(EngineError, match='one column'):
            execute_sql('SELECT * FROM emp WHERE salary > (SELECT id, salary FROM emp WHERE id = 1);')

        execute_sql('DROP TABLE emp;')
        execute_sql('DROP TABLE empty_t;')

    def test_count_sum_avg_distinct(self):
        execute_sql('CREATE TABLE logs (id NUMBER, dept STRING, amt NUMBER);')
        execute_sql(
            "INSERT INTO logs VALUES "
            "(1, 'Eng', 10), (2, 'Eng', 10.0), (3, 'Eng', 20), (4, 'Sales', NULL), (5, NULL, 20);"
        )
        rows = execute_sql(
            'SELECT COUNT(DISTINCT dept) AS d_dept, '
            'COUNT(DISTINCT amt) AS d_amt, '
            'SUM(DISTINCT amt) AS s_amt, '
            'AVG(DISTINCT amt) AS a_amt '
            'FROM logs;'
        )
        assert rows == [{
            'd_dept': Decimal('2'),
            'd_amt': Decimal('2'),
            's_amt': Decimal('30'),
            'a_amt': Decimal('15'),
        }]

        with pytest.raises(SqlSyntaxError):
            execute_sql('SELECT COUNT(DISTINCT *) FROM logs;')

        execute_sql('DROP TABLE logs;')


