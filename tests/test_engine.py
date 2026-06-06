"""End-to-end tests for the TxtSQL engine covering all CRUD operations."""
import os
import tempfile
from decimal import Decimal

import pytest

from txtsql.engine import execute_sql
from txtsql.exceptions import SqlSyntaxError, TableAlreadyExistsError


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
