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
        execute_sql('CREATE TABLE t (n NUMBER, s STRING, b BINARY)')
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
