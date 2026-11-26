"""Tests for secondary indexes (Stage 1) and query optimizer (Stage 2)."""

from decimal import Decimal

import pytest

from txtsql import Database
from txtsql.exceptions import ColumnNotFoundError, EngineError, TableNotFoundError, TypeMismatchError
from txtsql.result import StatementKind


class TestSecondaryIndexDDLAndLifecycle:
    def test_create_show_drop_index_and_persistence(self, tmp_path):
        db = Database(tmp_path)
        db.execute("CREATE TABLE users (id NUMBER, name STRING, age NUMBER);")
        db.execute("INSERT INTO users VALUES (1, 'Alice', 22), (2, 'Bob', 30), (3, 'Cara', NULL);")

        res_c = db.execute("CREATE INDEX idx_users_id ON users (id);")
        assert res_c.kind == StatementKind.DDL
        db.execute("CREATE INDEX IF NOT EXISTS idx_users_id ON users (id);")
        db.execute("CREATE INDEX idx_users_name ON users (name);")

        with pytest.raises(EngineError, match='Index already exists'):
            db.execute("CREATE INDEX idx_users_id ON users (id);")
        with pytest.raises(TableNotFoundError):
            db.execute("CREATE INDEX idx_missing ON no_tbl (id);")
        with pytest.raises(ColumnNotFoundError):
            db.execute("CREATE INDEX idx_bad_col ON users (missing_col);")

        shown = db.execute("SHOW INDEXES;")
        assert shown.columns == ('Index', 'Table', 'Column')
        assert {(r['Index'], r['Table'], r['Column']) for r in shown.rows} == {
            ('idx_users_id', 'users', 'id'),
            ('idx_users_name', 'users', 'name'),
        }

        # Reopen fresh Database session: indexes.txt must persist
        db2 = Database(tmp_path)
        shown2 = db2.execute("SHOW INDEXES FROM users;")
        assert len(shown2.rows) == 2

        db2.execute("DROP INDEX idx_users_name;")
        db2.execute("DROP INDEX IF EXISTS idx_users_name;")
        with pytest.raises(EngineError, match='Index does not exist'):
            db2.execute("DROP INDEX idx_users_name;")

        # DROP TABLE must cascade-remove its remaining indexes
        db2.execute("DROP TABLE users;")
        assert db2.execute("SHOW INDEXES;").rows == []

    def test_dml_and_external_file_modification_invalidate_index(self, tmp_path):
        db = Database(tmp_path)
        db.execute("CREATE TABLE items (id NUMBER, tag STRING);")
        db.execute("CREATE INDEX idx_tag ON items (tag);")
        db.execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta');")

        assert len(db.execute("SELECT * FROM items WHERE tag = 'alpha';").rows) == 1

        # INSERT / UPDATE / DELETE update index immediately
        db.execute("INSERT INTO items VALUES (3, 'alpha');")
        assert [r['id'] for r in db.execute("SELECT * FROM items WHERE tag = 'alpha';").rows] == [1, 3]

        db.execute("UPDATE items SET tag = 'gamma' WHERE id = 1;")
        assert [r['id'] for r in db.execute("SELECT * FROM items WHERE tag = 'alpha';").rows] == [3]
        assert [r['id'] for r in db.execute("SELECT * FROM items WHERE tag = 'gamma';").rows] == [1]

        db.execute("DELETE FROM items WHERE tag = 'gamma';")
        assert db.execute("SELECT * FROM items WHERE tag = 'gamma';").rows == []

        # Simulate external user editing items.txt directly on disk
        tsv_path = tmp_path / 'items.txt'
        tsv_path.write_text(
            tsv_path.read_text(encoding='utf-8') + '"99"\t"ext_tag"\n',
            encoding='utf-8',
        )
        res_ext = db.execute("SELECT * FROM items WHERE tag = 'ext_tag';")
        assert [r['id'] for r in res_ext.rows] == [Decimal('99')]


class TestSingleTableIndexScanParity:
    def _seed_numbers(self, db: Database) -> None:
        db.execute("CREATE TABLE scores (id NUMBER, grade NUMBER, name STRING);")
        db.execute(
            "INSERT INTO scores VALUES "
            "(1, 10, 'a'), (2, -5, 'b'), (3, 25, 'c'), "
            "(4, NULL, 'd'), (5, 10.0, 'e'), (6, 30, 'f'), (7, 15, 'g');"
        )
        db.execute("CREATE INDEX idx_grade ON scores (grade);")
        db.execute("CREATE INDEX idx_name ON scores (name);")

    @pytest.mark.parametrize(
        'sql',
        [
            "SELECT id FROM scores WHERE grade = 10 ORDER BY id;",
            "SELECT id FROM scores WHERE 10.0 = grade ORDER BY id;",
            "SELECT id FROM scores WHERE grade = -5 ORDER BY id;",
            "SELECT id FROM scores WHERE grade = NULL;",
            "SELECT id FROM scores WHERE grade IN (15, 30, NULL) ORDER BY id;",
            "SELECT id FROM scores WHERE grade BETWEEN 10 AND 25 ORDER BY id;",
            "SELECT id FROM scores WHERE grade > 10 AND grade <= 25 ORDER BY id;",
            "SELECT id FROM scores WHERE grade >= 10 AND name <> 'a' ORDER BY id;",
            "SELECT id FROM scores WHERE name = 'c' AND grade > 20;",
            "SELECT id FROM scores WHERE grade BETWEEN 1 AND NULL;",
        ],
    )
    def test_index_scan_matches_unoptimized_results(self, tmp_path, sql):
        db_opt = Database(tmp_path, enable_optimizations=True)
        self._seed_numbers(db_opt)
        db_noopt = Database(tmp_path, enable_optimizations=False)

        assert db_opt.execute(sql).rows == db_noopt.execute(sql).rows

    def test_index_scan_preserves_type_mismatch_error(self, tmp_path):
        db = Database(tmp_path, enable_optimizations=True)
        self._seed_numbers(db)
        with pytest.raises(TypeMismatchError):
            db.execute("SELECT * FROM scores WHERE grade = 'not_a_number';")
        with pytest.raises(TypeMismatchError):
            db.execute("SELECT * FROM scores WHERE grade BETWEEN 'a' AND 'z';")
