"""Tests for EXPLAIN / EXPLAIN ANALYZE, plan visualization helpers, CHECK TABLE, and transactions."""

from txtsql import Database
from txtsql.cli import print_table
from txtsql.exceptions import EngineError
from txtsql.gui_controller import GuiController
from txtsql.plan import collect_plan_optimizations
from txtsql.result import StatementKind


class TestExplainAndPlan:
    def _setup_sample(self, db: Database) -> None:
        db.execute("CREATE TABLE users (id NUMBER, name STRING, city STRING);")
        db.execute("INSERT INTO users VALUES (1, 'A', 'Harbin'), (2, 'B', 'Beijing'), (3, 'C', 'Harbin');")
        db.execute("CREATE TABLE orders (oid NUMBER, uid NUMBER, amount NUMBER);")
        db.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 1, 400), (12, 2, 250), (13, 3, 500);")
        db.execute("CREATE INDEX idx_u_city ON users (city);")

    def test_explain_and_explain_analyze_capture_optimizations_and_metrics(self, tmp_path, capsys):
        db = Database(tmp_path, enable_optimizations=True)
        self._setup_sample(db)

        sql = (
            "SELECT u.name, o.oid, o.amount "
            "FROM users u JOIN orders o ON u.id = o.uid "
            "WHERE u.city = 'Harbin' AND o.amount >= 400 "
            "ORDER BY o.amount DESC;"
        )

        res_static = db.execute("EXPLAIN " + sql)
        assert res_static.kind == StatementKind.EXPLAIN
        assert res_static.plan is not None
        assert res_static.ast_tree is not None
        assert len(res_static.tokens_info) > 0
        opts_static = collect_plan_optimizations(res_static.plan)
        assert 'Index Scan' in opts_static
        assert 'Hash Join' in opts_static
        assert 'Predicate Pushdown' in opts_static

        res_rt = db.execute("EXPLAIN ANALYZE " + sql)
        assert res_rt.kind == StatementKind.EXPLAIN
        assert res_rt.plan is not None
        assert res_rt.plan.rows_out == 2  # oid 11 (400) and oid 13 (500)
        assert res_rt.plan.time_ms is not None
        assert any(r['Optimization'] != '-' for r in res_rt.rows)
        print_table(res_rt.rows)
        rendered = capsys.readouterr().out
        assert 'HashJoin' in rendered
        assert 'IndexScan' in rendered

    def test_explain_union_and_disabled_optimizations(self, tmp_path):
        db = Database(tmp_path, enable_optimizations=False)
        self._setup_sample(db)
        res = db.execute(
            "EXPLAIN ANALYZE "
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.uid "
            "UNION ALL "
            "SELECT u.name, o.amount FROM users u, orders o WHERE u.id = o.uid;"
        )
        assert res.kind == StatementKind.EXPLAIN
        assert res.plan is not None
        assert res.plan.operator == 'Union'
        assert collect_plan_optimizations(res.plan) == []

    def test_gui_controller_explain_sql_and_index_metadata(self, tmp_path):
        ctrl = GuiController(tmp_path)
        self._setup_sample(ctrl.db)

        metas = ctrl.get_metadata('users')
        assert len(metas) == 1
        assert metas[0].indexes == (('idx_u_city', 'city'),)

        report = ctrl.explain_sql("SELECT * FROM users WHERE city = 'Harbin';", analyze=True)
        assert report.error_count == 0
        assert ctrl.last_plan_result is not None
        assert ctrl.last_plan_result.kind == StatementKind.EXPLAIN
        assert ctrl.last_plan_result.plan is not None
        assert ctrl.last_plan_result.plan.rows_out == 2


class TestCheckTableAndTransactions:
    def test_check_table_detects_corrupted_tsv_rows(self, tmp_path):
        db = Database(tmp_path)
        db.execute("CREATE TABLE accounts (id NUMBER, owner STRING, balance NUMBER);")
        db.execute("INSERT INTO accounts VALUES (1, 'Alice', 100), (2, 'Bob', NULL);")

        ok_res = db.execute("CHECK TABLE accounts;")
        assert ok_res.kind == StatementKind.SYSTEM
        assert ok_res.rows == [{'Line': 0, 'Status': 'OK', 'Detail': 'Checked 2 rows, 0 issues.'}]

        # Corrupt TSV directly: line 3 has wrong column count, line 4 has non-numeric balance
        tsv = tmp_path / 'accounts.txt'
        tsv.write_text(
            tsv.read_text(encoding='utf-8')
            + '"3"\t"OnlyTwoCols"\n'
            + '"4"\t"Dan"\t"not_a_num"\n',
            encoding='utf-8',
        )
        bad_res = db.execute("CHECK TABLE accounts;")
        assert len(bad_res.rows) == 2
        assert bad_res.rows[0]['Line'] == 3
        assert bad_res.rows[0]['Status'] == 'ERROR'
        assert 'Column count' in str(bad_res.rows[0]['Detail'])
        assert bad_res.rows[1]['Line'] == 4
        assert 'Invalid NUMBER' in str(bad_res.rows[1]['Detail'])

    def test_snapshot_transactions_commit_rollback_and_errors(self, tmp_path):
        import pytest

        db = Database(tmp_path)
        db.execute("CREATE TABLE items (id NUMBER, name STRING);")
        db.execute("CREATE INDEX idx_items_id ON items (id);")
        db.execute("INSERT INTO items VALUES (1, 'one');")

        # Cannot commit/rollback without active transaction
        with pytest.raises(EngineError, match='No active transaction'):
            db.execute("COMMIT;")
        with pytest.raises(EngineError, match='No active transaction'):
            db.execute("ROLLBACK;")

        # Begin -> mutate data + create extra table + drop index -> Rollback
        db.execute("BEGIN TRANSACTION;")
        assert db.storage.in_transaction is True
        with pytest.raises(EngineError, match='already active'):
            db.execute("BEGIN;")

        db.execute("INSERT INTO items VALUES (2, 'two');")
        db.execute("UPDATE items SET name = 'changed' WHERE id = 1;")
        db.execute("CREATE TABLE temp_tbl (x NUMBER);")
        db.execute("DROP INDEX idx_items_id;")
        assert len(db.execute("SELECT * FROM items;").rows) == 2

        db.execute("ROLLBACK TRANSACTION;")
        assert db.storage.in_transaction is False
        assert db.execute("SELECT * FROM items;").rows == [{'id': 1, 'name': 'one'}]
        assert not (tmp_path / 'temp_tbl.txt').exists()
        assert len(db.execute("SHOW INDEXES FROM items;").rows) == 1

        # Begin -> insert -> Commit persists cleanly
        db.execute("BEGIN;")
        db.execute("INSERT INTO items VALUES (2, 'two');")
        db.execute("COMMIT;")
        assert db.storage.in_transaction is False
        assert [r['id'] for r in db.execute("SELECT * FROM items ORDER BY id;").rows] == [1, 2]

