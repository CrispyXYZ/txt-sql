"""Tests for EXPLAIN / EXPLAIN ANALYZE, plan visualization helpers, CHECK TABLE, and transactions."""

from txtsql import Database
from txtsql.cli import print_table
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
