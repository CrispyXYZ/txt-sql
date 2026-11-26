"""Tests for the deepened architecture: Database(data_dir), QueryResult, EvalContext, and io_handlers."""

from decimal import Decimal

import openpyxl

from txtsql import Database, QueryResult, StatementKind
from txtsql.ast import ColumnExpression, BinaryExpression, ArithmeticOp, LiteralExpression
from txtsql.evaluator import EvalContext, evaluate_expression, is_number, resolve_column, to_bool, to_decimal
from txtsql.io_handlers import export_rows, read_excel_table
from txtsql.types import Types


class TestDatabaseIsolation:
    def test_multiple_databases_isolated_without_chdir(self, tmp_path):
        db1 = Database(tmp_path / 'db1')
        db2 = Database(tmp_path / 'db2')

        db1.execute('CREATE TABLE users (id NUMBER, name STRING);')
        db2.execute('CREATE TABLE users (id NUMBER, name STRING);')

        db1.execute("INSERT INTO users VALUES (1, 'Alice');")
        db2.execute("INSERT INTO users VALUES (10, 'Bob'), (20, 'Charlie');")

        r1 = db1.execute('SELECT * FROM users;')
        r2 = db2.execute('SELECT * FROM users ORDER BY id ASC;')

        assert r1.rows == [{'id': Decimal('1'), 'name': 'Alice'}]
        assert r2.rows == [
            {'id': Decimal('10'), 'name': 'Bob'},
            {'id': Decimal('20'), 'name': 'Charlie'},
        ]
        assert (tmp_path / 'db1' / 'metadata.txt').exists()
        assert (tmp_path / 'db2' / 'users.txt').exists()
        assert len(db1.list_tables()) == 1
        assert db2.get_table('users').count_rows() == 2

        # Dropping in db1 does not affect db2
        db1.execute('DROP TABLE users;')
        assert db1.list_tables() == []
        assert db2.get_table('users').count_rows() == 2


class TestStructuredQueryResult:
    def test_empty_select_preserves_columns(self, tmp_path):
        db = Database(tmp_path)
        db.execute('CREATE TABLE a (id NUMBER, x STRING);')
        db.execute('CREATE TABLE b (id NUMBER, y STRING);')

        res_single = db.execute('SELECT id, x AS label FROM a WHERE id = 999;')
        assert isinstance(res_single, QueryResult)
        assert res_single.kind == StatementKind.SELECT
        assert res_single.is_tabular is True
        assert res_single.columns == ('id', 'label')
        assert res_single.rows == []
        assert res_single.to_legacy() == []

        res_star = db.execute('SELECT * FROM a;')
        assert res_star.columns == ('id', 'x')

        res_join = db.execute('SELECT * FROM a JOIN b USING (id);')
        assert res_join.columns == ('id', 'x', 'y')

    def test_dml_ddl_system_union_import_export_results(self, tmp_path):
        db = Database(tmp_path / 'data')

        ddl_create = db.execute('CREATE TABLE items (id NUMBER, title STRING);')
        assert ddl_create.kind == StatementKind.DDL
        assert ddl_create.is_tabular is False
        assert ddl_create.to_legacy() is None

        ins = db.execute("INSERT INTO items VALUES (1, 'A'), (2, 'B');")
        assert ins.kind == StatementKind.INSERT
        assert ins.affected_rows == 2
        assert ins.to_legacy() is None

        upd = db.execute("UPDATE items SET title = 'Z' WHERE id = 2;")
        assert upd.kind == StatementKind.UPDATE
        assert upd.affected_rows == 1
        assert upd.to_legacy() == 1

        sys_show = db.execute('SHOW TABLES;')
        assert sys_show.kind == StatementKind.SYSTEM
        assert sys_show.columns == ('Table', 'Columns', 'Rows')
        assert sys_show.rows == [{'Table': 'items', 'Columns': 'id, title', 'Rows': 2}]

        sys_desc = db.execute('DESCRIBE items;')
        assert sys_desc.kind == StatementKind.SYSTEM
        assert sys_desc.columns == ('Column', 'Type')

        uni = db.execute('SELECT id FROM items UNION ALL SELECT id FROM items;')
        assert uni.kind == StatementKind.UNION
        assert uni.columns == ('id',)
        assert len(uni.rows) == 4

        out_csv = tmp_path / 'out.csv'
        exp = db.execute(f"SELECT * FROM items INTO OUTFILE '{out_csv}';")
        assert exp.kind == StatementKind.EXPORT
        assert exp.affected_rows == 2
        assert exp.to_legacy() == 2

        xlsx_path = tmp_path / 'in.xlsx'
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(['k', 'v'])
        ws.append([7, 'seven'])
        wb.save(xlsx_path)

        imp = db.execute(f"IMPORT imported FROM '{xlsx_path}';")
        assert imp.kind == StatementKind.IMPORT
        assert imp.affected_rows == 1
        assert imp.to_legacy() == 1

        dele = db.execute('DELETE FROM items WHERE id = 1;')
        assert dele.kind == StatementKind.DELETE
        assert dele.affected_rows == 1
        assert dele.to_legacy() == 1

        db.execute('CREATE TABLE items_copy (id NUMBER, title STRING);')
        ins_sel = db.execute('INSERT INTO items_copy SELECT id, title FROM items;')
        assert ins_sel.kind == StatementKind.INSERT
        assert ins_sel.affected_rows == 1
        assert ins_sel.to_legacy() is None

        empty_join = db.execute('SELECT items.*, items_copy.title FROM items JOIN items_copy ON items.id = items_copy.id WHERE items.id = 999;')
        assert empty_join.columns == ('id', 'title', 'items_copy.title')
        assert empty_join.rows == []


class TestPublicEvaluatorAndIOHandlers:
    def test_evaluator_public_helpers_and_context(self):
        assert is_number(Decimal('1')) is True
        assert is_number(True) is True
        assert is_number('1') is False
        assert to_decimal(True) == Decimal('1')
        assert to_decimal(False) == Decimal('0')
        assert to_bool(None) is False
        assert to_bool(Decimal('0')) is False
        assert to_bool(Decimal('2')) is True

        row = {'id': Decimal('5'), 'name': 'Alice'}
        assert resolve_column(row, 'name', table='emp', valid_single_qualifier='emp') == 'Alice'

        expr = BinaryExpression(ColumnExpression('id'), ArithmeticOp.ADD, LiteralExpression(Decimal('3')))
        ctx = EvalContext(valid_single_qualifier='emp')
        assert evaluate_expression(expr, row, ctx) == Decimal('8')

    def test_io_handlers_roundtrip(self, tmp_path):
        xlsx_path = tmp_path / 'data.xlsx'
        rows = [{'id': Decimal('1'), 'name': 'Alice'}, {'id': Decimal('2'), 'name': None}]
        written = export_rows(rows, ['id', 'name'], xlsx_path)
        assert written == 2

        defs, loaded = read_excel_table(xlsx_path)
        assert defs == {'id': Types.NUMBER, 'name': Types.STRING}
        assert loaded == [{'id': Decimal('1'), 'name': 'Alice'}, {'id': Decimal('2'), 'name': None}]
