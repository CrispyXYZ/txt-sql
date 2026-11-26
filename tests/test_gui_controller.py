"""Tests for txtsql.gui_controller, DEMO_TEMPLATES, txtsql.gui import, and CLI --help."""

import subprocess
import sys
from decimal import Decimal

import openpyxl
import pytest

from txtsql.exceptions import EngineError
from txtsql.gui_controller import (
    DEMO_TEMPLATES,
    NULL_DISPLAY,
    GuiController,
    build_create_table_sql,
    format_cell_display,
    format_cell_single_line,
    sort_rows_for_view,
    split_sql_statements,
)
from txtsql.result import StatementKind


class TestGuiHelpers:
    def test_split_sql_statements_ignores_strings_and_comments(self):
        script = (
            "-- comment with ; semicolon\n"
            "INSERT INTO t VALUES (1, 'a;b', 'O''Brien;still_str');\n"
            "SELECT * FROM t; -- trailing ; comment\n"
            "  -- only a comment at EOF\n"
        )
        stmts = split_sql_statements(script)
        assert len(stmts) == 2
        assert stmts[0].endswith("VALUES (1, 'a;b', 'O''Brien;still_str')")
        assert stmts[1] == 'SELECT * FROM t'

    def test_split_sql_single_statement_without_semicolon(self):
        assert split_sql_statements('  SELECT 1 FROM t  ') == ['SELECT 1 FROM t']
        assert split_sql_statements('   -- only comment\n  ') == []

    def test_format_cell_distinguishes_null_from_empty_string(self):
        assert format_cell_display(None) == NULL_DISPLAY == '<NULL>'
        assert format_cell_display('') == ''
        assert format_cell_display(Decimal('42')) == '42'

        assert format_cell_single_line(None) == '<NULL>'
        assert format_cell_single_line('') == ''
        assert format_cell_single_line('a\nb\tc') == 'a↵ b  c'
        long_text = 'x' * 20
        assert format_cell_single_line(long_text, max_len=10) == 'x' * 9 + '…'

    def test_build_create_table_sql_validation(self):
        sql = build_create_table_sql('users', [('id', 'NUMBER'), ('name', 'STRING')], if_not_exists=True)
        assert sql == 'CREATE TABLE IF NOT EXISTS users (id NUMBER, name STRING);'

        with pytest.raises(ValueError, match='表名'):
            build_create_table_sql('1bad', [('id', 'NUMBER')])
        with pytest.raises(ValueError, match='至少'):
            build_create_table_sql('ok', [])
        with pytest.raises(ValueError, match='重复'):
            build_create_table_sql('ok', [('id', 'NUMBER'), ('id', 'STRING')])
        with pytest.raises(ValueError, match='类型'):
            build_create_table_sql('ok', [('id', 'BOOLEAN')])

    def test_sort_rows_for_view_puts_nulls_last(self):
        rows = [
            {'id': Decimal('2'), 'v': None},
            {'id': Decimal('1'), 'v': Decimal('10')},
            {'id': Decimal('3'), 'v': Decimal('5')},
        ]
        asc = sort_rows_for_view(rows, 'v', descending=False)
        assert [r['v'] for r in asc] == [Decimal('5'), Decimal('10'), None]
        desc = sort_rows_for_view(rows, 'v', descending=True)
        assert [r['v'] for r in desc] == [Decimal('10'), Decimal('5'), None]


class TestGuiController:
    def test_directory_switch_metadata_filter_and_batch_execution(self, tmp_path):
        d1 = tmp_path / 'dir1'
        d2 = tmp_path / 'dir2'
        d1.mkdir()
        d2.mkdir()

        ctrl = GuiController(d1)
        report = ctrl.execute_script(
            "CREATE TABLE users (id NUMBER, bio STRING);\n"
            "INSERT INTO users VALUES (1, 'line1\nline2'), (2, NULL);\n"
            "SELECT * FROM users ORDER BY id ASC;\n"
        )
        assert report.error_count == 0
        assert report.schema_changed is True
        assert report.last_tabular is not None
        assert report.last_tabular.columns == ('id', 'bio')
        assert len(report.last_tabular.rows) == 2

        metas = ctrl.get_metadata()
        assert len(metas) == 1
        assert metas[0].name == 'users'
        assert metas[0].row_count == 2
        assert metas[0].columns == (('id', 'NUMBER'), ('bio', 'STRING'))
        assert ctrl.get_metadata('missing') == []

        # Switch to dir2: clean isolated database
        ctrl.switch_directory(d2)
        assert ctrl.last_tabular_result is None
        assert ctrl.get_metadata() == []

    def test_batch_stops_on_error_and_records_history(self, tmp_path):
        ctrl = GuiController(tmp_path)
        report = ctrl.execute_script(
            "CREATE TABLE t (id NUMBER);\n"
            "SELECT * FROM missing_table;\n"
            "INSERT INTO t VALUES (1);\n"
        )
        assert report.error_count == 1
        assert len(report.entries) == 2
        assert report.entries[0].ok is True
        assert report.entries[1].ok is False
        assert 'missing_table' in (report.entries[1].error or '')
        # Third statement must not have run
        assert ctrl.execute_script('SELECT * FROM t;').last_tabular.rows == []

    def test_import_and_export_via_controller(self, tmp_path):
        ctrl = GuiController(tmp_path)
        with pytest.raises(EngineError, match='没有可导出'):
            ctrl.export_last_result(tmp_path / 'empty.csv')

        xlsx_path = tmp_path / 'src.xlsx'
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(['id', 'name'])
        ws.append([1, 'Alice'])
        ws.append([2, 'Bob'])
        wb.save(xlsx_path)

        with pytest.raises(ValueError, match='表名'):
            ctrl.import_excel('bad-name', str(xlsx_path))

        entry = ctrl.import_excel('imported_users', str(xlsx_path))
        assert entry.ok is True
        assert entry.result is not None
        assert entry.result.kind == StatementKind.IMPORT
        assert entry.result.affected_rows == 2

        ctrl.execute_script('SELECT * FROM imported_users ORDER BY id ASC;')
        out_csv = tmp_path / 'out.csv'
        exported = ctrl.export_last_result(out_csv)
        assert exported == 2
        assert 'Alice' in out_csv.read_text(encoding='utf-8')

    @pytest.mark.parametrize('template_name', list(DEMO_TEMPLATES.keys()))
    def test_all_demo_templates_execute_cleanly(self, tmp_path, template_name):
        ctrl = GuiController(tmp_path)
        report = ctrl.execute_script(DEMO_TEMPLATES[template_name])
        assert report.error_count == 0
        assert report.last_tabular is not None
        assert len(report.last_tabular.columns) > 0
        assert len(report.last_tabular.rows) > 0
        # Re-running the same template in the same directory must also succeed cleanly
        report2 = ctrl.execute_script(DEMO_TEMPLATES[template_name])
        assert report2.error_count == 0

    def test_gui_module_imports_and_main_help(self):
        import txtsql.gui as gui_mod
        assert hasattr(gui_mod, 'run_gui')
        proc = subprocess.run(
            [sys.executable, '-m', 'txtsql', '--help'],
            capture_output=True,
            text=True,
            check=True,
        )
        assert '--gui' in proc.stdout
        assert '--dir' in proc.stdout
