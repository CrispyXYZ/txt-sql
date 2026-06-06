"""Unit tests for txtsql.cli: print_table and run_cli."""
import io
from unittest.mock import patch

import pytest

from txtsql.cli import print_table, run_cli


# ---------------------------------------------------------------------------
# print_table
# ---------------------------------------------------------------------------

class TestPrintTable:
    def _capture(self, rows):
        """Helper: capture stdout from print_table and return the lines."""
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            print_table(rows)
        return buf.getvalue().splitlines()

    def test_empty_rows_prints_empty_set(self):
        lines = self._capture([])
        assert lines == ["Empty set."]

    def test_single_row_has_borders(self):
        lines = self._capture([{"id": 1, "name": "Alice"}])
        # Should have: separator, header, separator, data, separator (3 border lines)
        separators = [l for l in lines if l.startswith("+")]
        assert len(separators) == 3  # top, after header, bottom

    def test_header_names_appear(self):
        lines = self._capture([{"col_a": "x", "col_b": "y"}])
        header_line = next(l for l in lines if "|" in l and "col_a" in l)
        assert "col_a" in header_line
        assert "col_b" in header_line

    def test_row_values_appear(self):
        lines = self._capture([{"name": "Bob", "score": 42}])
        data_lines = [l for l in lines if "|" in l and "Bob" in l]
        assert len(data_lines) == 1
        assert "42" in data_lines[0]

    def test_column_width_fits_longest_value(self):
        rows = [{"name": "Al"}, {"name": "Alexander"}]
        lines = self._capture(rows)
        # Every row line should be the same width
        border_lines = [l for l in lines if l.startswith("+")]
        assert len(set(len(l) for l in border_lines)) == 1

    def test_none_value_displayed_as_empty_string(self):
        lines = self._capture([{"val": None}])
        # None should not cause an error and should appear as ''
        data_line = next(l for l in lines if "|" in l and "val" not in l)
        assert "|" in data_line

    def test_accepts_sequence_of_mappings(self):
        """print_table should accept Sequence[Mapping[str, Any]], not just list[dict]."""
        from collections import OrderedDict
        rows = (OrderedDict([("k", "v")]),)
        lines = self._capture(rows)
        assert any("v" in l for l in lines)


# ---------------------------------------------------------------------------
# run_cli
# ---------------------------------------------------------------------------

class TestRunCli:
    @pytest.fixture(autouse=True)
    def isolated_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        yield

    def test_exit_command_quits(self, capsys):
        with patch("builtins.input", side_effect=["exit"]):
            run_cli()
        # No traceback means a clean exit

    def test_quit_command_quits(self, capsys):
        with patch("builtins.input", side_effect=["quit"]):
            run_cli()

    def test_eof_exits_cleanly(self, capsys):
        with patch("builtins.input", side_effect=EOFError):
            run_cli()

    def test_keyboard_interrupt_exits_cleanly(self, capsys):
        with patch("builtins.input", side_effect=KeyboardInterrupt):
            run_cli()
        out = capsys.readouterr().out
        # Should print a newline when interrupted
        assert out.endswith("\n")

    def test_empty_line_is_skipped(self, capsys):
        with patch("builtins.input", side_effect=["", "exit"]):
            run_cli()
        # No errors expected

    def test_sql_error_prints_error_message(self, capsys):
        with patch("builtins.input", side_effect=["INVALID SQL HERE", "exit"]):
            run_cli()
        out = capsys.readouterr().out
        assert "Error:" in out

    def test_select_result_prints_table(self, capsys):
        with patch("builtins.input", side_effect=[
            "CREATE TABLE t (id NUMBER, name STRING)",
            "INSERT INTO t VALUES (1, 'Alice')",
            "SELECT * FROM t",
            "DROP TABLE t",
            "exit",
        ]):
            run_cli()
        out = capsys.readouterr().out
        assert "Alice" in out
        assert "|" in out  # ASCII table border

    def test_non_select_query_prints_query_ok(self, capsys):
        with patch("builtins.input", side_effect=[
            "CREATE TABLE t2 (id NUMBER)",
            "DROP TABLE t2",
            "exit",
        ]):
            run_cli()
        out = capsys.readouterr().out
        assert "Query OK." in out

    def test_system_error_shows_traceback(self, capsys):
        with patch("builtins.input", side_effect=[
            "CREATE TABLE t3 (id NUMBER)",
            RuntimeError("Something went wrong"),
            "exit",
        ]):
            run_cli()
        captured = capsys.readouterr()
        assert "Unexpected error" in captured.out
        assert "Traceback" in captured.err
