"""Tkinter GUI client for TxtSql (Chinese interface)."""

from __future__ import annotations

import logging
import os
import re
import tkinter as tk
import tkinter.font as tkfont
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from .exceptions import TxtSqlError
from .gui_controller import (
    DEMO_TEMPLATES,
    SQL_KEYWORDS,
    CompletionItem,
    CompletionResult,
    ExecutionEntry,
    GuiController,
    StatementSpan,
    build_create_table_sql,
    find_active_statement,
    format_cell_display,
    format_cell_single_line,
    sort_rows_for_view,
)
from .result import QueryResult, StatementKind


_KW_PATTERN = re.compile(r'\b(' + '|'.join(SQL_KEYWORDS) + r')\b', re.IGNORECASE)
_STR_PATTERN = re.compile(r"'(?:''|[^'])*'")
_NUM_PATTERN = re.compile(r'\b\d+(?:\.\d+)?\b')
_COMMENT_PATTERN = re.compile(r'--[^\n]*')


def _pick_mono_font(root: tk.Misc) -> str:
    available = set(tkfont.families(root))
    for candidate in ('JetBrains Mono', 'DejaVu Sans Mono', 'Consolas', 'Liberation Mono', 'Courier New'):
        if candidate in available:
            return candidate
    return 'TkFixedFont'


class _TkLogHandler(logging.Handler):
    """Logging handler forwarding log records into a Tkinter Text widget."""

    def __init__(self, callback) -> None:
        super().__init__()
        self._callback = callback
        self.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', '%H:%M:%S'))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._callback(msg, record.levelname)
        except Exception:
            pass


def _configure_dialog(
    dlg: tk.Toplevel,
    parent: tk.Misc,
    base_w: int,
    base_h: int,
    min_w: int,
    min_h: int,
) -> None:
    """Size a dialog with HiDPI headroom and center it over its parent window."""
    dlg.update_idletasks()
    sw = dlg.winfo_screenwidth()
    sh = dlg.winfo_screenheight()
    w = min(int(sw * 0.85), max(base_w, dlg.winfo_reqwidth() + 48))
    h = min(int(sh * 0.82), max(base_h, dlg.winfo_reqheight() + 40))

    parent.update_idletasks()
    px = parent.winfo_rootx()
    py = parent.winfo_rooty()
    pw = parent.winfo_width() or sw
    ph = parent.winfo_height() or sh
    x = max(0, min(sw - w, px + (pw - w) // 2))
    y = max(0, min(sh - h, py + (ph - h) // 2))

    dlg.geometry(f'{w}x{h}+{x}+{y}')
    dlg.minsize(min(min_w, w), min(min_h, h))


class CreateTableDialog(tk.Toplevel):
    """Modal dialog for creating a new table visually."""

    def __init__(self, parent: tk.Misc, on_submit) -> None:
        super().__init__(parent)
        self.title('新建表')
        self.transient(parent)
        self.grab_set()
        self._on_submit = on_submit
        self._row_widgets: list[tuple[ttk.Frame, tk.StringVar, tk.StringVar]] = []

        self._build_ui()
        self._add_column_row('id', 'NUMBER')
        self._add_column_row('name', 'STRING')
        _configure_dialog(self, parent, base_w=780, base_h=580, min_w=640, min_h=480)

    def _build_ui(self) -> None:
        outer = ttk.Frame(self, padding=16)
        outer.pack(fill='both', expand=True)

        top = ttk.Frame(outer)
        top.pack(fill='x', pady=(0, 12))
        ttk.Label(top, text='表名：').pack(side='left')
        self.table_name_var = tk.StringVar()
        name_entry = ttk.Entry(top, textvariable=self.table_name_var, width=30)
        name_entry.pack(side='left', padx=(6, 16))
        name_entry.focus_set()

        self.if_not_exists_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text='IF NOT EXISTS', variable=self.if_not_exists_var).pack(side='left')

        header = ttk.Frame(outer)
        header.pack(fill='x', pady=(4, 8))
        ttk.Label(header, text='字段定义（至少 1 列）：').pack(side='left')
        ttk.Button(header, text='+ 添加列', command=self._add_column_row).pack(side='right')

        col_head = ttk.Frame(outer)
        col_head.pack(fill='x', pady=(2, 4))
        ttk.Label(col_head, text='列名', width=28).pack(side='left', padx=(8, 10))
        ttk.Label(col_head, text='数据类型', width=16).pack(side='left')

        list_container = ttk.Frame(outer, relief='solid', borderwidth=1)
        list_container.pack(fill='both', expand=True, pady=(0, 14))
        self._canvas = tk.Canvas(list_container, highlightthickness=0, height=240)
        scrollbar = ttk.Scrollbar(list_container, orient='vertical', command=self._canvas.yview)
        self._rows_frame = ttk.Frame(self._canvas, padding=8)
        self._rows_frame.bind(
            '<Configure>',
            lambda _e: self._canvas.configure(scrollregion=self._canvas.bbox('all')),
        )
        win_id = self._canvas.create_window((0, 0), window=self._rows_frame, anchor='nw')
        self._canvas.bind('<Configure>', lambda e: self._canvas.itemconfigure(win_id, width=e.width))
        self._canvas.configure(yscrollcommand=scrollbar.set)
        self._canvas.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        btns = ttk.Frame(outer)
        btns.pack(fill='x')
        ttk.Button(btns, text='取消', command=self.destroy).pack(side='right', padx=(8, 0))
        ttk.Button(btns, text='确认建表', style='Accent.TButton', command=self._submit).pack(side='right')

    def _add_column_row(self, default_name: str = '', default_type: str = 'STRING') -> None:
        row_frame = ttk.Frame(self._rows_frame)
        row_frame.pack(fill='x', pady=6)
        name_var = tk.StringVar(value=default_name)
        type_var = tk.StringVar(value=default_type)

        ttk.Entry(row_frame, textvariable=name_var, width=28).pack(side='left', padx=(2, 10))
        combo = ttk.Combobox(
            row_frame, textvariable=type_var, values=('STRING', 'NUMBER'), state='readonly', width=14)
        combo.pack(side='left', padx=(0, 10))

        entry = (row_frame, name_var, type_var)
        self._row_widgets.append(entry)
        ttk.Button(row_frame, text='删除', command=lambda e=entry: self._remove_row(e)).pack(side='left')

    def _remove_row(self, entry) -> None:
        if len(self._row_widgets) <= 1:
            messagebox.showwarning('提示', '表至少需要保留 1 个列。', parent=self)
            return
        self._row_widgets.remove(entry)
        entry[0].destroy()

    def _submit(self) -> None:
        cols = [(n.get(), t.get()) for _, n, t in self._row_widgets]
        try:
            sql = build_create_table_sql(
                self.table_name_var.get(),
                cols,
                if_not_exists=self.if_not_exists_var.get(),
            )
        except ValueError as exc:
            messagebox.showerror('输入校验失败', str(exc), parent=self)
            return
        self._on_submit(sql)
        self.destroy()


class ImportTableDialog(tk.Toplevel):
    """Modal dialog for specifying target table name when importing an Excel file."""

    def __init__(self, parent: tk.Misc, file_path: str, default_name: str) -> None:
        super().__init__(parent)
        self.title('导入 Excel 工作簿')
        self.transient(parent)
        self.grab_set()
        self.result: str | None = None

        outer = ttk.Frame(self, padding=16)
        outer.pack(fill='both', expand=True)

        ttk.Label(outer, text='源文件：').pack(anchor='w')
        path_var = tk.StringVar(value=file_path)
        ttk.Entry(outer, textvariable=path_var, state='readonly').pack(fill='x', pady=(4, 12))

        ttk.Label(outer, text='目标表名（首行为列名，自动推断 STRING / NUMBER 类型）：').pack(anchor='w')
        self._name_var = tk.StringVar(value=default_name)
        entry = ttk.Entry(outer, textvariable=self._name_var)
        entry.pack(fill='x', pady=(4, 16))
        entry.select_range(0, 'end')
        entry.focus_set()

        btns = ttk.Frame(outer)
        btns.pack(fill='x')
        ttk.Button(btns, text='取消', command=self.destroy).pack(side='right', padx=(8, 0))
        ttk.Button(btns, text='开始导入', style='Accent.TButton', command=self._confirm).pack(side='right')

        self.bind('<Return>', lambda _e: self._confirm())
        self.bind('<Escape>', lambda _e: self.destroy())
        _configure_dialog(self, parent, base_w=620, base_h=260, min_w=520, min_h=230)

    def _confirm(self) -> None:
        name = self._name_var.get().strip()
        if not name:
            messagebox.showwarning('提示', '请输入目标表名。', parent=self)
            return
        self.result = name
        self.destroy()


class TxtSqlApp:
    """Main Tkinter desktop window for TxtSql."""

    def __init__(self, root: tk.Tk, data_dir: str | os.PathLike[str] = '.') -> None:
        self.root = root
        self.controller = GuiController(data_dir)
        self._mono_font = _pick_mono_font(root)
        self._highlight_job: str | None = None
        self._complete_job: str | None = None
        self._suppress_space_release: bool = False
        self._completion_popup: tk.Toplevel | None = None
        self._completion_listbox: tk.Listbox | None = None
        self._completion_result: CompletionResult | None = None
        self._sort_state: tuple[str, bool] | None = None
        self._current_columns: tuple[str, ...] = ()
        self._displayed_rows: list = []
        self._log_handler: _TkLogHandler | None = None

        self._configure_window()
        self._configure_styles()
        self._build_menu()
        self._build_toolbar()
        self._build_main_panes()
        self._build_statusbar()
        self._bind_shortcuts()
        self._attach_logger()

        self.refresh_metadata()
        self._finalize_window_geometry()
        self._append_message('欢迎使用 TxtSql 图形客户端，输入 SQL 后按 F5 或 Ctrl+Enter 执行。', 'info')
        self.root.protocol('WM_DELETE_WINDOW', self._on_close)

    # ------------------------------------------------------------------
    # Window & Style setup
    # ------------------------------------------------------------------

    def _configure_window(self) -> None:
        self._update_title()

    def _finalize_window_geometry(self) -> None:
        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        req_w = self.root.winfo_reqwidth()
        req_h = self.root.winfo_reqheight()
        win_w = min(int(sw * 0.90), max(1360, req_w + 80))
        win_h = min(int(sh * 0.88), max(860, req_h + 60))
        x = max(0, (sw - win_w) // 2)
        y = max(0, (sh - win_h) // 2)
        self.root.geometry(f'{win_w}x{win_h}+{x}+{y}')
        self.root.minsize(min(1040, win_w), min(680, win_h))
        self.root.after(50, lambda: self._apply_initial_sashes(win_w, win_h))

    def _apply_initial_sashes(self, win_w: int, win_h: int) -> None:
        try:
            self._h_pane.sashpos(0, max(300, int(win_w * 0.24)))
            self._v_pane.sashpos(0, int(win_h * 0.36))
        except tk.TclError:
            pass

    def _update_title(self) -> None:
        self.root.title(f'TxtSql - {self.controller.data_dir}')

    def _configure_styles(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use('clam')
        except tk.TclError:
            pass
        self._body_font = tkfont.nametofont('TkDefaultFont')
        self._heading_font = tkfont.Font(font=self._body_font)
        self._heading_font.configure(weight='bold')
        line_h = self._body_font.metrics('linespace') or 20
        row_h = max(34, int(line_h * 1.65))

        style.configure('TButton', padding=(10, 5))
        style.configure('Accent.TButton', padding=(12, 5), foreground='#ffffff', background='#2563eb')
        style.map(
            'Accent.TButton',
            background=[('active', '#1d4ed8'), ('pressed', '#1e40af')],
        )
        style.configure('TEntry', padding=(4, 3))
        style.configure('TNotebook.Tab', padding=(12, 6))
        style.configure('Treeview', rowheight=row_h)
        style.configure('Treeview.Heading', padding=(8, 6), font=self._heading_font)

    # ------------------------------------------------------------------
    # Menu & Toolbar
    # ------------------------------------------------------------------

    def _build_menu(self) -> None:
        menubar = tk.Menu(self.root)

        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label='切换数据目录...', accelerator='Ctrl+O', command=self._choose_directory)
        file_menu.add_separator()
        file_menu.add_command(label='打开 SQL 脚本...', command=self._open_sql_file)
        file_menu.add_command(label='保存 SQL 脚本...', accelerator='Ctrl+S', command=self._save_sql_file)
        file_menu.add_separator()
        file_menu.add_command(label='导入 Excel (.xlsx)...', command=self._import_excel)
        file_menu.add_command(label='导出当前结果...', command=self._export_result)
        file_menu.add_separator()
        file_menu.add_command(label='退出', accelerator='Ctrl+Q', command=self._on_close)
        menubar.add_cascade(label='文件', menu=file_menu)

        run_menu = tk.Menu(menubar, tearoff=0)
        run_menu.add_command(label='运行当前语句', accelerator='Ctrl+Enter', command=self._run_current_statement)
        run_menu.add_command(label='运行全部语句', accelerator='F5', command=self._run_all)
        run_menu.add_command(label='运行选中语句', accelerator='Ctrl+Shift+Enter', command=self._run_selected)
        run_menu.add_separator()
        run_menu.add_command(label='触发代码补全', accelerator='Ctrl+Space', command=lambda: self._trigger_completions(manual=True))
        run_menu.add_command(label='清空编辑器', accelerator='Ctrl+L', command=self._clear_editor)
        menubar.add_cascade(label='执行', menu=run_menu)

        demo_menu = tk.Menu(menubar, tearoff=0)
        for title, sql in DEMO_TEMPLATES.items():
            demo_menu.add_command(label=title, command=lambda s=sql, t=title: self._load_template(t, s))
        menubar.add_cascade(label='示例模板', menu=demo_menu)

        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label='快捷键与 SQL 速查', command=self._show_help)
        help_menu.add_command(label='关于 TxtSql', command=self._show_about)
        menubar.add_cascade(label='帮助', menu=help_menu)

        self.root.config(menu=menubar)

    def _build_toolbar(self) -> None:
        container = ttk.Frame(self.root, padding=(10, 6))
        container.pack(fill='x')

        row1 = ttk.Frame(container)
        row1.pack(fill='x', pady=(0, 6))
        ttk.Label(row1, text='数据目录：').pack(side='left')
        self.dir_var = tk.StringVar(value=str(self.controller.data_dir))
        dir_label = ttk.Entry(row1, textvariable=self.dir_var, state='readonly')
        dir_label.pack(side='left', fill='x', expand=True, padx=(4, 8))
        ttk.Button(row1, text='切换目录', command=self._choose_directory).pack(side='left', padx=2)
        ttk.Button(row1, text='刷新', command=self.refresh_metadata).pack(side='left', padx=2)

        row2 = ttk.Frame(container)
        row2.pack(fill='x')
        ttk.Button(row2, text='▶ 运行当前 (Ctrl+Enter)', style='Accent.TButton', command=self._run_current_statement).pack(side='left', padx=(0, 4))
        ttk.Button(row2, text='运行全部 (F5)', command=self._run_all).pack(side='left', padx=2)
        ttk.Button(row2, text='运行选中', command=self._run_selected).pack(side='left', padx=2)
        ttk.Button(row2, text='清空', command=self._clear_editor).pack(side='left', padx=2)

        ttk.Separator(row2, orient='vertical').pack(side='left', fill='y', padx=10, pady=2)

        ttk.Button(row2, text='导入 Excel', command=self._import_excel).pack(side='left', padx=2)
        ttk.Button(row2, text='导出结果', command=self._export_result).pack(side='left', padx=2)

        ttk.Separator(self.root, orient='horizontal').pack(fill='x')

    # ------------------------------------------------------------------
    # Main Panes: Left Metadata + Right (Editor / Results)
    # ------------------------------------------------------------------

    def _build_main_panes(self) -> None:
        h_pane = ttk.PanedWindow(self.root, orient='horizontal')
        h_pane.pack(fill='both', expand=True, padx=8, pady=8)
        self._h_pane = h_pane

        # Left sidebar
        left_frame = ttk.Frame(h_pane, padding=6)
        self._build_metadata_panel(left_frame)
        h_pane.add(left_frame, weight=1)

        # Right vertical split
        v_pane = ttk.PanedWindow(h_pane, orient='vertical')
        self._v_pane = v_pane
        editor_frame = ttk.Frame(v_pane, padding=6)
        self._build_editor_panel(editor_frame)
        v_pane.add(editor_frame, weight=2)

        bottom_frame = ttk.Frame(v_pane, padding=6)
        self._build_bottom_notebook(bottom_frame)
        v_pane.add(bottom_frame, weight=3)

        h_pane.add(v_pane, weight=4)

    def _build_metadata_panel(self, parent: ttk.Frame) -> None:
        title_row = ttk.Frame(parent)
        title_row.pack(fill='x', pady=(0, 6))
        ttk.Label(title_row, text='数据表浏览器', font=('TkDefaultFont', 10, 'bold')).pack(side='left')

        btn_row = ttk.Frame(parent)
        btn_row.pack(fill='x', pady=(0, 8))
        ttk.Button(btn_row, text='+ 新建表', command=self._open_create_table_dialog).pack(side='left')
        ttk.Button(btn_row, text='刷新', command=self.refresh_metadata).pack(side='left', padx=(6, 0))

        search_row = ttk.Frame(parent)
        search_row.pack(fill='x', pady=(0, 8))
        ttk.Label(search_row, text='筛选：').pack(side='left')
        self.meta_filter_var = tk.StringVar()
        self.meta_filter_var.trace_add('write', lambda *_: self.refresh_metadata())
        ttk.Entry(search_row, textvariable=self.meta_filter_var).pack(side='left', fill='x', expand=True)

        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill='both', expand=True)

        self.meta_tree = ttk.Treeview(tree_frame, show='tree', selectmode='browse')
        vsb = ttk.Scrollbar(tree_frame, orient='vertical', command=self.meta_tree.yview)
        hsb = ttk.Scrollbar(parent, orient='horizontal', command=self.meta_tree.xview)
        self.meta_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.meta_tree.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')
        hsb.pack(fill='x')

        self.meta_tree.bind('<Double-1>', self._on_meta_double_click)
        self.meta_tree.bind('<Button-3>', self._on_meta_right_click)

        self.meta_menu = tk.Menu(self.root, tearoff=0)
        self.meta_menu.add_command(label='预览前 100 行', command=self._preview_selected_table)
        self.meta_menu.add_command(label='查看表结构 (DESCRIBE)', command=self._describe_selected_table)
        self.meta_menu.add_command(label='插入表名到编辑器', command=self._insert_selected_table_name)
        self.meta_menu.add_separator()
        self.meta_menu.add_command(label='删除表 (DROP TABLE)', command=self._drop_selected_table)

    def _build_editor_panel(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.pack(fill='x', pady=(0, 6))
        ttk.Label(header, text='SQL 编辑器', font=('TkDefaultFont', 10, 'bold')).pack(side='left')
        ttk.Label(header, text='  (蓝底高亮当前语句: Ctrl+Enter 运行当前 / F5 运行全部 / Ctrl+Space 补全)', foreground='#64748b').pack(side='left')

        self.history_combo_var = tk.StringVar()
        self.history_combo = ttk.Combobox(header, textvariable=self.history_combo_var, state='readonly', width=28)
        self.history_combo.pack(side='right')
        self.history_combo.bind('<<ComboboxSelected>>', self._on_history_combo_select)
        ttk.Label(header, text='历史快速回填：').pack(side='right', padx=(8, 4))

        body = ttk.Frame(parent, relief='solid', borderwidth=1)
        body.pack(fill='both', expand=True)

        self.line_numbers = tk.Text(
            body, width=4, padx=6, pady=6, takefocus=0, border=0,
            background='#f1f5f9', foreground='#64748b',
            spacing1=3, spacing3=3,
            state='disabled', font=(self._mono_font, 11),
        )
        self.line_numbers.tag_configure(
            'active_line', background='#dbeafe', foreground='#1d4ed8',
            font=(self._mono_font, 11, 'bold'),
        )
        self.line_numbers.pack(side='left', fill='y')

        self.sql_text = tk.Text(
            body, wrap='none', undo=True, border=0, padx=8, pady=6,
            spacing1=3, spacing3=3,
            font=(self._mono_font, 11), insertbackground='#0f172a',
        )
        ysb = ttk.Scrollbar(body, orient='vertical', command=self._sync_editor_yscroll)
        xsb = ttk.Scrollbar(parent, orient='horizontal', command=self.sql_text.xview)
        self.sql_text.configure(yscrollcommand=lambda *args: self._on_editor_yview(ysb, *args), xscrollcommand=xsb.set)

        self.sql_text.pack(side='left', fill='both', expand=True)
        ysb.pack(side='right', fill='y')
        xsb.pack(fill='x')

        self.sql_text.tag_configure('active_stmt', background='#eaf3ff')
        self.sql_text.tag_configure('kw', foreground='#1d4ed8', font=(self._mono_font, 11, 'bold'))
        self.sql_text.tag_configure('str', foreground='#15803d')
        self.sql_text.tag_configure('num', foreground='#b45309')
        self.sql_text.tag_configure('comment', foreground='#64748b', font=(self._mono_font, 11, 'italic'))
        self.sql_text.tag_lower('active_stmt')

        self.sql_text.bind('<KeyRelease>', self._on_editor_key_release)
        self.sql_text.bind('<Control-space>', lambda _e: self._on_ctrl_space())
        self.sql_text.bind('<Control-Return>', lambda _e: self._on_ctrl_return())
        self.sql_text.bind('<Control-Shift-Return>', lambda _e: self._on_ctrl_shift_return())
        self.sql_text.bind('<Up>', lambda _e: self._on_popup_nav(-1))
        self.sql_text.bind('<Down>', lambda _e: self._on_popup_nav(1))
        self.sql_text.bind('<Return>', lambda _e: self._on_popup_accept())
        self.sql_text.bind('<Tab>', lambda _e: self._on_editor_tab())
        self.sql_text.bind('<Escape>', lambda _e: self._on_popup_escape())
        self.sql_text.bind('<ButtonRelease-1>', self._on_editor_click)
        self.sql_text.bind('<FocusIn>', lambda _e: self._update_active_statement_highlight())
        self.sql_text.bind('<FocusOut>', lambda _e: self.root.after(120, self._hide_completion_popup_if_unfocused))
        self.sql_text.bind('<MouseWheel>', lambda _e: self._hide_completion_popup())
        self.sql_text.bind('<Button-4>', lambda _e: self._hide_completion_popup())
        self.sql_text.bind('<Button-5>', lambda _e: self._hide_completion_popup())
        self.sql_text.bind('<Configure>', lambda _e: self._update_line_numbers())

        self._update_line_numbers()

    def _build_bottom_notebook(self, parent: ttk.Frame) -> None:
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill='both', expand=True)

        # Tab 1: 查询结果
        res_tab = ttk.Frame(self.notebook, padding=4)
        self.notebook.add(res_tab, text='查询结果')
        res_info_row = ttk.Frame(res_tab)
        res_info_row.pack(fill='x', pady=(0, 4))
        self.result_summary_var = tk.StringVar(value='尚未执行查询（表格中 <NULL> 表示 NULL，空白表示空字符串）')
        ttk.Label(res_info_row, textvariable=self.result_summary_var, foreground='#475569').pack(side='left')
        ttk.Label(res_info_row, text='提示：点击列头排序，双击单元格查看完整内容', foreground='#64748b').pack(side='right')

        res_tree_frame = ttk.Frame(res_tab)
        res_tree_frame.pack(fill='both', expand=True)
        self.result_tree = ttk.Treeview(res_tree_frame, show='headings', selectmode='browse')
        rvsb = ttk.Scrollbar(res_tree_frame, orient='vertical', command=self.result_tree.yview)
        rhsb = ttk.Scrollbar(res_tab, orient='horizontal', command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=rvsb.set, xscrollcommand=rhsb.set)
        self.result_tree.pack(side='left', fill='both', expand=True)
        rvsb.pack(side='right', fill='y')
        rhsb.pack(fill='x')

        self.result_tree.tag_configure('even', background='#ffffff')
        self.result_tree.tag_configure('odd', background='#f8fafc')
        self.result_tree.bind('<Double-1>', self._on_result_double_click)
        self.result_tree.bind('<Button-3>', self._on_result_right_click)

        self.result_menu = tk.Menu(self.root, tearoff=0)
        self.result_menu.add_command(label='复制选中行 (制表符分隔)', command=self._copy_selected_row)

        # Tab 2: 执行消息
        msg_tab = ttk.Frame(self.notebook, padding=4)
        self.notebook.add(msg_tab, text='执行消息')
        msg_top = ttk.Frame(msg_tab)
        msg_top.pack(fill='x', pady=(0, 4))
        ttk.Button(msg_top, text='清空消息', command=self._clear_messages).pack(side='right')

        msg_body = ttk.Frame(msg_tab)
        msg_body.pack(fill='both', expand=True)
        self.msg_text = tk.Text(
            msg_body, state='disabled', wrap='word',
            padx=6, pady=6, spacing1=2, spacing3=2,
            font=(self._mono_font, 10),
        )
        mvsb = ttk.Scrollbar(msg_body, orient='vertical', command=self.msg_text.yview)
        self.msg_text.configure(yscrollcommand=mvsb.set)
        self.msg_text.pack(side='left', fill='both', expand=True)
        mvsb.pack(side='right', fill='y')
        self.msg_text.tag_configure('ok', foreground='#15803d')
        self.msg_text.tag_configure('err', foreground='#dc2626')
        self.msg_text.tag_configure('info', foreground='#334155')

        # Tab 3: 历史记录
        hist_tab = ttk.Frame(self.notebook, padding=4)
        self.notebook.add(hist_tab, text='历史记录')
        hist_frame = ttk.Frame(hist_tab)
        hist_frame.pack(fill='both', expand=True)
        self.history_tree = ttk.Treeview(
            hist_frame,
            columns=('time', 'status', 'elapsed', 'sql'),
            show='headings',
            selectmode='browse',
        )
        self.history_tree.heading('time', text='时间')
        self.history_tree.heading('status', text='状态')
        self.history_tree.heading('elapsed', text='耗时 (ms)')
        self.history_tree.heading('sql', text='SQL 语句（双击回填）')
        self.history_tree.column('time', width=80, stretch=False, anchor='center')
        self.history_tree.column('status', width=70, stretch=False, anchor='center')
        self.history_tree.column('elapsed', width=90, stretch=False, anchor='e')
        self.history_tree.column('sql', width=620, stretch=True)
        hvsb = ttk.Scrollbar(hist_frame, orient='vertical', command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=hvsb.set)
        self.history_tree.pack(side='left', fill='both', expand=True)
        hvsb.pack(side='right', fill='y')
        self.history_tree.bind('<Double-1>', self._on_history_double_click)

        # Tab 4: 系统日志
        log_tab = ttk.Frame(self.notebook, padding=4)
        self.notebook.add(log_tab, text='系统日志')
        log_top = ttk.Frame(log_tab)
        log_top.pack(fill='x', pady=(0, 4))
        ttk.Label(log_top, text='日志级别：').pack(side='left')
        self.log_level_var = tk.StringVar(value='INFO')
        level_box = ttk.Combobox(
            log_top, textvariable=self.log_level_var, values=('DEBUG', 'INFO', 'WARNING'), state='readonly', width=10)
        level_box.pack(side='left', padx=(2, 8))
        level_box.bind('<<ComboboxSelected>>', self._on_log_level_change)
        ttk.Button(log_top, text='清空日志', command=self._clear_logs).pack(side='right')

        log_body = ttk.Frame(log_tab)
        log_body.pack(fill='both', expand=True)
        self.log_text = tk.Text(
            log_body, state='disabled', wrap='word',
            padx=6, pady=6, spacing1=2, spacing3=2,
            font=(self._mono_font, 9),
        )
        lvsb = ttk.Scrollbar(log_body, orient='vertical', command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=lvsb.set)
        self.log_text.pack(side='left', fill='both', expand=True)
        lvsb.pack(side='right', fill='y')
        self.log_text.tag_configure('ERROR', foreground='#dc2626')
        self.log_text.tag_configure('WARNING', foreground='#b45309')
        self.log_text.tag_configure('INFO', foreground='#1e293b')
        self.log_text.tag_configure('DEBUG', foreground='#64748b')

    def _build_statusbar(self) -> None:
        ttk.Separator(self.root, orient='horizontal').pack(fill='x')
        bar = ttk.Frame(self.root, padding=(8, 3))
        bar.pack(fill='x')
        self.status_left_var = tk.StringVar(value='就绪')
        self.status_mid_var = tk.StringVar(value='')
        self.status_right_var = tk.StringVar(value='行 1, 列 1')
        ttk.Label(bar, textvariable=self.status_left_var).pack(side='left')
        ttk.Label(bar, textvariable=self.status_right_var).pack(side='right')
        ttk.Label(bar, textvariable=self.status_mid_var, foreground='#475569').pack(side='right', padx=(0, 24))

    def _bind_shortcuts(self) -> None:
        self.root.bind('<F5>', lambda _e: self._run_all())
        self.root.bind('<Control-Return>', lambda _e: self._on_ctrl_return())
        self.root.bind('<Control-Shift-Return>', lambda _e: self._on_ctrl_shift_return())
        self.root.bind('<Control-o>', lambda _e: self._choose_directory())
        self.root.bind('<Control-s>', lambda _e: self._save_sql_file())
        self.root.bind('<Control-l>', lambda _e: self._clear_editor())
        self.root.bind('<Control-q>', lambda _e: self._on_close())
        self.root.bind('<Configure>', self._on_root_configure)

    def _on_root_configure(self, event: tk.Event) -> None:
        if event.widget is self.root:
            self._hide_completion_popup()

    # ------------------------------------------------------------------
    # Logging integration
    # ------------------------------------------------------------------

    def _attach_logger(self) -> None:
        pkg_logger = logging.getLogger('txtsql')
        pkg_logger.setLevel(logging.INFO)
        self._log_handler = _TkLogHandler(self._append_log_line)
        pkg_logger.addHandler(self._log_handler)

    def _on_log_level_change(self, _event=None) -> None:
        level_name = self.log_level_var.get()
        logging.getLogger('txtsql').setLevel(getattr(logging, level_name, logging.INFO))

    def _append_log_line(self, message: str, level: str) -> None:
        if not self.root.winfo_exists():
            return
        self.log_text.configure(state='normal')
        tag = level if level in ('ERROR', 'WARNING', 'INFO', 'DEBUG') else 'INFO'
        self.log_text.insert('end', message + '\n', tag)
        self.log_text.see('end')
        self.log_text.configure(state='disabled')

    def _clear_logs(self) -> None:
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', 'end')
        self.log_text.configure(state='disabled')

    # ------------------------------------------------------------------
    # Metadata sidebar actions
    # ------------------------------------------------------------------

    def refresh_metadata(self) -> None:
        for item in self.meta_tree.get_children():
            self.meta_tree.delete(item)
        metas = self.controller.get_metadata(self.meta_filter_var.get())
        for meta in metas:
            table_iid = f'table:{meta.name}'
            self.meta_tree.insert('', 'end', iid=table_iid, text=f'{meta.name} ({meta.row_count} 行)', open=False)
            for col_name, col_type in meta.columns:
                self.meta_tree.insert(table_iid, 'end', iid=f'col:{meta.name}.{col_name}', text=f'{col_name} : {col_type}')
        total_tables = len(self.controller.db.list_tables())
        self.status_left_var.set(f'目录: {self.controller.data_dir}  |  共 {total_tables} 张表')

    def _selected_table_name(self) -> str | None:
        sel = self.meta_tree.selection()
        if not sel:
            return None
        iid = sel[0]
        if iid.startswith('table:'):
            return iid.removeprefix('table:')
        if iid.startswith('col:'):
            return iid.removeprefix('col:').split('.', 1)[0]
        return None

    def _on_meta_double_click(self, _event=None) -> None:
        table = self._selected_table_name()
        if table:
            self._preview_table(table)

    def _on_meta_right_click(self, event: tk.Event) -> None:
        iid = self.meta_tree.identify_row(event.y)
        if iid:
            self.meta_tree.selection_set(iid)
            self.meta_menu.tk_popup(event.x_root, event.y_root)

    def _preview_selected_table(self) -> None:
        table = self._selected_table_name()
        if table:
            self._preview_table(table)

    def _preview_table(self, table_name: str) -> None:
        sql = f'SELECT * FROM {table_name} LIMIT 100;'
        self._set_editor_text(sql)
        self._execute_text(sql)

    def _describe_selected_table(self) -> None:
        table = self._selected_table_name()
        if table:
            sql = f'DESCRIBE {table};'
            self._set_editor_text(sql)
            self._execute_text(sql)

    def _insert_selected_table_name(self) -> None:
        table = self._selected_table_name()
        if table:
            self.sql_text.insert('insert', table)
            self.sql_text.focus_set()
            self._on_editor_change()

    def _drop_selected_table(self) -> None:
        table = self._selected_table_name()
        if not table:
            return
        if not messagebox.askyesno('确认删除表', f'确定要永久删除表 "{table}" 及其全部数据吗？', parent=self.root):
            return
        self._execute_text(f'DROP TABLE {table};')

    def _open_create_table_dialog(self) -> None:
        CreateTableDialog(self.root, on_submit=self._execute_generated_ddl)

    def _execute_generated_ddl(self, sql: str) -> None:
        self._set_editor_text(sql)
        self._execute_text(sql)

    # ------------------------------------------------------------------
    # Editor helpers (line numbers, syntax highlight, active stmt & completion)
    # ------------------------------------------------------------------

    _IGNORE_COMPLETE_KEYSYMS = frozenset({
        'Up', 'Down', 'Left', 'Right', 'Home', 'End', 'Prior', 'Next',
        'Shift_L', 'Shift_R', 'Control_L', 'Control_R', 'Alt_L', 'Alt_R',
        'Meta_L', 'Meta_R', 'Super_L', 'Super_R', 'Caps_Lock', 'Num_Lock',
        'Escape', 'Tab', 'Return', 'KP_Enter', 'F1', 'F2', 'F3', 'F4', 'F5',
        'F6', 'F7', 'F8', 'F9', 'F10', 'F11', 'F12',
    })

    def _set_editor_text(self, text: str) -> None:
        self._hide_completion_popup()
        self.sql_text.delete('1.0', 'end')
        self.sql_text.insert('1.0', text)
        self._update_line_numbers()
        self._apply_syntax_highlight()
        self._update_cursor_pos()

    def _clear_editor(self) -> None:
        self._hide_completion_popup()
        self.sql_text.delete('1.0', 'end')
        self._update_line_numbers()
        self._update_cursor_pos()

    def _insert_tab_spaces(self, _event=None) -> str:
        self.sql_text.insert('insert', '    ')
        self._on_editor_change()
        return 'break'

    def _sync_editor_yscroll(self, *args) -> None:
        self._hide_completion_popup()
        self.sql_text.yview(*args)
        self.line_numbers.yview(*args)

    def _on_editor_yview(self, scrollbar: ttk.Scrollbar, *args) -> None:
        scrollbar.set(*args)
        self.line_numbers.yview_moveto(args[0])

    def _on_editor_change(self, _event=None) -> None:
        self._update_line_numbers()
        self._update_cursor_pos()
        if self._highlight_job is not None:
            self.root.after_cancel(self._highlight_job)
        self._highlight_job = self.root.after(80, self._apply_syntax_highlight)

    def _on_editor_key_release(self, event: tk.Event) -> None:
        if self._suppress_space_release and event.keysym == 'space':
            self._suppress_space_release = False
            self._update_cursor_pos()
            return
        self._suppress_space_release = False
        self._on_editor_change()
        if event.keysym in ('Left', 'Right', 'Home', 'End', 'Prior', 'Next'):
            self._hide_completion_popup()
            return
        if event.keysym in self._IGNORE_COMPLETE_KEYSYMS:
            return
        if event.state & 0x4:
            self._hide_completion_popup()
            return
        if self._complete_job is not None:
            self.root.after_cancel(self._complete_job)
        self._complete_job = self.root.after(60, lambda: self._trigger_completions(manual=False))

    def _on_editor_click(self, _event=None) -> None:
        self._hide_completion_popup()
        self._update_cursor_pos()

    def _on_ctrl_space(self) -> str:
        self._suppress_space_release = True
        if self._complete_job is not None:
            self.root.after_cancel(self._complete_job)
            self._complete_job = None
        self._trigger_completions(manual=True)
        return 'break'

    def _on_ctrl_return(self) -> str:
        self._hide_completion_popup()
        self._run_current_statement()
        return 'break'

    def _on_ctrl_shift_return(self) -> str:
        self._hide_completion_popup()
        self._run_selected()
        return 'break'

    def _on_popup_nav(self, delta: int) -> str | None:
        if not self._is_completion_visible():
            return None
        self._move_completion(delta)
        return 'break'

    def _on_popup_accept(self) -> str | None:
        if not self._is_completion_visible():
            return None
        self._accept_completion()
        return 'break'

    def _on_editor_tab(self) -> str:
        if self._is_completion_visible():
            self._accept_completion()
            return 'break'
        return self._insert_tab_spaces()

    def _on_popup_escape(self) -> str | None:
        if not self._is_completion_visible():
            return None
        self._hide_completion_popup()
        return 'break'

    def _update_line_numbers(self) -> None:
        lines = int(self.sql_text.index('end-1c').split('.')[0])
        content = '\n'.join(str(i) for i in range(1, lines + 1))
        self.line_numbers.configure(state='normal')
        self.line_numbers.delete('1.0', 'end')
        self.line_numbers.insert('1.0', content)
        self.line_numbers.configure(state='disabled')
        self._update_active_statement_highlight()

    def _update_cursor_pos(self) -> None:
        pos = self.sql_text.index('insert')
        line, col = pos.split('.')
        self.status_right_var.set(f'行 {line}, 列 {int(col) + 1}')
        self._update_active_statement_highlight()

    def _get_active_statement(self) -> StatementSpan | None:
        text = self.sql_text.get('1.0', 'end-1c')
        cursor_offset = len(self.sql_text.get('1.0', 'insert'))
        return find_active_statement(text, cursor_offset)

    def _update_active_statement_highlight(self) -> None:
        self.sql_text.tag_remove('active_stmt', '1.0', 'end')
        self.line_numbers.tag_remove('active_line', '1.0', 'end')

        text = self.sql_text.get('1.0', 'end-1c')
        cursor_offset = len(self.sql_text.get('1.0', 'insert'))
        span = find_active_statement(text, cursor_offset)
        if span is None:
            return

        start_line = text.count('\n', 0, span.start) + 1
        end_line = text.count('\n', 0, max(span.start, span.end - 1)) + 1
        start_idx = f'{start_line}.0'
        end_idx = f'{end_line + 1}.0'
        self.sql_text.tag_add('active_stmt', start_idx, end_idx)
        self.sql_text.tag_lower('active_stmt')
        self.line_numbers.tag_add('active_line', start_idx, end_idx)

    def _is_completion_visible(self) -> bool:
        return (
            self._completion_popup is not None
            and self._completion_popup.winfo_exists()
            and self._completion_popup.winfo_viewable() == 1
        )

    def _trigger_completions(self, manual: bool = False) -> None:
        self._complete_job = None
        if not self.sql_text.winfo_exists():
            return
        script = self.sql_text.get('1.0', 'end-1c')
        cursor_offset = len(self.sql_text.get('1.0', 'insert'))
        result = self.controller.get_completions(script, cursor_offset, manual=manual)
        if result is None or not result.items:
            self._hide_completion_popup()
            return
        self._show_completion_popup(result)

    def _format_completion_item(self, item: CompletionItem) -> str:
        kind_tag = {
            'column': '[列]',
            'table': '[表]',
            'function': '[函数]',
            'keyword': '[词]',
        }.get(item.kind, '[ ]')
        if item.detail:
            return f' {kind_tag:<4} {item.label:<20} {item.detail}'
        return f' {kind_tag:<4} {item.label}'

    def _show_completion_popup(self, result: CompletionResult) -> None:
        bbox = self.sql_text.bbox('insert')
        if bbox is None:
            self.sql_text.see('insert')
            bbox = self.sql_text.bbox('insert')
            if bbox is None:
                self._hide_completion_popup()
                return

        if self._completion_popup is None or not self._completion_popup.winfo_exists():
            popup = tk.Toplevel(self.root)
            popup.overrideredirect(True)
            popup.configure(background='#94a3b8')
            frame = tk.Frame(popup, background='#ffffff', borderwidth=0)
            frame.pack(fill='both', expand=True, padx=1, pady=1)
            lb = tk.Listbox(
                frame,
                activestyle='none',
                background='#ffffff',
                foreground='#0f172a',
                selectbackground='#dbeafe',
                selectforeground='#1e3a8a',
                borderwidth=0,
                highlightthickness=0,
                exportselection=False,
                takefocus=0,
                font=(self._mono_font, 10),
            )
            lb.pack(fill='both', expand=True)
            lb.bind('<Button-1>', self._on_listbox_click)
            lb.bind('<ButtonRelease-1>', self._on_listbox_release)
            self._completion_popup = popup
            self._completion_listbox = lb

        assert self._completion_popup is not None and self._completion_listbox is not None
        self._completion_result = result

        lb = self._completion_listbox
        lb.delete(0, 'end')
        max_len = 28
        for item in result.items:
            line = self._format_completion_item(item)
            max_len = max(max_len, len(line) + 2)
            lb.insert('end', line)

        visible_rows = min(len(result.items), 10)
        lb.configure(height=visible_rows, width=min(max_len, 62))
        lb.selection_clear(0, 'end')
        lb.selection_set(0)
        lb.activate(0)

        self._completion_popup.update_idletasks()
        popup_w = self._completion_popup.winfo_reqwidth()
        popup_h = self._completion_popup.winfo_reqheight()

        rx = self.sql_text.winfo_rootx() + bbox[0]
        ry_below = self.sql_text.winfo_rooty() + bbox[1] + bbox[3] + 2
        ry_above = self.sql_text.winfo_rooty() + bbox[1] - popup_h - 2
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()

        if rx + popup_w > screen_w - 8:
            rx = max(4, screen_w - popup_w - 8)
        if ry_below + popup_h > screen_h - 24 and ry_above >= 4:
            ry = ry_above
        else:
            ry = ry_below

        self._completion_popup.geometry(f'+{rx}+{ry}')
        self._completion_popup.deiconify()
        self._completion_popup.lift()

    def _hide_completion_popup(self) -> None:
        if self._complete_job is not None:
            self.root.after_cancel(self._complete_job)
            self._complete_job = None
        self._completion_result = None
        if self._completion_popup is not None and self._completion_popup.winfo_exists():
            self._completion_popup.withdraw()

    def _hide_completion_popup_if_unfocused(self) -> None:
        if not self.root.winfo_exists():
            return
        try:
            focus = self.root.focus_get()
        except (KeyError, tk.TclError):
            focus = None
        if focus in (self.sql_text, self._completion_popup, self._completion_listbox):
            return
        self._hide_completion_popup()

    def _move_completion(self, delta: int) -> None:
        if self._completion_listbox is None:
            return
        total = self._completion_listbox.size()
        if total == 0:
            return
        sel = self._completion_listbox.curselection()
        idx = sel[0] if sel else 0
        nxt = max(0, min(total - 1, idx + delta))
        self._completion_listbox.selection_clear(0, 'end')
        self._completion_listbox.selection_set(nxt)
        self._completion_listbox.activate(nxt)
        self._completion_listbox.see(nxt)

    def _on_listbox_click(self, event: tk.Event) -> str:
        if self._completion_listbox is not None:
            idx = self._completion_listbox.nearest(event.y)
            self._completion_listbox.selection_clear(0, 'end')
            self._completion_listbox.selection_set(idx)
            self._completion_listbox.activate(idx)
        return 'break'

    def _on_listbox_release(self, event: tk.Event) -> str:
        if self._completion_listbox is not None:
            idx = self._completion_listbox.nearest(event.y)
            self._completion_listbox.selection_clear(0, 'end')
            self._completion_listbox.selection_set(idx)
            self._completion_listbox.activate(idx)
        self._accept_completion()
        return 'break'

    def _accept_completion(self) -> None:
        result = self._completion_result
        lb = self._completion_listbox
        if result is None or lb is None or not result.items:
            self._hide_completion_popup()
            return
        sel = lb.curselection()
        idx = sel[0] if sel else 0
        if idx < 0 or idx >= len(result.items):
            idx = 0
        item = result.items[idx]
        start_idx = f'1.0+{result.replace_start}c'
        end_idx = f'1.0+{result.replace_end}c'
        self._hide_completion_popup()
        self.sql_text.delete(start_idx, end_idx)
        self.sql_text.insert(start_idx, item.insert_text)
        self.sql_text.mark_set('insert', f'1.0+{result.replace_start + len(item.insert_text)}c')
        self.sql_text.focus_set()
        self._on_editor_change()

    def _apply_syntax_highlight(self) -> None:
        self._highlight_job = None
        text = self.sql_text.get('1.0', 'end-1c')
        for tag in ('kw', 'str', 'num', 'comment'):
            self.sql_text.tag_remove(tag, '1.0', 'end')

        for m in _KW_PATTERN.finditer(text):
            self.sql_text.tag_add('kw', f'1.0+{m.start()}c', f'1.0+{m.end()}c')
        for m in _NUM_PATTERN.finditer(text):
            self.sql_text.tag_add('num', f'1.0+{m.start()}c', f'1.0+{m.end()}c')
        for m in _STR_PATTERN.finditer(text):
            start, end = f'1.0+{m.start()}c', f'1.0+{m.end()}c'
            self.sql_text.tag_remove('kw', start, end)
            self.sql_text.tag_remove('num', start, end)
            self.sql_text.tag_add('str', start, end)
        for m in _COMMENT_PATTERN.finditer(text):
            start, end = f'1.0+{m.start()}c', f'1.0+{m.end()}c'
            for t in ('kw', 'num', 'str'):
                self.sql_text.tag_remove(t, start, end)
            self.sql_text.tag_add('comment', start, end)

    # ------------------------------------------------------------------
    # SQL execution & Result rendering
    # ------------------------------------------------------------------

    def _run_current_statement(self) -> None:
        span = self._get_active_statement()
        if span is None:
            self._append_message('当前光标所在位置没有可执行的 SQL 语句（可按 F5 运行全部）。', 'info')
            self.notebook.select(1)
            return
        self._execute_text(span.sql)

    def _run_all(self) -> None:
        script = self.sql_text.get('1.0', 'end-1c')
        self._execute_text(script)

    def _run_selected(self) -> None:
        try:
            script = self.sql_text.get('sel.first', 'sel.last')
        except tk.TclError:
            messagebox.showinfo('提示', '请先在编辑器中选中要执行的 SQL 语句。', parent=self.root)
            return
        self._execute_text(script)

    def _execute_text(self, script: str) -> None:
        self._hide_completion_popup()
        report = self.controller.execute_script(script)
        if not report.entries:
            self._append_message('未检测到可执行 SQL 语句。', 'info')
            return

        for entry in report.entries:
            self._record_entry_ui(entry)

        if report.schema_changed:
            self.refresh_metadata()

        if report.last_tabular is not None:
            self._sort_state = None
            self._render_result_table(report.last_tabular, preserve_widths=False)
            self.notebook.select(0)
        elif report.error_count > 0:
            self.notebook.select(1)

        last_entry = report.entries[-1]
        if last_entry.ok and last_entry.result is not None:
            self.status_mid_var.set(self._format_entry_summary(last_entry))
        else:
            self.status_mid_var.set(f'执行失败 ({last_entry.elapsed_ms:.2f} ms)')

    def _format_entry_summary(self, entry: ExecutionEntry) -> str:
        res = entry.result
        assert res is not None
        if res.is_tabular:
            return f'{res.kind.value}: 返回 {len(res.rows)} 行 ({entry.elapsed_ms:.2f} ms)'
        if res.affected_rows is not None:
            return f'{res.kind.value}: 影响 {res.affected_rows} 行 ({entry.elapsed_ms:.2f} ms)'
        return f'{res.kind.value}: {res.message} ({entry.elapsed_ms:.2f} ms)'

    def _record_entry_ui(self, entry: ExecutionEntry) -> None:
        single_line_sql = ' '.join(entry.sql.split())
        if len(single_line_sql) > 120:
            single_line_sql = single_line_sql[:117] + '...'

        if entry.ok:
            summary = self._format_entry_summary(entry)
            self._append_message(f'[{entry.timestamp}] {summary} | {single_line_sql}', 'ok')
            status_text = '成功'
        else:
            self._append_message(
                f'[{entry.timestamp}] 错误 ({entry.elapsed_ms:.2f} ms): {entry.error} | {single_line_sql}',
                'err',
            )
            status_text = '失败'

        self.history_tree.insert(
            '',
            0,
            values=(entry.timestamp, status_text, f'{entry.elapsed_ms:.2f}', entry.sql),
        )
        recent = [' '.join(h.sql.split())[:80] for h in reversed(self.controller.history[-20:])]
        self.history_combo['values'] = recent

    def _compute_column_width(self, col: str, rows: list) -> int:
        """Compute pixel width fitting both header (including sort marker) and cell contents."""
        header_px = self._heading_font.measure(f'{col} ▼') + 28
        content_px = 0
        for row in rows[:500]:
            text = format_cell_single_line(row.get(col))
            w = self._body_font.measure(text) + 28
            if w > content_px:
                content_px = w
        return min(max(header_px, content_px, 90), 600)

    def _render_result_table(
        self,
        result: QueryResult,
        sorted_rows: list | None = None,
        preserve_widths: bool = False,
    ) -> None:
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        cols = result.columns
        self._current_columns = cols
        rows = sorted_rows if sorted_rows is not None else result.rows
        self._displayed_rows = rows

        columns_changed = tuple(self.result_tree['columns']) != cols
        if columns_changed:
            self.result_tree['columns'] = cols

        for col in cols:
            marker = ''
            if self._sort_state and self._sort_state[0] == col:
                marker = ' ▼' if self._sort_state[1] else ' ▲'
            self.result_tree.heading(col, text=f'{col}{marker}', command=lambda c=col: self._sort_by_column(c))

            if not preserve_widths or columns_changed:
                col_px = self._compute_column_width(col, rows)
                self.result_tree.column(col, width=col_px, minwidth=70, stretch=False)

        for idx, row in enumerate(rows):
            vals = [format_cell_single_line(row.get(c)) for c in cols]
            tag = 'even' if idx % 2 == 0 else 'odd'
            self.result_tree.insert('', 'end', iid=str(idx), values=vals, tags=(tag,))

        self.result_summary_var.set(f'共 {len(rows)} 行，{len(cols)} 列（<NULL> 表示 NULL，空白表示空字符串）')

    def _sort_by_column(self, col: str) -> None:
        res = self.controller.last_tabular_result
        if res is None or not res.rows:
            return
        desc = False
        if self._sort_state and self._sort_state[0] == col:
            desc = not self._sort_state[1]
        self._sort_state = (col, desc)
        sorted_rows = sort_rows_for_view(res.rows, col, descending=desc)
        self._render_result_table(res, sorted_rows=sorted_rows, preserve_widths=True)

    def _on_result_double_click(self, event: tk.Event) -> None:
        item_iid = self.result_tree.identify_row(event.y)
        col_id = self.result_tree.identify_column(event.x)
        if not item_iid or not col_id or not self._current_columns:
            return
        col_idx = int(col_id.removeprefix('#')) - 1
        if not (0 <= col_idx < len(self._current_columns)):
            return
        col_name = self._current_columns[col_idx]
        row_idx = int(item_iid)
        if 0 <= row_idx < len(self._displayed_rows):
            raw_val = self._displayed_rows[row_idx].get(col_name)
            text = format_cell_display(raw_val)
        else:
            values = self.result_tree.item(item_iid, 'values')
            text = str(values[col_idx]) if col_idx < len(values) else ''
        self._show_cell_detail_dialog(col_name, text)

    def _show_cell_detail_dialog(self, col_name: str, text: str) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title(f'单元格详情 - {col_name}')
        dlg.transient(self.root)
        frame = ttk.Frame(dlg, padding=14)
        frame.pack(fill='both', expand=True)

        body = ttk.Frame(frame)
        body.pack(fill='both', expand=True, pady=(0, 12))
        txt = tk.Text(
            body, wrap='word', padx=8, pady=6,
            spacing1=3, spacing3=3, font=(self._mono_font, 11),
        )
        vsb = ttk.Scrollbar(body, orient='vertical', command=txt.yview)
        txt.configure(yscrollcommand=vsb.set)
        txt.insert('1.0', text)
        txt.configure(state='disabled')
        txt.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')

        btns = ttk.Frame(frame)
        btns.pack(fill='x')
        ttk.Button(btns, text='复制内容', style='Accent.TButton', command=lambda: self._copy_to_clipboard(text)).pack(side='left')
        ttk.Button(btns, text='关闭', command=dlg.destroy).pack(side='right')
        _configure_dialog(dlg, self.root, base_w=680, base_h=440, min_w=520, min_h=340)

    def _on_result_right_click(self, event: tk.Event) -> None:
        iid = self.result_tree.identify_row(event.y)
        if iid:
            self.result_tree.selection_set(iid)
            self.result_menu.tk_popup(event.x_root, event.y_root)

    def _copy_selected_row(self) -> None:
        sel = self.result_tree.selection()
        if not sel:
            return
        vals = self.result_tree.item(sel[0], 'values')
        self._copy_to_clipboard('\t'.join(str(v) for v in vals))

    def _copy_to_clipboard(self, text: str) -> None:
        self.root.clipboard_clear()
        self.root.clipboard_append(text)

    # ------------------------------------------------------------------
    # Messages & History callbacks
    # ------------------------------------------------------------------

    def _append_message(self, text: str, tag: str = 'info') -> None:
        self.msg_text.configure(state='normal')
        self.msg_text.insert('end', text + '\n', tag)
        self.msg_text.see('end')
        self.msg_text.configure(state='disabled')

    def _clear_messages(self) -> None:
        self.msg_text.configure(state='normal')
        self.msg_text.delete('1.0', 'end')
        self.msg_text.configure(state='disabled')

    def _on_history_double_click(self, _event=None) -> None:
        sel = self.history_tree.selection()
        if not sel:
            return
        vals = self.history_tree.item(sel[0], 'values')
        if len(vals) >= 4:
            self._set_editor_text(vals[3])

    def _on_history_combo_select(self, _event=None) -> None:
        idx = self.history_combo.current()
        recent = list(reversed(self.controller.history[-20:]))
        if 0 <= idx < len(recent):
            self._set_editor_text(recent[idx].sql)

    # ------------------------------------------------------------------
    # File, Directory, Import/Export & Help dialogs
    # ------------------------------------------------------------------

    def _choose_directory(self) -> None:
        self._hide_completion_popup()
        selected = filedialog.askdirectory(initialdir=str(self.controller.data_dir), title='选择数据目录')
        if not selected:
            return
        new_dir = self.controller.switch_directory(selected)
        self.dir_var.set(str(new_dir))
        self._update_title()
        self._sort_state = None
        self.refresh_metadata()
        self._append_message(f'已切换数据目录至: {new_dir}', 'info')

    def _open_sql_file(self) -> None:
        path = filedialog.askopenfilename(
            title='打开 SQL 脚本',
            filetypes=[('SQL 文件', '*.sql'), ('文本文件', '*.txt'), ('所有文件', '*.*')],
        )
        if not path:
            return
        content = Path(path).read_text(encoding='utf-8')
        self._set_editor_text(content)

    def _save_sql_file(self) -> None:
        path = filedialog.asksaveasfilename(
            title='保存 SQL 脚本',
            defaultextension='.sql',
            filetypes=[('SQL 文件', '*.sql'), ('文本文件', '*.txt')],
        )
        if not path:
            return
        Path(path).write_text(self.sql_text.get('1.0', 'end-1c'), encoding='utf-8')
        self._append_message(f'SQL 脚本已保存至: {path}', 'info')

    def _import_excel(self) -> None:
        file_path = filedialog.askopenfilename(
            title='选择要导入的 Excel 文件',
            filetypes=[('Excel 工作簿', '*.xlsx')],
        )
        if not file_path:
            return
        default_name = re.sub(r'[^A-Za-z0-9_]', '_', Path(file_path).stem) or 'imported_table'
        if not default_name[0].isalpha() and default_name[0] != '_':
            default_name = f't_{default_name}'
        dlg = ImportTableDialog(self.root, file_path, default_name)
        self.root.wait_window(dlg)
        table_name = dlg.result
        if not table_name:
            return
        try:
            entry = self.controller.import_excel(table_name, file_path)
        except (ValueError, TxtSqlError) as exc:
            messagebox.showerror('导入失败', str(exc), parent=self.root)
            return
        self._record_entry_ui(entry)
        if entry.ok:
            self.refresh_metadata()
            self._preview_table(table_name.strip())
        else:
            messagebox.showerror('导入失败', entry.error or '未知错误', parent=self.root)

    def _export_result(self) -> None:
        if self.controller.last_tabular_result is None:
            messagebox.showinfo('提示', '请先执行一条查询语句后再导出结果。', parent=self.root)
            return
        file_path = filedialog.asksaveasfilename(
            title='导出查询结果',
            defaultextension='.xlsx',
            filetypes=[('Excel 工作簿', '*.xlsx'), ('CSV 文件', '*.csv'), ('TSV 文件', '*.tsv')],
        )
        if not file_path:
            return
        try:
            count = self.controller.export_last_result(file_path)
        except Exception as exc:
            messagebox.showerror('导出失败', str(exc), parent=self.root)
            return
        self._append_message(f'成功导出 {count} 行至: {file_path}', 'ok')
        messagebox.showinfo('导出成功', f'已成功导出 {count} 行记录到:\n{file_path}', parent=self.root)

    def _load_template(self, title: str, sql: str) -> None:
        self._set_editor_text(sql)
        self._append_message(f'已加载示例模板：{title}（Ctrl+Enter 运行当前 / F5 运行全部）', 'info')

    def _show_help(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title('快捷键与 SQL 速查')
        dlg.transient(self.root)
        outer = ttk.Frame(dlg, padding=16)
        outer.pack(fill='both', expand=True)

        body = ttk.Frame(outer)
        body.pack(fill='both', expand=True, pady=(0, 12))
        txt = tk.Text(
            body, wrap='word', padx=10, pady=8,
            spacing1=3, spacing3=3, font=(self._mono_font, 10),
        )
        vsb = ttk.Scrollbar(body, orient='vertical', command=txt.yview)
        txt.configure(yscrollcommand=vsb.set)
        txt.insert(
            '1.0',
            '【快捷键】\n'
            '  Ctrl+Enter             运行光标所在当前高亮 SQL 语句\n'
            '  F5                     运行编辑器全部 SQL 语句\n'
            '  Ctrl+Shift+Enter       仅运行当前选中的 SQL 语句\n'
            '  Ctrl+Space             手动弹出 SQL 代码补全列表\n'
            '  Tab / Enter / ↑↓ / Esc 补全弹窗确认、上下选择或关闭\n'
            '  Ctrl+O                 切换数据目录\n'
            '  Ctrl+S                 保存 SQL 脚本到文件\n'
            '  Ctrl+L                 清空编辑器内容\n'
            '  Ctrl+Q                 退出客户端\n\n'
            '【支持的 SQL 语法】\n'
            '  • DDL / DML:\n'
            '      CREATE TABLE [IF NOT EXISTS] tbl (col1 NUMBER, col2 STRING)\n'
            '      DROP TABLE [IF EXISTS] tbl\n'
            '      INSERT INTO tbl [(cols)] VALUES (...), (...) | SELECT ...\n'
            '      UPDATE tbl SET col = expr [WHERE ...]\n'
            '      DELETE FROM tbl [WHERE ...]\n'
            '  • 单表与聚合查询:\n'
            '      SELECT [DISTINCT] *, tbl.*, expr [AS alias]\n'
            '      WHERE (SQL 三值逻辑: AND / OR / NOT / IS [NOT] NULL / IN / BETWEEN / LIKE)\n'
            '      GROUP BY / HAVING / COUNT(*) / COUNT(DISTINCT col) / SUM / AVG / MIN / MAX\n'
            '      ORDER BY col [ASC|DESC] / LIMIT n / OFFSET n\n'
            '  • 多表连接、子查询与集合运算:\n'
            '      INNER / LEFT / RIGHT / FULL [OUTER] / CROSS / NATURAL / USING JOIN\n'
            '      IN / NOT IN / ANY / ALL / EXISTS / NOT EXISTS / 标量子查询 (含关联)\n'
            '      SELECT ... UNION [ALL] SELECT ...\n'
            '  • 系统与导入导出:\n'
            "      SHOW TABLES;  DESCRIBE tbl;  IMPORT tbl FROM 'file.xlsx';  INTO OUTFILE '...'",
        )
        txt.configure(state='disabled')
        txt.pack(side='left', fill='both', expand=True)
        vsb.pack(side='right', fill='y')

        btns = ttk.Frame(outer)
        btns.pack(fill='x')
        ttk.Button(btns, text='关闭', style='Accent.TButton', command=dlg.destroy).pack(side='right')
        _configure_dialog(dlg, self.root, base_w=780, base_h=520, min_w=620, min_h=400)

    def _show_about(self) -> None:
        dlg = tk.Toplevel(self.root)
        dlg.title('关于 TxtSql')
        dlg.transient(self.root)
        outer = ttk.Frame(dlg, padding=18)
        outer.pack(fill='both', expand=True)

        ttk.Label(
            outer,
            text='TxtSql — 基于纯文本 TSV 文件的关系型数据库系统',
            font=('TkDefaultFont', 12, 'bold'),
        ).pack(anchor='w', pady=(0, 12))
        ttk.Label(
            outer,
            text=(
                '• 自主实现词法分析器、递归下降语法分析器与 SQL 三值逻辑执行引擎\n\n'
                '• 支持单表/多表连接、聚合分组、子查询、集合运算与 Excel/CSV/TSV 导入导出\n\n'
                '• 提供命令行 (CLI) 与 Tkinter 图形用户界面 (GUI) 双交互模式'
            ),
            justify='left',
        ).pack(anchor='w', fill='x', expand=True)

        btns = ttk.Frame(outer)
        btns.pack(fill='x', pady=(14, 0))
        ttk.Button(btns, text='确定', style='Accent.TButton', command=dlg.destroy).pack(side='right')
        _configure_dialog(dlg, self.root, base_w=600, base_h=280, min_w=480, min_h=240)

    def _on_close(self) -> None:
        if self._log_handler is not None:
            logging.getLogger('txtsql').removeHandler(self._log_handler)
            self._log_handler = None
        self.root.destroy()


def run_gui(data_dir: str | os.PathLike[str] = '.') -> None:
    """Launch the TxtSql Tkinter desktop GUI."""
    root = tk.Tk()
    TxtSqlApp(root, data_dir=data_dir)
    root.mainloop()
