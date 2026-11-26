"""Statement executors: interpret AST and drive a StorageEngine instance."""

from dataclasses import dataclass
from decimal import Decimal

from .ast import (
    DropTable, CreateTable, CreateIndex, DropIndex, InsertValues, InsertSelect,
    DeleteStatement, SelectStatement, UnionStatement, UpdateStatement, ImportStatement,
    ShowTables, ShowIndexes, DescribeTable,
    AggFunc, AggregateExpression, ColumnExpression, LiteralExpression, StarExpression,
    JoinClause, JoinType, SelectExpression,
    contains_aggregate,
)
from .evaluator import (
    EvalContext, evaluate_where, evaluate_expression, resolve_column_name,
    resolve_column, to_bool, to_decimal,
)
from .exceptions import ColumnNotFoundError, TableAlreadyExistsError, TableNotFoundError, EngineError, TypeMismatchError
from .index import match_single_table_index
from .io_handlers import export_rows, read_excel_table
from .result import QueryResult, StatementKind
from .storage import StorageEngine, Table
from .types import Types, RowDict, DataValue


_TYPE_MAP = {'STRING': Types.STRING, 'NUMBER': Types.NUMBER}

Statement = (
    CreateTable | DropTable | CreateIndex | DropIndex |
    InsertValues | InsertSelect | DeleteStatement |
    SelectStatement | UnionStatement | UpdateStatement |
    ImportStatement | ShowTables | ShowIndexes | DescribeTable
)


@dataclass(frozen=True)
class _VisibleColumn:
    """A column visible in the current joined relation."""
    qualifier: str
    name: str
    key: str
    coalesce_keys: tuple[str, ...] = ()


class QueryExecutor:
    """Executes parsed SQL AST statements against a directory-scoped StorageEngine."""

    def __init__(self, storage_engine: StorageEngine, enable_optimizations: bool = True) -> None:
        self.storage = storage_engine
        self.enable_optimizations = enable_optimizations

    def execute(self, statement: Statement) -> QueryResult:
        """Dispatch any parsed statement AST and return a structured QueryResult."""
        match statement:
            case CreateTable():
                return self.execute_create(statement)
            case DropTable():
                return self.execute_drop(statement)
            case CreateIndex():
                return self.execute_create_index(statement)
            case DropIndex():
                return self.execute_drop_index(statement)
            case InsertValues():
                return self.execute_insert(statement)
            case InsertSelect():
                return self.execute_insert_select(statement)
            case DeleteStatement():
                return self.execute_delete(statement)
            case UpdateStatement():
                return self.execute_update(statement)
            case SelectStatement():
                return self.execute_select_result(statement)
            case UnionStatement():
                return self.execute_union_result(statement)
            case ImportStatement():
                return self.execute_import(statement)
            case ShowTables():
                return self.execute_show_tables(statement)
            case ShowIndexes():
                return self.execute_show_indexes(statement)
            case DescribeTable():
                return self.execute_describe(statement)
            case _:
                raise EngineError(f'Unsupported statement type: {type(statement)}')

    # ------------------------------------------------------------------
    # DDL / DML
    # ------------------------------------------------------------------

    def execute_create(self, statement: CreateTable) -> QueryResult:
        defs = {col_name: _TYPE_MAP[type_str] for col_name, type_str in statement.columns}
        try:
            self.storage.create_table(statement.table_name, defs)
        except TableAlreadyExistsError:
            if not statement.if_not_exists:
                raise
        return QueryResult(kind=StatementKind.DDL)

    def execute_drop(self, statement: DropTable) -> QueryResult:
        try:
            self.storage.drop_table(statement.table_name)
        except TableNotFoundError:
            if not statement.if_exists:
                raise
        return QueryResult(kind=StatementKind.DDL)

    def execute_create_index(self, statement: CreateIndex) -> QueryResult:
        meta = self.storage.create_index(
            statement.index_name,
            statement.table_name,
            statement.column_name,
            if_not_exists=statement.if_not_exists,
        )
        return QueryResult(
            kind=StatementKind.DDL,
            message=f'Index {meta.name} created on {meta.table_name}({meta.column_name}).',
        )

    def execute_drop_index(self, statement: DropIndex) -> QueryResult:
        self.storage.drop_index(statement.index_name, if_exists=statement.if_exists)
        return QueryResult(
            kind=StatementKind.DDL,
            message=f'Index {statement.index_name} dropped.',
        )

    def execute_insert(self, statement: InsertValues) -> QueryResult:
        table = self._get_table(statement.table_name)
        all_columns = table.column_names()

        if statement.columns is None:
            target_columns = all_columns
        else:
            for col in statement.columns:
                if col not in all_columns:
                    raise ColumnNotFoundError(f'Column does not exist: {col}')
            target_columns = statement.columns

        prepared_rows: list[RowDict] = []
        for value_row in statement.values:
            if len(target_columns) != len(value_row):
                raise EngineError('Column count mismatch')
            row_data: RowDict = {col: val for col, val in zip(target_columns, value_row)}
            for col in all_columns:
                if col not in row_data:
                    row_data[col] = None
            prepared_rows.append(row_data)

        table.insert_rows(prepared_rows)
        count = len(prepared_rows)
        return QueryResult(
            kind=StatementKind.INSERT,
            affected_rows=count,
            message=f'{count} rows inserted.',
        )

    def execute_insert_select(self, statement: InsertSelect) -> QueryResult:
        table = self._get_table(statement.table_name)
        all_columns = table.column_names()

        if statement.columns is None:
            target_columns = all_columns
        else:
            for col in statement.columns:
                if col not in all_columns:
                    raise ColumnNotFoundError(f'Column does not exist: {col}')
            target_columns = statement.columns

        # Materialize full source query before writing to avoid self-insert feedback loops.
        query_res = (
            self.execute_select_result(statement.query)
            if isinstance(statement.query, SelectStatement)
            else self.execute_union_result(statement.query)
        )
        if len(query_res.columns) != len(target_columns):
            raise EngineError('Column count mismatch')

        prepared_rows: list[RowDict] = []
        for src_row in query_res.rows:
            row_data: RowDict = {col: val for col, val in zip(target_columns, src_row.values())}
            for col in all_columns:
                if col not in row_data:
                    row_data[col] = None
            prepared_rows.append(row_data)

        table.insert_rows(prepared_rows)
        count = len(prepared_rows)
        return QueryResult(
            kind=StatementKind.INSERT,
            affected_rows=count,
            message=f'{count} rows inserted.',
        )

    def execute_delete(self, statement: DeleteStatement) -> QueryResult:
        table = self._get_table(statement.table_name)
        where_func = None
        if statement.where_clause is not None:
            ctx = EvalContext(
                table_defs=table.column_types(),
                subquery_exec=self._exec_subquery,
                valid_single_qualifier=statement.table_name,
            )
            where_func = evaluate_where(statement.where_clause.expression, ctx)
        deleted = table.delete(where=where_func)
        return QueryResult(
            kind=StatementKind.DELETE,
            affected_rows=deleted,
            message=f'{deleted} rows deleted.',
        )

    def execute_update(self, statement: UpdateStatement) -> QueryResult:
        table = self._get_table(statement.table_name)
        col_types = table.column_types()
        for col, expr in statement.set_clauses:
            if col not in col_types:
                raise ColumnNotFoundError(f'Column does not exist: {col}')
            if isinstance(expr, LiteralExpression):
                _validate_literal_type(expr.value, col_types[col])

        ctx = EvalContext(
            table_defs=col_types,
            subquery_exec=self._exec_subquery,
            valid_single_qualifier=statement.table_name,
        )
        where_func = None
        if statement.where_clause is not None:
            where_func = evaluate_where(statement.where_clause.expression, ctx)

        def _updater(row_dict: RowDict) -> RowDict:
            return {col: evaluate_expression(expr, row_dict, ctx) for col, expr in statement.set_clauses}

        updated = table.update_rows(_updater, where=where_func)
        return QueryResult(
            kind=StatementKind.UPDATE,
            affected_rows=updated,
            message=f'{updated} rows updated.',
        )

    # ------------------------------------------------------------------
    # System commands & Import
    # ------------------------------------------------------------------

    def execute_show_tables(self, statement: ShowTables) -> QueryResult:
        rows: list[RowDict] = []
        for name, _, defs in self.storage.list_tables():
            table = self.storage.get_table(name)
            row_count = table.count_rows() if table else 0
            rows.append({'Table': name, 'Columns': ', '.join(defs.keys()), 'Rows': row_count})
        return QueryResult(
            kind=StatementKind.SYSTEM,
            columns=('Table', 'Columns', 'Rows'),
            rows=rows,
        )

    def execute_show_indexes(self, statement: ShowIndexes) -> QueryResult:
        metas = self.storage.list_indexes(statement.table_name)
        rows: list[RowDict] = [
            {'Index': m.name, 'Table': m.table_name, 'Column': m.column_name}
            for m in metas
        ]
        return QueryResult(
            kind=StatementKind.SYSTEM,
            columns=('Index', 'Table', 'Column'),
            rows=rows,
        )

    def execute_describe(self, statement: DescribeTable) -> QueryResult:
        table = self.storage.get_table(statement.table_name)
        if table is None:
            raise TableNotFoundError(f'Table does not exist: {statement.table_name}')
        rows: list[RowDict] = [
            {'Column': col_name, 'Type': col_type.value}
            for col_name, col_type in table.defs.items()
        ]
        rows.append({'Column': '__rows__', 'Type': str(table.count_rows())})
        return QueryResult(
            kind=StatementKind.SYSTEM,
            columns=('Column', 'Type'),
            rows=rows,
        )

    def execute_import(self, statement: ImportStatement) -> QueryResult:
        col_defs, prepared_rows = read_excel_table(statement.file_path, statement.columns)
        self.storage.create_table(statement.table_name, col_defs)
        try:
            table = self._get_table(statement.table_name)
            table.insert_rows(prepared_rows)
        except Exception:
            try:
                self.storage.drop_table(statement.table_name)
            except Exception:
                pass
            raise
        count = len(prepared_rows)
        return QueryResult(
            kind=StatementKind.IMPORT,
            affected_rows=count,
            message=f'{count} rows imported.',
        )

    # ------------------------------------------------------------------
    # SELECT & UNION
    # ------------------------------------------------------------------

    def execute_select_result(
        self,
        statement: SelectStatement,
        outer_row: RowDict | None = None,
    ) -> QueryResult:
        result, headers = self._build_select_rows(statement, outer_row=outer_row)

        if statement.offset > 0:
            result = result[statement.offset:]
        if statement.limit is not None:
            result = result[:statement.limit]

        if statement.output_file is not None:
            exported = export_rows(result, headers, statement.output_file)
            return QueryResult(
                kind=StatementKind.EXPORT,
                affected_rows=exported,
                message=f'{exported} rows exported.',
            )

        return QueryResult(
            kind=StatementKind.SELECT,
            columns=tuple(headers),
            rows=result,
        )

    def execute_union_result(
        self,
        statement: UnionStatement,
        outer_row: RowDict | None = None,
    ) -> QueryResult:
        first_stmt, _ = statement.branches[0]
        combined, headers = self._build_select_rows(first_stmt, outer_row=outer_row)
        expected_cols = len(headers)

        for branch_stmt, is_all in statement.branches[1:]:
            branch_rows, branch_headers = self._build_select_rows(branch_stmt, outer_row=outer_row)
            if len(branch_headers) != expected_cols:
                raise EngineError('The used SELECT statements have a different number of columns')
            rekeyed = [
                {h: v for h, v in zip(headers, r.values())}
                for r in branch_rows
            ]
            if is_all:
                combined.extend(rekeyed)
            else:
                combined = _dedup_rows(combined + rekeyed)

        if statement.order_by:
            pairs = [(r, r) for r in combined]
            pairs = _order_pairs(pairs, statement.order_by, valid_single_qualifier=None)
            combined = [proj_r for _, proj_r in pairs]

        if statement.offset > 0:
            combined = combined[statement.offset:]
        if statement.limit is not None:
            combined = combined[:statement.limit]

        if statement.output_file is not None:
            exported = export_rows(combined, headers, statement.output_file)
            return QueryResult(
                kind=StatementKind.EXPORT,
                affected_rows=exported,
                message=f'{exported} rows exported.',
            )

        return QueryResult(
            kind=StatementKind.UNION,
            columns=tuple(headers),
            rows=combined,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_table(self, name: str) -> Table:
        t = self.storage.get_table(name)
        if t is None:
            raise TableNotFoundError(f'Table does not exist: {name}')
        return t

    def _exec_subquery(self, sub: object, outer_row: RowDict | None = None) -> list[RowDict]:
        if isinstance(sub, SelectStatement):
            return self.execute_select_result(sub, outer_row=outer_row).rows
        if isinstance(sub, UnionStatement):
            return self.execute_union_result(sub, outer_row=outer_row).rows
        return []

    def _build_select_rows(
        self,
        statement: SelectStatement,
        outer_row: RowDict | None = None,
    ) -> tuple[list[RowDict], list[str]]:
        """Execute the core SELECT pipeline up through ORDER BY (before LIMIT/OFFSET)."""
        sources = statement.sources
        tables = [self._get_table(src.name) for src in sources]

        seen_aliases: set[str] = set()
        for src in sources:
            alias = src.alias or src.name
            if alias in seen_aliases:
                raise EngineError(f'Not unique table/alias: {alias}')
            seen_aliases.add(alias)
        for jc in statement.joins:
            alias = jc.source.alias or jc.source.name
            if alias in seen_aliases:
                raise EngineError(f'Not unique table/alias: {alias}')
            seen_aliases.add(alias)

        use_prefixes = len(sources) > 1 or bool(statement.joins)
        single_qualifier = (sources[0].alias or sources[0].name) if not use_prefixes else None

        combined: list[RowDict] = []
        visible_columns: list[_VisibleColumn] | None = None
        residual_where = statement.where_clause.expression if statement.where_clause is not None else None

        if use_prefixes:
            rows_per_table = [self.storage.get_table_rows(table) for table in tables]
            for combo in _cartesian_product(rows_per_table):
                row: RowDict = {}
                for src, r in zip(sources, combo):
                    alias = src.alias or src.name
                    row.update(_prefix_row(r, alias))
                combined.append(row)
            visible_columns = []
            for src, table in zip(sources, tables):
                alias = src.alias or src.name
                visible_columns.extend(
                    _VisibleColumn(alias, col, f"{alias}.{col}") for col in table.column_names()
                )
        else:
            all_rows = self.storage.get_table_rows(tables[0])
            idx_match = None
            if self.enable_optimizations and residual_where is not None:
                table_indexes = self.storage.get_table_indexes(tables[0])
                idx_match = match_single_table_index(residual_where, table_indexes, single_qualifier)
            if idx_match is not None:
                combined = [dict(all_rows[rid]) for rid in idx_match.row_ids]
                residual_where = idx_match.residual_expr
            else:
                combined = [dict(r) for r in all_rows]

        # Apply explicit JOIN clauses left-to-right.
        for jc in statement.joins:
            join_table = self._get_table(jc.source.name)
            join_alias = jc.source.alias or jc.source.name
            right_columns = [
                _VisibleColumn(join_alias, col, f"{join_alias}.{col}")
                for col in join_table.column_names()
            ]
            right_rows = [_prefix_row(r, join_alias) for r in self.storage.get_table_rows(join_table)]
            combined, visible_columns = self._apply_join(
                combined, visible_columns or [], right_rows, right_columns, jc,
                outer_row=outer_row,
            )
            tables.append(join_table)

        all_defs: dict[str, Types] = {}
        for t in tables:
            all_defs.update(t.column_types())

        base_ctx = EvalContext(
            table_defs=all_defs,
            subquery_exec=self._exec_subquery,
            outer_row=outer_row,
            valid_single_qualifier=single_qualifier,
        )

        # WHERE applies after the full FROM/JOIN has been built.
        if residual_where is not None:
            where_func = evaluate_where(residual_where, base_ctx)
            combined = [r for r in combined if where_func(r)]

        table_columns_map: dict[str, list[str]] = {}
        for src, t in zip(sources, tables[:len(sources)]):
            table_columns_map[src.alias or src.name] = t.column_names()
        for jc, t in zip(statement.joins, tables[len(sources):]):
            table_columns_map[jc.source.alias or jc.source.name] = t.column_names()

        expanded_columns = (
            _expand_select_expressions(
                statement.columns,
                tables[0].column_names(),
                table_columns_map,
                visible_columns,
                use_prefixes,
            )
            if statement.columns is not None
            else None
        )

        has_agg_in_select = expanded_columns is not None and any(
            contains_aggregate(se.expr) for se in expanded_columns
        )
        has_agg_in_having = statement.having is not None and contains_aggregate(statement.having.expression)
        is_grouped = has_agg_in_select or statement.group_by is not None or has_agg_in_having

        paired: list[tuple[RowDict, RowDict]] = []

        if not is_grouped:
            if expanded_columns is not None:
                for row in combined:
                    proj = _project_select_expressions(row, expanded_columns, base_ctx)
                    paired.append((row, proj))
            elif visible_columns is not None:
                projected = _project_star(combined, visible_columns)
                paired = list(zip(combined, projected))
            else:
                paired = [(row, dict(row)) for row in combined]
        else:
            groups = _group_rows(combined, statement.group_by, single_qualifier)
            for _, rep_row, group_rows in groups:
                def _agg_eval(agg_expr: AggregateExpression, _gr=group_rows) -> DataValue:
                    return _eval_aggregate(agg_expr, _gr, single_qualifier)

                group_ctx = base_ctx.with_agg(_agg_eval)

                if expanded_columns is not None:
                    proj = _project_select_expressions(rep_row, expanded_columns, group_ctx)
                else:
                    proj = dict(rep_row)

                if statement.having is not None:
                    having_row = {**rep_row, **proj}
                    if not to_bool(evaluate_expression(statement.having.expression, having_row, group_ctx)):
                        continue

                paired.append((rep_row, proj))

        # DISTINCT (dedup on projected row)
        if statement.distinct:
            seen = set()
            unique_pairs: list[tuple[RowDict, RowDict]] = []
            for raw_r, proj_r in paired:
                key = _row_dedup_key(proj_r)
                if key not in seen:
                    seen.add(key)
                    unique_pairs.append((raw_r, proj_r))
            paired = unique_pairs

        # ORDER BY (resolve against projected row first, then raw/group row)
        if statement.order_by:
            paired = _order_pairs(paired, statement.order_by, single_qualifier)

        result = [proj_r for _, proj_r in paired]

        if result:
            headers = list(result[0].keys())
        elif expanded_columns is not None:
            headers = _compute_select_headers(expanded_columns)
        elif visible_columns is not None:
            headers = _compute_star_headers(visible_columns)
        else:
            headers = tables[0].column_names()

        return result, headers

    def _apply_join(
        self,
        left_rows: list[RowDict],
        left_columns: list[_VisibleColumn],
        right_rows: list[RowDict],
        right_columns: list[_VisibleColumn],
        jc: JoinClause,
        outer_row: RowDict | None = None,
    ) -> tuple[list[RowDict], list[_VisibleColumn]]:
        """Apply one JOIN clause to the current left relation."""
        common = _common_join_columns(left_columns, right_columns, jc)
        null_right = {col.key: None for col in right_columns}
        null_left = {col.key: None for col in left_columns}
        new_rows: list[RowDict] = []
        matched_right: set[int] = set()

        if jc.join_type == JoinType.CROSS:
            for left_row in left_rows:
                for right_row in right_rows:
                    new_rows.append({**left_row, **right_row})
        else:
            for left_row in left_rows:
                matches: list[tuple[int, RowDict]] = []
                for right_index, right_row in enumerate(right_rows):
                    if self._join_match(left_row, right_row, left_columns, right_columns, jc, common, outer_row):
                        matches.append((right_index, {**left_row, **right_row}))
                        if jc.join_type in (JoinType.RIGHT, JoinType.FULL):
                            matched_right.add(right_index)
                if matches:
                    new_rows.extend(row for _, row in matches)
                elif jc.join_type in (JoinType.LEFT, JoinType.FULL):
                    new_rows.append({**left_row, **null_right})

            if jc.join_type in (JoinType.RIGHT, JoinType.FULL):
                for right_index, right_row in enumerate(right_rows):
                    if right_index not in matched_right:
                        new_rows.append({**null_left, **right_row})

        visible = _visible_after_join(left_columns, right_columns, jc, common)

        if common is not None:
            coalesced_cols = [col for col in visible if col.coalesce_keys]
            for row in new_rows:
                for col in coalesced_cols:
                    row[col.name] = _coalesced_value(row, col)

        return new_rows, visible

    def _join_match(
        self,
        left_row: RowDict,
        right_row: RowDict,
        left_columns: list[_VisibleColumn],
        right_columns: list[_VisibleColumn],
        jc: JoinClause,
        common: list[str] | None,
        outer_row: RowDict | None = None,
    ) -> bool:
        """Test whether one left/right row pair satisfies the JOIN condition."""
        if jc.condition is not None:
            ctx = EvalContext(subquery_exec=self._exec_subquery, outer_row=outer_row)
            return to_bool(evaluate_expression(jc.condition, {**left_row, **right_row}, ctx))

        if common is not None:
            for name in common:
                left_cols = [col for col in left_columns if col.name == name]
                right_cols = [col for col in right_columns if col.name == name]
                if len(left_cols) != 1 or len(right_cols) != 1:
                    raise EngineError(f'Ambiguous column in JOIN: {name}')
                left_value = _coalesced_value(left_row, left_cols[0])
                right_value = _coalesced_value(right_row, right_cols[0])
                if left_value is None or right_value is None or left_value != right_value:
                    return False
            return True

        return True


# ======================================================================
# Pure aggregation, projection, dedup, ordering, and JOIN helpers
# ======================================================================

def _validate_literal_type(value: DataValue, col_type: Types) -> None:
    if value is None:
        return
    if col_type == Types.NUMBER:
        to_decimal(value)
    elif col_type == Types.STRING and not isinstance(value, str):
        raise TypeMismatchError(f'Expected STRING value, got {type(value).__name__}')


def _expand_select_expressions(
    select_exprs: list[SelectExpression],
    single_table_columns: list[str],
    table_columns_map: dict[str, list[str]],
    visible_columns: list[_VisibleColumn] | None,
    use_prefixes: bool,
) -> list[SelectExpression]:
    """Expand * and table.* wildcards into concrete ColumnExpression items."""
    expanded: list[SelectExpression] = []
    for se in select_exprs:
        if not isinstance(se.expr, StarExpression):
            expanded.append(se)
            continue
        target_table = se.expr.table
        if target_table is None:
            if visible_columns is not None:
                for col in visible_columns:
                    col_expr = (
                        ColumnExpression(col.name, None)
                        if col.coalesce_keys
                        else ColumnExpression(col.name, col.qualifier)
                    )
                    expanded.append(SelectExpression(col_expr, None))
            else:
                for col_name in single_table_columns:
                    expanded.append(SelectExpression(ColumnExpression(col_name, None), None))
        else:
            if target_table not in table_columns_map:
                raise TableNotFoundError(f'Unknown table or alias: {target_table}')
            for col_name in table_columns_map[target_table]:
                col_expr = ColumnExpression(col_name, target_table if use_prefixes else target_table)
                expanded.append(SelectExpression(col_expr, None))
    return expanded


def _eval_aggregate(
    agg_expr: AggregateExpression,
    group_rows: list[RowDict],
    valid_single_qualifier: str | None,
) -> DataValue:
    func, col = agg_expr.func, agg_expr.column
    if func == AggFunc.COUNT and col is None:
        return Decimal(len(group_rows))

    assert col is not None
    ctx = EvalContext(valid_single_qualifier=valid_single_qualifier)
    vals = [evaluate_expression(col, r, ctx) for r in group_rows]
    non_null = [v for v in vals if v is not None]

    if agg_expr.distinct:
        seen = set()
        deduped: list[DataValue] = []
        for v in non_null:
            key = (1, v) if isinstance(v, Decimal) else (2, v)
            if key not in seen:
                seen.add(key)
                deduped.append(v)
        non_null = deduped

    match func:
        case AggFunc.COUNT:
            return Decimal(len(non_null))
        case AggFunc.SUM:
            if not non_null:
                return None
            return sum((to_decimal(v) for v in non_null), Decimal(0))
        case AggFunc.AVG:
            if not non_null:
                return None
            num_vals = [to_decimal(v) for v in non_null]
            return sum(num_vals, Decimal(0)) / Decimal(len(num_vals))
        case AggFunc.MIN:
            if not non_null:
                return None
            try:
                return min(non_null)
            except TypeError as exc:
                raise TypeMismatchError(f'Incompatible types in MIN: {exc}') from exc
        case AggFunc.MAX:
            if not non_null:
                return None
            try:
                return max(non_null)
            except TypeError as exc:
                raise TypeMismatchError(f'Incompatible types in MAX: {exc}') from exc


def _group_rows(
    rows: list[RowDict],
    group_by: list[ColumnExpression] | None,
    valid_single_qualifier: str | None,
) -> list[tuple[tuple, RowDict, list[RowDict]]]:
    """Group rows and build a representative row containing only GROUP BY columns."""
    if not group_by:
        return [((), {}, rows)]

    grouped: dict[tuple, tuple[RowDict, list[RowDict]]] = {}
    for row in rows:
        key_vals: list[DataValue] = []
        rep_row: RowDict = {}
        for col in group_by:
            val = resolve_column(row, col.column_name, col.table, valid_single_qualifier=valid_single_qualifier)
            key_vals.append(val)
            if col.table is not None:
                rep_row[f"{col.table}.{col.column_name}"] = val
            else:
                rep_row[col.column_name] = val
                matching_keys = [k for k in row if k.endswith(f".{col.column_name}")]
                if len(matching_keys) == 1:
                    rep_row[matching_keys[0]] = val
        key = tuple(
            (0, None) if v is None else (1, v) if isinstance(v, Decimal) else (2, v)
            for v in key_vals
        )
        if key not in grouped:
            grouped[key] = (rep_row, [])
        grouped[key][1].append(row)

    return [(key, rep, g_rows) for key, (rep, g_rows) in grouped.items()]


def _project_select_expressions(
    row: RowDict,
    select_exprs: list[SelectExpression],
    ctx: EvalContext,
) -> RowDict:
    new_row: RowDict = {}
    for se in select_exprs:
        val = evaluate_expression(se.expr, row, ctx)
        name = se.alias or resolve_column_name(se.expr) or '?'
        if se.alias is None and isinstance(se.expr, ColumnExpression) and se.expr.table is not None and name in new_row:
            name = f"{se.expr.table}.{se.expr.column_name}"
        new_row[name] = val
    return new_row


def _compute_select_headers(select_exprs: list[SelectExpression]) -> list[str]:
    headers: list[str] = []
    used: set[str] = set()
    for se in select_exprs:
        name = se.alias or resolve_column_name(se.expr) or '?'
        if se.alias is None and isinstance(se.expr, ColumnExpression) and se.expr.table is not None and name in used:
            name = f"{se.expr.table}.{se.expr.column_name}"
        used.add(name)
        headers.append(name)
    return headers


def _compute_star_headers(visible_columns: list[_VisibleColumn]) -> list[str]:
    used_names: set[str] = set()
    headers: list[str] = []
    for col in visible_columns:
        header = col.name
        if header in used_names:
            header = f"{col.qualifier}.{col.name}"
        used_names.add(col.name)
        headers.append(header)
    return headers


def _row_dedup_key(row: RowDict) -> tuple:
    return tuple(
        (k, (0, None) if v is None else (1, v) if isinstance(v, Decimal) else (2, v))
        for k, v in row.items()
    )


def _dedup_rows(rows: list[RowDict]) -> list[RowDict]:
    seen = set()
    result: list[RowDict] = []
    for r in rows:
        key = _row_dedup_key(r)
        if key not in seen:
            seen.add(key)
            result.append(r)
    return result


def _resolve_order_value(
    raw_row: RowDict,
    proj_row: RowDict,
    col_expr: ColumnExpression,
    valid_single_qualifier: str | None,
) -> DataValue:
    if col_expr.table is None:
        if col_expr.column_name in proj_row:
            return proj_row[col_expr.column_name]
        return resolve_column(
            raw_row, col_expr.column_name, None,
            valid_single_qualifier=valid_single_qualifier,
        )
    qualified = f"{col_expr.table}.{col_expr.column_name}"
    if qualified in proj_row:
        return proj_row[qualified]
    return resolve_column(
        raw_row, col_expr.column_name, col_expr.table,
        valid_single_qualifier=valid_single_qualifier,
    )


def _order_pairs(
    paired: list[tuple[RowDict, RowDict]],
    order_by: list[tuple[ColumnExpression, bool]],
    valid_single_qualifier: str | None,
) -> list[tuple[RowDict, RowDict]]:
    result = list(paired)
    for col_expr, desc in reversed(order_by):
        keyed = [
            (_resolve_order_value(raw_r, proj_r, col_expr, valid_single_qualifier), raw_r, proj_r)
            for raw_r, proj_r in result
        ]
        null_items = [(raw_r, proj_r) for val, raw_r, proj_r in keyed if val is None]
        non_null_items = [(val, raw_r, proj_r) for val, raw_r, proj_r in keyed if val is not None]
        try:
            non_null_items.sort(key=lambda item: item[0], reverse=desc)
        except TypeError as exc:
            raise TypeMismatchError(f'Cannot sort mixed types in column {col_expr.column_name}: {exc}') from exc
        result = [(raw_r, proj_r) for _, raw_r, proj_r in non_null_items] + null_items
    return result


def _prefix_row(row: RowDict, alias: str) -> RowDict:
    return {f"{alias}.{col}": val for col, val in row.items()}


def _coalesced_value(row: RowDict, col: _VisibleColumn) -> DataValue:
    value = row.get(col.key)
    if value is not None:
        return value
    for fallback in col.coalesce_keys:
        value = row.get(fallback)
        if value is not None:
            return value
    return None


def _common_join_columns(
    left_columns: list[_VisibleColumn],
    right_columns: list[_VisibleColumn],
    jc: JoinClause,
) -> list[str] | None:
    left_names = {col.name for col in left_columns}
    right_names = {col.name for col in right_columns}

    if jc.natural:
        common: list[str] = []
        for col in left_columns:
            if col.name in right_names and col.name not in common:
                common.append(col.name)
        return common

    if jc.using_columns is not None:
        missing = [
            name for name in jc.using_columns
            if name not in left_names or name not in right_names
        ]
        if missing:
            raise ColumnNotFoundError(
                f'USING column does not exist in both tables: {missing[0]}')
        return list(jc.using_columns)

    return None


def _visible_after_join(
    left_columns: list[_VisibleColumn],
    right_columns: list[_VisibleColumn],
    jc: JoinClause,
    common: list[str] | None,
) -> list[_VisibleColumn]:
    if common is None:
        return [*left_columns, *right_columns]

    common_set = set(common)
    visible: list[_VisibleColumn] = []

    for name in common:
        left_col = next((col for col in left_columns if col.name == name), None)
        right_col = next((col for col in right_columns if col.name == name), None)
        if left_col is not None and right_col is not None:
            visible.append(_VisibleColumn(
                left_col.qualifier,
                name,
                left_col.key,
                (*left_col.coalesce_keys, right_col.key),
            ))
        elif left_col is not None:
            visible.append(left_col)
        elif right_col is not None:
            visible.append(right_col)

    visible.extend(col for col in left_columns if col.name not in common_set)
    visible.extend(col for col in right_columns if col.name not in common_set)
    return visible


def _project_star(rows: list[RowDict], visible_columns: list[_VisibleColumn]) -> list[RowDict]:
    if not rows:
        return []
    headers = _compute_star_headers(visible_columns)
    result: list[RowDict] = []
    for row in rows:
        projected: RowDict = {}
        for col, header in zip(visible_columns, headers):
            projected[header] = _coalesced_value(row, col)
        result.append(projected)
    return result


def _cartesian_product(rows_per_table: list[list[RowDict]]) -> list[list[RowDict]]:
    if not rows_per_table:
        return []
    result = [[r] for r in rows_per_table[0]]
    for table_rows in rows_per_table[1:]:
        new_result = []
        for combo in result:
            for r in table_rows:
                new_result.append(combo + [r])
        result = new_result
    return result


# ======================================================================
# Backward-compatible module-level functions (operating on current working dir)
# ======================================================================

def execute_create(statement: CreateTable) -> None:
    QueryExecutor(StorageEngine()).execute_create(statement)


def execute_drop(statement: DropTable) -> None:
    QueryExecutor(StorageEngine()).execute_drop(statement)


def execute_insert(statement: InsertValues) -> None:
    QueryExecutor(StorageEngine()).execute_insert(statement)


def execute_delete(statement: DeleteStatement) -> int:
    return QueryExecutor(StorageEngine()).execute_delete(statement).affected_rows or 0


def execute_update(statement: UpdateStatement) -> int:
    return QueryExecutor(StorageEngine()).execute_update(statement).affected_rows or 0


def execute_select(statement: SelectStatement, outer_row: RowDict | None = None) -> list[RowDict] | int:
    return QueryExecutor(StorageEngine()).execute_select_result(statement, outer_row=outer_row).to_legacy()


def execute_union(statement: UnionStatement, outer_row: RowDict | None = None) -> list[RowDict] | int:
    return QueryExecutor(StorageEngine()).execute_union_result(statement, outer_row=outer_row).to_legacy()


def execute_import(statement: ImportStatement) -> int:
    return QueryExecutor(StorageEngine()).execute_import(statement).affected_rows or 0


def execute_show_tables(statement: ShowTables) -> list[RowDict]:
    return QueryExecutor(StorageEngine()).execute_show_tables(statement).rows


def execute_describe(statement: DescribeTable) -> list[RowDict]:
    return QueryExecutor(StorageEngine()).execute_describe(statement).rows
