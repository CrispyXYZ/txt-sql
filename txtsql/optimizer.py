"""Query optimization helpers: predicate pushdown, implicit join rewrite, and hash-join planning."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

from .ast import (
    AggregateExpression,
    AnyAllExpression,
    BetweenExpression,
    BinaryExpression,
    ColumnExpression,
    ComparisonOp,
    ConditionExpression,
    ExistsExpression,
    Expression,
    InExpression,
    InListExpression,
    JoinClause,
    JoinType,
    LikeExpression,
    LogicalExpression,
    NotExpression,
    NullCheckExpression,
    ScalarSubqueryExpression,
    TableSource,
)
from .exceptions import EngineError
from .index import combine_and_conjuncts, split_and_conjuncts
from .types import DataValue, RowDict


@dataclass(frozen=True, slots=True)
class VisibleColumn:
    """A column visible in the current joined relation."""
    qualifier: str
    name: str
    key: str
    coalesce_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EquiKeySpec:
    """Specifies how to read a join key value from a prefixed/coalesced row dict."""
    primary_key: str
    coalesce_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PushdownPlan:
    """Result of single-table predicate pushdown and comma-join rewriting."""
    source_predicates: dict[str, Expression]
    rewritten_joins: list[JoinClause]
    residual_where: Expression | None
    pushed_count: int
    rewritten_implicit_joins: int


def read_equi_key(row: RowDict, spec: EquiKeySpec) -> DataValue:
    """Read a join key from a row dict, supporting coalesced USING/NATURAL fallback keys."""
    val = row.get(spec.primary_key)
    if val is not None or not spec.coalesce_keys:
        return val
    for fb in spec.coalesce_keys:
        val = row.get(fb)
        if val is not None:
            return val
    return None


def _contains_subquery_or_agg(expr: Expression) -> bool:
    match expr:
        case ScalarSubqueryExpression() | InExpression() | AnyAllExpression() | ExistsExpression() | AggregateExpression():
            return True
        case BinaryExpression(left, _, right) | ConditionExpression(left, _, right) | LogicalExpression(left, _, right):
            return _contains_subquery_or_agg(left) or _contains_subquery_or_agg(right)
        case NullCheckExpression(inner, _) | NotExpression(inner):
            return _contains_subquery_or_agg(inner)
        case InListExpression(inner, items, _):
            return _contains_subquery_or_agg(inner) or any(_contains_subquery_or_agg(i) for i in items)
        case BetweenExpression(inner, low, high, _):
            return (
                _contains_subquery_or_agg(inner)
                or _contains_subquery_or_agg(low)
                or _contains_subquery_or_agg(high)
            )
        case LikeExpression(inner, pattern, _):
            return _contains_subquery_or_agg(inner) or _contains_subquery_or_agg(pattern)
        case _:
            return False


def _collect_expr_columns(expr: Expression) -> list[ColumnExpression]:
    match expr:
        case ColumnExpression():
            return [expr]
        case BinaryExpression(left, _, right) | ConditionExpression(left, _, right) | LogicalExpression(left, _, right):
            return _collect_expr_columns(left) + _collect_expr_columns(right)
        case NullCheckExpression(inner, _) | NotExpression(inner):
            return _collect_expr_columns(inner)
        case InListExpression(inner, items, _):
            cols = _collect_expr_columns(inner)
            for item in items:
                cols.extend(_collect_expr_columns(item))
            return cols
        case BetweenExpression(inner, low, high, _):
            return _collect_expr_columns(inner) + _collect_expr_columns(low) + _collect_expr_columns(high)
        case LikeExpression(inner, pattern, _):
            return _collect_expr_columns(inner) + _collect_expr_columns(pattern)
        case _:
            return []


def _resolve_col_alias(col: ColumnExpression, table_columns_map: dict[str, list[str]]) -> str | None:
    if col.table is not None:
        cols = table_columns_map.get(col.table)
        if cols is not None and col.column_name in cols:
            return col.table
        return None
    matches = [alias for alias, cols in table_columns_map.items() if col.column_name in cols]
    if len(matches) == 1:
        return matches[0]
    return None


def _compute_null_producing_aliases(
    sources: Sequence[TableSource],
    joins: Sequence[JoinClause],
) -> set[str]:
    """Return aliases that may be NULL-padded by LEFT/RIGHT/FULL outer joins."""
    accumulated: set[str] = {src.alias or src.name for src in sources}
    blocked: set[str] = set()
    for jc in joins:
        right_alias = jc.source.alias or jc.source.name
        if jc.join_type == JoinType.LEFT:
            blocked.add(right_alias)
        elif jc.join_type == JoinType.RIGHT:
            blocked.update(accumulated)
        elif jc.join_type == JoinType.FULL:
            blocked.update(accumulated)
            blocked.add(right_alias)
        accumulated.add(right_alias)
    return blocked


def analyze_predicate_pushdown(
    where_expr: Expression | None,
    sources: Sequence[TableSource],
    joins: Sequence[JoinClause],
    table_columns_map: dict[str, list[str]],
    using_or_natural_cols: set[str] | None = None,
) -> PushdownPlan:
    """Push safe single-table WHERE conjuncts below joins and rewrite comma FROMs."""
    if where_expr is None:
        cross_joins = [JoinClause(JoinType.CROSS, extra_src) for extra_src in sources[1:]]
        return PushdownPlan(
            source_predicates={},
            rewritten_joins=cross_joins + list(joins),
            residual_where=None,
            pushed_count=0,
            rewritten_implicit_joins=0,
        )

    blocked_aliases = _compute_null_producing_aliases(sources, joins)
    merged_cols = using_or_natural_cols or set()
    conjuncts = split_and_conjuncts(where_expr)

    pushed_lists: dict[str, list[Expression]] = defaultdict(list)
    remaining: list[Expression] = []
    pushed_count = 0

    for conj in conjuncts:
        if _contains_subquery_or_agg(conj):
            remaining.append(conj)
            continue
        cols = _collect_expr_columns(conj)
        if not cols:
            remaining.append(conj)
            continue
        # Do not push predicates referencing unqualified USING/NATURAL merged columns.
        if any(c.table is None and c.column_name in merged_cols for c in cols):
            remaining.append(conj)
            continue

        resolved_aliases = [_resolve_col_alias(c, table_columns_map) for c in cols]
        if any(a is None for a in resolved_aliases):
            remaining.append(conj)
            continue
        unique_aliases = set(resolved_aliases)  # type: ignore[arg-type]
        if len(unique_aliases) == 1:
            target_alias = next(iter(unique_aliases))
            if target_alias not in blocked_aliases:
                pushed_lists[target_alias].append(conj)
                pushed_count += 1
                continue
        remaining.append(conj)

    rewritten_joins: list[JoinClause] = []
    rewritten_implicit_joins = 0

    # Convert comma-separated FROM sources (s0, s1, ...) into explicit synthetic INNER/CROSS joins
    # so equi-join conditions in WHERE execute via Hash Join instead of a full Cartesian product.
    if len(sources) > 1:
        available: set[str] = {sources[0].alias or sources[0].name}
        next_remaining: list[Expression] = []
        used_idx: set[int] = set()
        for extra_src in sources[1:]:
            extra_alias = extra_src.alias or extra_src.name
            scope = available | {extra_alias}
            join_conds: list[Expression] = []
            for idx, conj in enumerate(remaining):
                if idx in used_idx or _contains_subquery_or_agg(conj):
                    continue
                if not isinstance(conj, ConditionExpression) or conj.op != ComparisonOp.EQ:
                    continue
                if not (isinstance(conj.left, ColumnExpression) and isinstance(conj.right, ColumnExpression)):
                    continue
                if any(c.table is None and c.column_name in merged_cols for c in (conj.left, conj.right)):
                    continue
                la = _resolve_col_alias(conj.left, table_columns_map)
                ra = _resolve_col_alias(conj.right, table_columns_map)
                if la is None or ra is None or la == ra:
                    continue
                if la in scope and ra in scope and (la == extra_alias or ra == extra_alias):
                    join_conds.append(conj)
                    used_idx.add(idx)

            if join_conds:
                rewritten_joins.append(
                    JoinClause(JoinType.INNER, extra_src, condition=combine_and_conjuncts(join_conds))
                )
                rewritten_implicit_joins += 1
            else:
                rewritten_joins.append(JoinClause(JoinType.CROSS, extra_src))
            available.add(extra_alias)

        next_remaining = [c for idx, c in enumerate(remaining) if idx not in used_idx]
        remaining = next_remaining

    rewritten_joins.extend(joins)
    source_predicates = {
        alias: expr
        for alias, exprs in pushed_lists.items()
        if (expr := combine_and_conjuncts(exprs)) is not None
    }
    return PushdownPlan(
        source_predicates=source_predicates,
        rewritten_joins=rewritten_joins,
        residual_where=combine_and_conjuncts(remaining),
        pushed_count=pushed_count,
        rewritten_implicit_joins=rewritten_implicit_joins,
    )


def _find_side_equi_key(
    col_expr: ColumnExpression,
    left_columns: Sequence[VisibleColumn],
    right_columns: Sequence[VisibleColumn],
) -> tuple[str, EquiKeySpec] | None:
    if col_expr.table is not None:
        target = f'{col_expr.table}.{col_expr.column_name}'
        in_left = any(target == c.key or target in c.coalesce_keys for c in left_columns)
        in_right = any(target == c.key or target in c.coalesce_keys for c in right_columns)
        if in_left and not in_right:
            return ('L', EquiKeySpec(target))
        if in_right and not in_left:
            return ('R', EquiKeySpec(target))
        return None

    col_name = col_expr.column_name
    left_coalesced = [c for c in left_columns if c.coalesce_keys and c.name == col_name]
    right_matches = [c for c in right_columns if c.name == col_name]
    if left_coalesced and not right_matches:
        if len(left_coalesced) == 1:
            c = left_coalesced[0]
            return ('L', EquiKeySpec(c.key, c.coalesce_keys))
        return None

    left_matches = [c for c in left_columns if c.name == col_name]
    total = len(left_matches) + len(right_matches)
    if total != 1:
        return None
    if left_matches:
        c = left_matches[0]
        return ('L', EquiKeySpec(c.key, c.coalesce_keys))
    c = right_matches[0]
    return ('R', EquiKeySpec(c.key, c.coalesce_keys))


def extract_equi_join_keys(
    left_columns: Sequence[VisibleColumn],
    right_columns: Sequence[VisibleColumn],
    jc: JoinClause,
    common: list[str] | None,
) -> tuple[list[tuple[EquiKeySpec, EquiKeySpec]], Expression | None]:
    """Extract (equi_key_pairs, residual_condition) for a JOIN clause.

    Returns empty equi_key_pairs when Hash Join cannot be applied (e.g. non-equi or CROSS).
    """
    if jc.join_type == JoinType.CROSS:
        return [], None

    if common is not None:
        pairs: list[tuple[EquiKeySpec, EquiKeySpec]] = []
        for name in common:
            left_cols = [col for col in left_columns if col.name == name]
            right_cols = [col for col in right_columns if col.name == name]
            if len(left_cols) != 1 or len(right_cols) != 1:
                raise EngineError(f'Ambiguous column in JOIN: {name}')
            lc, rc = left_cols[0], right_cols[0]
            pairs.append((
                EquiKeySpec(lc.key, lc.coalesce_keys),
                EquiKeySpec(rc.key, rc.coalesce_keys),
            ))
        return pairs, None

    if jc.condition is None:
        return [], None

    pairs = []
    residual: list[Expression] = []
    for conj in split_and_conjuncts(jc.condition):
        if (
            isinstance(conj, ConditionExpression)
            and conj.op == ComparisonOp.EQ
            and isinstance(conj.left, ColumnExpression)
            and isinstance(conj.right, ColumnExpression)
        ):
            ls = _find_side_equi_key(conj.left, left_columns, right_columns)
            rs = _find_side_equi_key(conj.right, left_columns, right_columns)
            if ls is not None and rs is not None and ls[0] != rs[0]:
                if ls[0] == 'L':
                    pairs.append((ls[1], rs[1]))
                else:
                    pairs.append((rs[1], ls[1]))
                continue
        residual.append(conj)

    return pairs, combine_and_conjuncts(residual)
