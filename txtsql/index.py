"""In-memory secondary index (Hash + sorted range bisect) for TSV tables."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from .ast import (
    ArithmeticOp,
    BetweenExpression,
    BinaryExpression,
    ColumnExpression,
    ComparisonOp,
    ConditionExpression,
    Expression,
    InListExpression,
    LiteralExpression,
    LogicalExpression,
    LogicalOp,
)
from .evaluator import is_number, to_decimal
from .exceptions import TypeMismatchError
from .types import DataValue, RowDict, Types


_UNBOUNDED = object()


@dataclass(frozen=True, slots=True)
class IndexMeta:
    """Metadata describing a secondary index persisted in indexes.txt."""
    name: str
    table_name: str
    column_name: str


def _coerce_key(value: object, col_type: Types) -> Decimal | str | None:
    """Normalize a value to the index's canonical comparable key, or None if incompatible."""
    if value is None:
        return None
    if col_type == Types.NUMBER:
        if is_number(value):
            return to_decimal(value)
        return None
    if col_type == Types.STRING:
        if isinstance(value, str):
            return value
        return None
    return None


class ColumnIndex:
    """Hybrid Hash + sorted-array index supporting O(1) equality/IN and O(log N) range scans."""

    def __init__(self, meta: IndexMeta, col_type: Types, rows: list[RowDict]) -> None:
        self.meta = meta
        self.col_type = col_type
        self.row_count = len(rows)
        self.hash_map: dict[Decimal | str, list[int]] = defaultdict(list)

        pairs: list[tuple[Decimal | str, int]] = []
        col_name = meta.column_name
        for idx, row in enumerate(rows):
            val = row.get(col_name)
            key = _coerce_key(val, col_type)
            if key is None:
                continue
            self.hash_map[key].append(idx)
            pairs.append((key, idx))

        pairs.sort(key=lambda item: (item[0], item[1]))
        self.sorted_keys: list[Decimal | str] = [k for k, _ in pairs]
        self.sorted_row_ids: list[int] = [rid for _, rid in pairs]

    def lookup_eq(self, value: object) -> list[int] | None:
        """Return matching row indices for `col = value`, [] for NULL, or None on type mismatch."""
        if value is None:
            return []
        key = _coerce_key(value, self.col_type)
        if key is None:
            return None
        return list(self.hash_map.get(key, ()))

    def lookup_in(self, values: list[object]) -> list[int] | None:
        """Return sorted unique matching row indices for `col IN (...)`, or None if incompatible."""
        seen_ids: set[int] = set()
        for val in values:
            if val is None:
                continue
            key = _coerce_key(val, self.col_type)
            if key is None:
                return None
            seen_ids.update(self.hash_map.get(key, ()))
        return sorted(seen_ids)

    def lookup_range(
        self,
        low: object = _UNBOUNDED,
        include_low: bool = True,
        high: object = _UNBOUNDED,
        include_high: bool = True,
    ) -> list[int] | None:
        """Return sorted row indices within optional [low, high] bounds, or None on type mismatch."""
        start = 0
        end = len(self.sorted_keys)

        if low is not _UNBOUNDED:
            if low is None:
                return []
            low_key = _coerce_key(low, self.col_type)
            if low_key is None:
                return None
            start = (
                bisect_left(self.sorted_keys, low_key)
                if include_low
                else bisect_right(self.sorted_keys, low_key)
            )

        if high is not _UNBOUNDED:
            if high is None:
                return []
            high_key = _coerce_key(high, self.col_type)
            if high_key is None:
                return None
            end = (
                bisect_right(self.sorted_keys, high_key)
                if include_high
                else bisect_left(self.sorted_keys, high_key)
            )

        if start >= end:
            return []
        return sorted(self.sorted_row_ids[start:end])


UNBOUNDED = _UNBOUNDED


def split_and_conjuncts(expr: Expression | None) -> list[Expression]:
    """Flatten top-level AND expressions into a list of conjuncts."""
    if expr is None:
        return []
    if isinstance(expr, LogicalExpression) and expr.op == LogicalOp.AND:
        return split_and_conjuncts(expr.left) + split_and_conjuncts(expr.right)
    return [expr]


def combine_and_conjuncts(exprs: list[Expression]) -> Expression | None:
    """Combine a list of expressions using LogicalOp.AND, or return None if empty."""
    if not exprs:
        return None
    result = exprs[0]
    for item in exprs[1:]:
        result = LogicalExpression(result, LogicalOp.AND, item)
    return result


def _eval_constant(expr: Expression) -> tuple[bool, DataValue]:
    """Evaluate a pure literal/constant arithmetic expression without row context."""
    if isinstance(expr, LiteralExpression):
        return True, expr.value
    if isinstance(expr, BinaryExpression):
        ok_l, lv = _eval_constant(expr.left)
        if not ok_l:
            return False, None
        ok_r, rv = _eval_constant(expr.right)
        if not ok_r:
            return False, None
        if lv is None or rv is None:
            return True, None
        if not is_number(lv) or not is_number(rv):
            return False, None
        ld, rd = to_decimal(lv), to_decimal(rv)
        if expr.op == ArithmeticOp.ADD:
            return True, ld + rd
        if expr.op == ArithmeticOp.SUB:
            return True, ld - rd
        if expr.op == ArithmeticOp.MUL:
            return True, ld * rd
        if expr.op == ArithmeticOp.DIV:
            if rd == 0:
                return False, None
            return True, ld / rd
    return False, None


def _match_column_name(expr: Expression, single_qualifier: str | None) -> str | None:
    if not isinstance(expr, ColumnExpression):
        return None
    if expr.table is None or expr.table == single_qualifier:
        return expr.column_name
    return None


_FLIP_OP = {
    ComparisonOp.EQ: ComparisonOp.EQ,
    ComparisonOp.GT: ComparisonOp.LT,
    ComparisonOp.GE: ComparisonOp.LE,
    ComparisonOp.LT: ComparisonOp.GT,
    ComparisonOp.LE: ComparisonOp.GE,
}


@dataclass(frozen=True, slots=True)
class IndexLookupMatch:
    """Result of matching a single-table WHERE expression against secondary indexes."""
    meta: IndexMeta
    mode: str          # 'EQ' | 'IN' | 'BETWEEN' | 'RANGE'
    detail: str
    row_ids: list[int]
    residual_expr: Expression | None


def match_single_table_index(
    where_expr: Expression | None,
    indexes: dict[str, tuple[IndexMeta, ColumnIndex]],
    single_qualifier: str | None = None,
) -> IndexLookupMatch | None:
    """Pick the highest-priority index access plan for a single-table WHERE predicate."""
    if where_expr is None or not indexes:
        return None

    conjuncts = split_and_conjuncts(where_expr)
    best: tuple[int, IndexLookupMatch] | None = None

    # First check point/IN/BETWEEN matches (each takes 1 conjunct).
    for idx_pos, conj in enumerate(conjuncts):
        if isinstance(conj, ConditionExpression) and conj.op == ComparisonOp.EQ:
            col = _match_column_name(conj.left, single_qualifier)
            ok, val = _eval_constant(conj.right) if col is not None else (False, None)
            if col is None or not ok:
                col = _match_column_name(conj.right, single_qualifier)
                ok, val = _eval_constant(conj.left) if col is not None else (False, None)
            if col is not None and ok and col in indexes:
                meta, col_idx = indexes[col]
                row_ids = col_idx.lookup_eq(val)
                if row_ids is not None:
                    rest = combine_and_conjuncts([c for j, c in enumerate(conjuncts) if j != idx_pos])
                    match = IndexLookupMatch(
                        meta=meta,
                        mode='EQ',
                        detail=f'{meta.name}({col} = {val!r})',
                        row_ids=row_ids,
                        residual_expr=rest,
                    )
                    best = (4, match)
                    break

        elif isinstance(conj, InListExpression) and not conj.not_in:
            col = _match_column_name(conj.expr, single_qualifier)
            if col is not None and col in indexes:
                vals: list[object] = []
                all_const = True
                for item in conj.items:
                    ok, val = _eval_constant(item)
                    if not ok:
                        all_const = False
                        break
                    vals.append(val)
                if all_const:
                    meta, col_idx = indexes[col]
                    row_ids = col_idx.lookup_in(vals)
                    if row_ids is not None:
                        rest = combine_and_conjuncts([c for j, c in enumerate(conjuncts) if j != idx_pos])
                        match = IndexLookupMatch(
                            meta=meta,
                            mode='IN',
                            detail=f'{meta.name}({col} IN [{len(vals)}])',
                            row_ids=row_ids,
                            residual_expr=rest,
                        )
                        if best is None or 3 > best[0]:
                            best = (3, match)

        elif isinstance(conj, BetweenExpression) and not conj.negated:
            col = _match_column_name(conj.expr, single_qualifier)
            if col is not None and col in indexes:
                ok_l, low = _eval_constant(conj.low)
                ok_h, high = _eval_constant(conj.high)
                if ok_l and ok_h:
                    meta, col_idx = indexes[col]
                    row_ids = col_idx.lookup_range(low, True, high, True)
                    if row_ids is not None:
                        rest = combine_and_conjuncts([c for j, c in enumerate(conjuncts) if j != idx_pos])
                        match = IndexLookupMatch(
                            meta=meta,
                            mode='BETWEEN',
                            detail=f'{meta.name}({col} BETWEEN {low!r} AND {high!r})',
                            row_ids=row_ids,
                            residual_expr=rest,
                        )
                        if best is None or 2 > best[0]:
                            best = (2, match)

    if best is not None and best[0] >= 3:
        return best[1]

    # Collect comparison range bounds per indexed column (combining e.g. col >= 10 AND col < 20).
    col_bounds: dict[str, list[tuple[int, ComparisonOp, DataValue]]] = defaultdict(list)
    for idx_pos, conj in enumerate(conjuncts):
        if not isinstance(conj, ConditionExpression):
            continue
        if conj.op not in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.LT, ComparisonOp.LE):
            continue
        col = _match_column_name(conj.left, single_qualifier)
        op = conj.op
        ok, val = _eval_constant(conj.right) if col is not None else (False, None)
        if col is None or not ok:
            col = _match_column_name(conj.right, single_qualifier)
            op = _FLIP_OP[conj.op]
            ok, val = _eval_constant(conj.left) if col is not None else (False, None)
        if col is not None and ok and col in indexes:
            col_bounds[col].append((idx_pos, op, val))

    for col, entries in col_bounds.items():
        meta, col_idx = indexes[col]
        low: object = _UNBOUNDED
        inc_low = True
        high: object = _UNBOUNDED
        inc_high = True
        used_positions: list[int] = []
        type_error = False

        for pos, op, val in entries:
            if val is not None and _coerce_key(val, col_idx.col_type) is None:
                type_error = True
                break
            used_positions.append(pos)
            if op in (ComparisonOp.GT, ComparisonOp.GE):
                is_inc = (op == ComparisonOp.GE)
                if low is _UNBOUNDED:
                    low, inc_low = val, is_inc
                elif val is None or low is None:
                    low = None
                else:
                    lk = _coerce_key(low, col_idx.col_type)
                    vk = _coerce_key(val, col_idx.col_type)
                    assert lk is not None and vk is not None
                    if vk > lk or (vk == lk and not is_inc):
                        low, inc_low = val, is_inc
            else:
                is_inc = (op == ComparisonOp.LE)
                if high is _UNBOUNDED:
                    high, inc_high = val, is_inc
                elif val is None or high is None:
                    high = None
                else:
                    hk = _coerce_key(high, col_idx.col_type)
                    vk = _coerce_key(val, col_idx.col_type)
                    assert hk is not None and vk is not None
                    if vk < hk or (vk == hk and not is_inc):
                        high, inc_high = val, is_inc

        if type_error or not used_positions:
            continue

        row_ids = col_idx.lookup_range(low, inc_low, high, inc_high)
        if row_ids is None:
            continue

        prio = 2 if (low is not _UNBOUNDED and high is not _UNBOUNDED) else 1
        used_set = set(used_positions)
        rest = combine_and_conjuncts([c for j, c in enumerate(conjuncts) if j not in used_set])
        parts = []
        if low is not _UNBOUNDED:
            parts.append(f'{col} {">=" if inc_low else ">"} {low!r}')
        if high is not _UNBOUNDED:
            parts.append(f'{col} {"<=" if inc_high else "<"} {high!r}')
        match = IndexLookupMatch(
            meta=meta,
            mode='RANGE',
            detail=f'{meta.name}({" AND ".join(parts)})',
            row_ids=row_ids,
            residual_expr=rest,
        )
        if best is None or prio > best[0]:
            best = (prio, match)

    return best[1] if best is not None else None
