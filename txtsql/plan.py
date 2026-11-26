"""Execution plan tree, operator metrics, and token/AST serialization for EXPLAIN."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum

from .lexer import Token, TokenType
from .types import RowDict


@dataclass(frozen=True, slots=True)
class PlanNode:
    """Single operator node in a logical/physical execution plan tree."""
    operator: str
    detail: str
    optimization: str = ''
    rows_in: int | None = None
    rows_out: int | None = None
    time_ms: float | None = None
    children: tuple['PlanNode', ...] = ()


def flatten_plan_rows(node: PlanNode, analyze: bool) -> tuple[tuple[str, ...], list[RowDict]]:
    """Flatten a PlanNode tree into indented tabular rows for CLI / Treeview display."""
    rows: list[RowDict] = []

    def _walk(cur: PlanNode, depth: int) -> None:
        prefix = '' if depth == 0 else ('  ' * (depth - 1) + '└─ ')
        plan_label = f'{prefix}{cur.operator}'
        if analyze:
            if cur.rows_in is not None and cur.rows_in > 0 and cur.rows_out is not None:
                sel = f'{(cur.rows_out * 100.0) / cur.rows_in:.1f}%'
            else:
                sel = '-'
            rows.append({
                'Plan': plan_label,
                'Optimization': cur.optimization or '-',
                'Detail': cur.detail,
                'RowsIn': '-' if cur.rows_in is None else str(cur.rows_in),
                'RowsOut': '-' if cur.rows_out is None else str(cur.rows_out),
                'Selectivity': sel,
                'Time(ms)': '-' if cur.time_ms is None else f'{cur.time_ms:.3f}',
            })
        else:
            rows.append({
                'Plan': plan_label,
                'Optimization': cur.optimization or '-',
                'Detail': cur.detail,
            })
        for child in cur.children:
            _walk(child, depth + 1)

    _walk(node, 0)
    if analyze:
        cols = ('Plan', 'Optimization', 'Detail', 'RowsIn', 'RowsOut', 'Selectivity', 'Time(ms)')
    else:
        cols = ('Plan', 'Optimization', 'Detail')
    return cols, rows


def collect_plan_optimizations(node: PlanNode) -> list[str]:
    """Collect unique optimization labels present in the plan tree, preserving order."""
    seen: set[str] = set()
    ordered: list[str] = []

    def _walk(cur: PlanNode) -> None:
        if cur.optimization and cur.optimization not in seen:
            seen.add(cur.optimization)
            ordered.append(cur.optimization)
        for child in cur.children:
            _walk(child)

    _walk(node)
    return ordered


def serialize_tokens(tokens: Sequence[Token]) -> tuple[tuple[str, str, int, int], ...]:
    """Convert lexer tokens (excluding EOF) into display tuples."""
    result: list[tuple[str, str, int, int]] = []
    for tok in tokens:
        if tok.type == TokenType.EOF:
            continue
        val_str = '' if tok.value is None else str(tok.value)
        result.append((tok.type.value, val_str, tok.line, tok.column))
    return tuple(result)


def serialize_ast_node(node: object, name: str | None = None) -> dict:
    """Recursively convert an AST object into a JSON-like tree dict for visualization."""
    prefix = f'{name}: ' if name else ''
    if node is None:
        return {'label': f'{prefix}None', 'kind': 'literal', 'children': ()}
    if isinstance(node, Enum):
        return {'label': f'{prefix}{node.value}', 'kind': 'enum', 'children': ()}
    if isinstance(node, (str, int, float, bool)) or type(node).__name__ == 'Decimal':
        return {'label': f'{prefix}{node!r}', 'kind': 'literal', 'children': ()}

    if is_dataclass(node) and not isinstance(node, type):
        children: list[dict] = []
        inline_parts: list[str] = []
        for f in fields(node):
            val = getattr(node, f.name)
            if val is None or val == [] or val == () or val is False:
                continue
            if isinstance(val, (str, int, float, bool, Enum)) or type(val).__name__ == 'Decimal':
                rendered = val.value if isinstance(val, Enum) else repr(val)
                inline_parts.append(f'{f.name}={rendered}')
            elif isinstance(val, (list, tuple)):
                seq_children = [serialize_ast_node(item) for item in val]
                if seq_children:
                    children.append({
                        'label': f'{f.name} ({len(seq_children)})',
                        'kind': 'list',
                        'children': tuple(seq_children),
                    })
            else:
                children.append(serialize_ast_node(val, name=f.name))
        summary = f' ({", ".join(inline_parts)})' if inline_parts else ''
        return {
            'label': f'{prefix}{type(node).__name__}{summary}',
            'kind': 'node',
            'children': tuple(children),
        }

    if isinstance(node, (list, tuple)):
        if len(node) == 2 and not is_dataclass(node[0]):
            return {
                'label': f'{prefix}{node[0]!r} -> {node[1]!r}',
                'kind': 'pair',
                'children': (),
            }
        return {
            'label': f'{prefix}({len(node)})',
            'kind': 'list',
            'children': tuple(serialize_ast_node(item) for item in node),
        }

    return {'label': f'{prefix}{node!r}', 'kind': 'literal', 'children': ()}
