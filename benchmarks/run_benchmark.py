"""Reproducible benchmark suite comparing unoptimized vs optimized TxtSql execution."""

from __future__ import annotations

import os
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

# Ensure repo root is importable when invoked as a script.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from txtsql import Database


def _populate_join_tables(db: Database, n_users: int, n_orders: int) -> None:
    db.execute("CREATE TABLE bench_u (id NUMBER, tier NUMBER, tag STRING);")
    db.execute("CREATE TABLE bench_o (oid NUMBER, uid NUMBER, amount NUMBER);")
    u_rows = [
        {'id': Decimal(i), 'tier': Decimal(i % 10), 'tag': f'user_{i % 50}'}
        for i in range(n_users)
    ]
    o_rows = [
        {'oid': Decimal(i), 'uid': Decimal(i % n_users), 'amount': Decimal((i * 37) % 1000)}
        for i in range(n_orders)
    ]
    u_tbl = db.get_table('bench_u')
    o_tbl = db.get_table('bench_o')
    assert u_tbl is not None and o_tbl is not None
    u_tbl.insert_rows(u_rows)
    o_tbl.insert_rows(o_rows)


def _populate_scan_table(db: Database, n_rows: int) -> None:
    db.execute("CREATE TABLE bench_scan (id NUMBER, uid NUMBER, score NUMBER, city STRING);")
    rows = [
        {
            'id': Decimal(i),
            'uid': Decimal((i * 13) % n_rows),
            'score': Decimal((i * 29) % 1001),
            'city': f'City_{i % 20}',
        }
        for i in range(n_rows)
    ]
    tbl = db.get_table('bench_scan')
    assert tbl is not None
    tbl.insert_rows(rows)


def _measure_ms(fn, repeats: int = 3) -> float:
    fn()  # warmup
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return min(samples)


def run_benchmarks() -> None:
    print("# TxtSql 查询优化与索引性能基准实测\n")
    print("| 实验场景 | 数据规模 | 未优化耗时 (ms) | 优化后耗时 (ms) | 加速比 | 核心优化机制 |")
    print("|---|---|---|---|---|---|")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        # 1. Join: 1,000 x 1,000 and 2,000 x 2,000
        for n in (1000, 2000):
            d_join = root / f'join_{n}'
            db_opt = Database(d_join, enable_optimizations=True)
            _populate_join_tables(db_opt, n, n)
            db_no = Database(d_join, enable_optimizations=False)

            sql_j = "SELECT u.id, o.oid FROM bench_u u JOIN bench_o o ON u.id = o.uid;"
            r_opt = db_opt.execute(sql_j)
            r_no = db_no.execute(sql_j)
            assert len(r_opt.rows) == len(r_no.rows) == n

            t_no = _measure_ms(lambda: db_no.execute(sql_j), repeats=1)
            t_opt = _measure_ms(lambda: db_opt.execute(sql_j), repeats=5)
            speedup = t_no / max(t_opt, 1e-4)
            print(f"| 等值连接 (JOIN) | {n:,} × {n:,} 行 | {t_no:.2f} | {t_opt:.2f} | **{speedup:.1f}x** | Hash Join ($O(M+N)$) |")

        # 2. Index Scan vs Full Scan (10,000 rows)
        d_scan = root / 'scan_10k'
        db_s_opt = Database(d_scan, enable_optimizations=True)
        _populate_scan_table(db_s_opt, 10000)
        db_s_opt.execute("CREATE INDEX idx_scan_uid ON bench_scan (uid);")
        db_s_opt.execute("CREATE INDEX idx_scan_score ON bench_scan (score);")
        db_s_no = Database(d_scan, enable_optimizations=False)

        sql_eq = "SELECT id, score FROM bench_scan WHERE uid = 5432;"
        assert db_s_opt.execute(sql_eq).rows == db_s_no.execute(sql_eq).rows
        t_no_eq = _measure_ms(lambda: db_s_no.execute(sql_eq), repeats=3)
        t_opt_eq = _measure_ms(lambda: db_s_opt.execute(sql_eq), repeats=5)
        print(
            f"| 索引等值点查 (`=`) | 10,000 行 | {t_no_eq:.2f} | {t_opt_eq:.2f} | "
            f"**{t_no_eq / max(t_opt_eq, 1e-4):.1f}x** | Hash 二级索引 (`IndexScan`) |"
        )

        sql_rng = "SELECT id, score FROM bench_scan WHERE score BETWEEN 995 AND 1000;"
        assert db_s_opt.execute(sql_rng).rows == db_s_no.execute(sql_rng).rows
        t_no_rng = _measure_ms(lambda: db_s_no.execute(sql_rng), repeats=3)
        t_opt_rng = _measure_ms(lambda: db_s_opt.execute(sql_rng), repeats=5)
        print(
            f"| 索引范围查询 (`BETWEEN`) | 10,000 行 | {t_no_rng:.2f} | {t_opt_rng:.2f} | "
            f"**{t_no_rng / max(t_opt_rng, 1e-4):.1f}x** | 有序数组二分 (`bisect`) |"
        )

        # 3. Implicit Comma Join + Predicate Pushdown + Index (1,200 x 1,200)
        d_imp = root / 'imp_1200'
        db_i_opt = Database(d_imp, enable_optimizations=True)
        _populate_join_tables(db_i_opt, 1200, 1200)
        db_i_opt.execute("CREATE INDEX idx_u_tier ON bench_u (tier);")
        db_i_no = Database(d_imp, enable_optimizations=False)

        sql_imp = (
            "SELECT u.id, o.oid, o.amount FROM bench_u u, bench_o o "
            "WHERE u.id = o.uid AND u.tier = 9 AND o.amount >= 950;"
        )
        assert db_i_opt.execute(sql_imp).rows == db_i_no.execute(sql_imp).rows
        t_no_imp = _measure_ms(lambda: db_i_no.execute(sql_imp), repeats=1)
        t_opt_imp = _measure_ms(lambda: db_i_opt.execute(sql_imp), repeats=5)
        print(
            f"| 逗号连接 + 单表过滤 | 1,200 × 1,200 行 | {t_no_imp:.2f} | {t_opt_imp:.2f} | "
            f"**{t_no_imp / max(t_opt_imp, 1e-4):.1f}x** | 索引 + 谓词下推 + 隐式连接重写 + Hash Join |"
        )


if __name__ == '__main__':
    run_benchmarks()
