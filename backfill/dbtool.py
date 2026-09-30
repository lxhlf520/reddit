# -*- coding: utf-8 -*-
"""PostgreSQL 运维小工具（psql 客户端不可用环境的替代通道）。

远程服务器常无 psql，但本项目依赖 psycopg（libpq 绑定），
足以完成建表、就绪检查、日常查询核对。

子命令:
    init    执行 backfill/pg_schema.sql 建表（幂等，可重复执行）
    check   检查 schema 就绪状态（分区数对照 pg_schema.sql + 关键对象存在性）
    sql     执行单条 SQL 并打印结果（SELECT 打印表格，DML 打印影响行数）

用法:
    uv run python -m backfill.dbtool init
    uv run python -m backfill.dbtool check
    uv run python -m backfill.dbtool sql "SELECT count(*) FROM comments_2020_01"
    （DSN 默认读 .env / 环境变量 PG_DSN；--dsn 覆盖）
"""
import argparse
import re
import sys
from pathlib import Path

import psycopg
from psycopg import pq

HERE = Path(__file__).parent
SCHEMA_SQL = HERE / "pg_schema.sql"

_PART_RE = re.compile(
    r"CREATE TABLE IF NOT EXISTS (comments|posts)_\d{4}_\d{2} PARTITION OF")

_COUNT_SQL = (
    "SELECT split_part(relname, '_', 1) AS kind, count(*) "
    "FROM pg_class WHERE relkind = 'r' "
    "AND relname ~ '^(posts|comments)_[0-9]{4}_[0-9]{2}$' GROUP BY 1")


def _dsn(args) -> str:
    if args.dsn:
        return args.dsn
    from backfill.pipeline import load_env_cfg
    dsn = load_env_cfg()["dsn"]
    if not dsn:
        sys.exit("缺少 PG_DSN（--dsn、环境变量或 .env）")
    return dsn


def cmd_init(dsn: str) -> int:
    """执行 pg_schema.sql（多语句走 libpq 简单查询协议）。

    PQexec 语义：一批多语句在单个隐式事务中执行，任一条失败整体回滚，
    不会留下「建了一半」的 schema；全部语句为 IF NOT EXISTS，重跑幂等。
    """
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    with psycopg.connect(dsn, autocommit=True) as conn:
        res = conn.pgconn.exec_(sql.encode("utf-8"))
        if res.status not in (pq.ExecStatus.COMMAND_OK,
                              pq.ExecStatus.TUPLES_OK):
            msg = (res.error_message or b"").decode("utf-8", "replace")
            if not msg.strip():
                msg = conn.pgconn.error_message.decode("utf-8", "replace")
            print(f"建表失败（已整体回滚）：{msg.strip()}", file=sys.stderr)
            return 1
        counts = dict(conn.execute(_COUNT_SQL).fetchall())
    print("schema 已应用（幂等）：backfill/pg_schema.sql")
    print(f"  comments_* 分区：{counts.get('comments', 0)}")
    print(f"  posts_* 分区：{counts.get('posts', 0)}")
    return 0


def cmd_check(dsn: str) -> int:
    """就绪检查：分区数对照 pg_schema.sql 期望 + 关键表与默认月分区存在性。"""
    expected = {"comments": 0, "posts": 0}
    for m in _PART_RE.finditer(SCHEMA_SQL.read_text(encoding="utf-8")):
        expected[m.group(1)] += 1

    probe = ["posts", "comments", "import_log",
             "comments_2020_01", "posts_2020_01"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        actual = dict(cur.execute(_COUNT_SQL).fetchall())
        cur.execute(
            "SELECT c, to_regclass(c) IS NOT NULL "
            "FROM unnest(%s::text[]) AS c", (probe,))
        regs = dict(cur.fetchall())

    ok = True
    print("分区数（对照 pg_schema.sql 期望）：")
    for kind in ("comments", "posts"):
        n, exp = actual.get(kind, 0), expected[kind]
        hit = n == exp
        ok &= hit
        print(f"  [{'OK' if hit else '!!'}] {kind}_*：{n}/{exp}")
    print("关键对象：")
    for t in probe:
        hit = bool(regs.get(t))
        ok &= hit
        print(f"  [{'OK' if hit else '!!'}] {t}")
    if ok:
        print("schema 就绪")
    else:
        print("schema 不完整 —— 先跑："
              "uv run python -m backfill.dbtool init", file=sys.stderr)
    return 0 if ok else 1


def cmd_sql(dsn: str, query: str, limit: int) -> int:
    """执行单条 SQL 并打印结果（替代 psql -c）。"""
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            cur = conn.execute(query)
        except psycopg.Error as e:
            print(f"SQL 失败：{e}", file=sys.stderr)
            return 1
        if cur.description:
            cols = [d.name for d in cur.description]
            rows = cur.fetchmany(limit)
            print(" | ".join(cols))
            print("-" * 40)
            for r in rows:
                print(" | ".join("NULL" if v is None else str(v) for v in r))
            print(f"（{len(rows)} 行"
                  + (f"，仅显示前 {limit} 行" if len(rows) == limit else "")
                  + "）")
        else:
            print(f"OK（影响 {cur.rowcount} 行）")
    return 0


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dsn", default=None,
                        help="PG 连接串（默认读 .env / 环境变量 PG_DSN）")
    ap = argparse.ArgumentParser(
        prog="dbtool", description="PostgreSQL 运维小工具（无 psql 环境的替代）")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", parents=[common],
                   help="执行 pg_schema.sql 建表（幂等）")
    sub.add_parser("check", parents=[common],
                   help="检查 schema 就绪状态")
    p_sql = sub.add_parser("sql", parents=[common],
                           help="执行单条 SQL 并打印结果")
    p_sql.add_argument("query", help="SQL 语句（引号包裹）")
    p_sql.add_argument("--limit", type=int, default=50,
                       help="结果最多打印行数（默认 50）")
    args = ap.parse_args(argv)

    dsn = _dsn(args)
    try:
        if args.cmd == "init":
            return cmd_init(dsn)
        if args.cmd == "check":
            return cmd_check(dsn)
        return cmd_sql(dsn, args.query, args.limit)
    except psycopg.OperationalError as e:
        print(f"PG 连接失败：{e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
