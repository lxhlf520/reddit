# -*- coding: utf-8 -*-
"""从 Arctic Shift HTTP API 拉取小样本直灌 PostgreSQL（轻量验收工具）。

背景:
  BT dump 的最小下载单元是整月（最小月 ~22GB），远程部署验收不必等整月下载。
  本工具用 Arctic Shift API 拉几千条（与 dump 同源、字段同构）验证全链路：
  建表 → 字段映射 → COPY 入库 → 月分区路由。
  正式全量数据仍走 pipeline.py（BT dump）；本工具不写 import_log（样本 ≠ 月份完成）。

网络: Arctic Shift 国内直连可达且更快，本工具显式空代理（禁止套代理）。

API 契约（2026-09 实测）:
- 端点 /api/{posts,comments}/search；after/before 为 epoch 秒、双开区间 (after, before)
- limit 单请求上限 100（200 及以上返回 400）；响应 {"data": [...]} 无分页元数据
- after=尾时间戳 推进会漏同秒剩余 → 本工具用「回退 1 秒 + id 去重」推进；
  仅当单秒记录数超单页上限时强制跳过（样本用途无碍，日志有提示）

用法:
  uv run python -m backfill.fetch_sample                        # comments 2000 + posts 1000
  uv run python -m backfill.fetch_sample --comments 5000 --posts 0
  uv run python -m backfill.fetch_sample --month 2022-06 --dsn postgresql://...
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import psycopg

from backfill.gen_schema import month_bounds
from backfill.ingest import COMMENT_COLUMNS, POST_COLUMNS, _adapt

API_BASE = "https://arctic-shift.photon-reddit.com/api"
PAGE_LIMIT = 100          # 实测 API 单请求上限（limit=200 起 400）
PACE_SECONDS = 0.3        # 请求间隔（礼貌 pacing；实测限流宽松）
RETRY = 3

# 显式空代理：Arctic Shift 直连可达，禁止走系统/环境代理
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _request(kind: str, after: int, before: int, timeout: int = 60) -> list:
    """单页请求（429/5xx/网络错误自动重试）。返回 data 列表。"""
    qs = urllib.parse.urlencode(
        {"after": after, "before": before, "limit": PAGE_LIMIT, "sort": "asc"})
    url = f"{API_BASE}/{kind}/search?{qs}"
    req = urllib.request.Request(
        url, headers={"User-Agent": "reddit-backfill-sample/1.0"})
    for attempt in range(1, RETRY + 1):
        try:
            with _OPENER.open(req, timeout=timeout) as r:
                return json.loads(r.read()).get("data", [])
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                wait = 5 * attempt
                print(f"    HTTP {e.code}，{wait}s 后重试（{attempt}/{RETRY}）")
                time.sleep(wait)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, OSError):
            wait = 5 * attempt
            print(f"    网络错误，{wait}s 后重试（{attempt}/{RETRY}）")
            time.sleep(wait)
    raise RuntimeError(f"请求连续失败：{url}")


def fetch_rows(kind: str, month: str, target: int) -> tuple:
    """从 month 起点向后拉 target 条（升序推进）。

    推进策略：after = 本页尾时间戳 - 1（回退 1 秒）+ id 去重，
    保证单秒记录被截断时不丢尾部；整页全重复（单秒超单页上限）时
    强制跳过该秒剩余，并记录到返回的 skipped_secs。

    返回 (rows, requests, skipped_secs)。
    """
    y, m = month.split("-")
    lo, hi = month_bounds(int(y), int(m))
    after = lo - 1                 # after 开区间：从 lo-1 起才能包含月首秒
    seen = set()
    rows = []
    requests = 0
    skipped = []
    while len(rows) < target:
        page = _request(kind, after, hi)
        requests += 1
        if not page:
            break                  # 时间窗耗尽
        fresh = 0
        for rec in page:
            rid = rec.get("id")
            if rid in seen:
                continue
            seen.add(rid)
            rows.append(rec)
            fresh += 1
        tail = page[-1]["created_utc"]
        if fresh == 0:
            skipped.append(tail)   # 单秒溢出：跳过该秒剩余
            after = tail + 1
        else:
            after = tail - 1       # 回退 1 秒，同秒尾部不丢
        if PACE_SECONDS:
            time.sleep(PACE_SECONDS)
    return rows[:target], requests, skipped


def check_partitions(dsn: str, month: str, kinds: list) -> list:
    """入库前置检查：返回缺失的目标分区表名（fail-fast）。

    PG 不可达或表未建时在拉数据前就退出，避免白拉几分钟才发现。
    """
    y, m = month.split("-")
    missing = []
    with psycopg.connect(dsn, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            for kind in kinds:
                partition = f"{kind}_{y}_{m}"
                if not cur.execute("SELECT to_regclass(%s)",
                                   (partition,)).fetchone()[0]:
                    missing.append(partition)
    return missing


def load_rows(dsn: str, kind: str, month: str, rows: list) -> tuple:
    """COPY 到临时表 → INSERT ON CONFLICT DO NOTHING（可重复运行）。

    返回 (inserted, dup, skipped_range)。
    """
    columns = POST_COLUMNS if kind == "posts" else COMMENT_COLUMNS
    y, m = month.split("-")
    partition = f"{kind}_{y}_{m}"
    lo, hi = month_bounds(int(y), int(m))
    cols = ", ".join(columns)
    inserted = skipped_range = 0
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            exists = cur.execute(
                "SELECT to_regclass(%s)", (partition,)).fetchone()[0]
            if not exists:
                raise SystemExit(
                    f"分区表 {partition} 不存在 —— 请先建表：\n"
                    f"  uv run python -m backfill.dbtool init")
            cur.execute(f"CREATE TEMP TABLE tmp_sample "
                        f"(LIKE {partition} INCLUDING DEFAULTS) "
                        f"ON COMMIT DROP")
            with cur.copy(f"COPY tmp_sample ({cols}) FROM STDIN") as cp:
                for rec in rows:
                    cu = rec.get("created_utc")
                    if not isinstance(cu, (int, float)) or not (lo <= cu < hi):
                        skipped_range += 1
                        continue
                    cp.write_row(_adapt(rec, columns))
            cur.execute(f"INSERT INTO {partition} ({cols}) "
                        f"SELECT {cols} FROM tmp_sample "
                        f"ON CONFLICT (id, created_utc) DO NOTHING")
            inserted = cur.rowcount
        conn.commit()
    return inserted, len(rows) - inserted - skipped_range, skipped_range


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(
        prog="fetch_sample",
        description="从 Arctic Shift API 拉小样本灌入 PG"
                    "（BT 整月下载前的轻量验收）")
    ap.add_argument("--comments", type=int, default=2000,
                    help="评论条数（默认 2000；0 = 跳过）")
    ap.add_argument("--posts", type=int, default=1000,
                    help="帖子条数（默认 1000；0 = 跳过）")
    ap.add_argument("--month", default="2020-01",
                    help="取样月份 YYYY-MM（默认 2020-01）")
    ap.add_argument("--dsn", default=None,
                    help="PG 连接串（默认读 .env / 环境变量 PG_DSN）")
    args = ap.parse_args(argv)

    from backfill.pipeline import load_env_cfg
    dsn = args.dsn or load_env_cfg()["dsn"]
    if not dsn:
        print("缺少 PG_DSN（--dsn、环境变量或 .env）", file=sys.stderr)
        return 2
    y, m = args.month.split("-")
    month_bounds(int(y), int(m))   # 提前校验月份格式

    # fail-fast：分区不存在 / PG 不可达时，在拉数据前退出（避免白拉）
    kinds = [k for k, n in (("comments", args.comments),
                            ("posts", args.posts)) if n > 0]
    try:
        missing = check_partitions(dsn, args.month, kinds)
    except psycopg.Error as e:
        print(f"PG 连接失败：{e}", file=sys.stderr)
        return 2
    if missing:
        print(f"分区表 {', '.join(missing)} 不存在 —— 请先建表：",
              file=sys.stderr)
        print("  uv run python -m backfill.dbtool init", file=sys.stderr)
        return 2

    print(f"目标：{args.month}  comments={args.comments}  posts={args.posts}")
    total_new = 0
    for kind, n in (("comments", args.comments), ("posts", args.posts)):
        if n <= 0:
            continue
        print(f"\n[{kind}] 拉取 {n} 条（自 {args.month} 月初起）...")
        t0 = time.time()
        rows, reqs, skipped = fetch_rows(kind, args.month, n)
        print(f"[{kind}] 拉到 {len(rows)} 条，用时 {time.time() - t0:.0f}s，"
              f"{reqs} 请求"
              + (f"，单秒溢出跳过 {len(skipped)} 秒 {skipped}" if skipped else ""))
        if not rows:
            print(f"[{kind}] 无数据，跳过入库")
            continue
        inserted, dup, skipped_range = load_rows(dsn, kind, args.month, rows)
        print(f"[{kind}] 入库完成：新增 {inserted} 条，重复跳过 {dup} 条"
              + (f"，越界过滤 {skipped_range} 条" if skipped_range else ""))
        total_new += inserted

    suffix = f"{y}_{m}"
    print(f"\n完成：共新增 {total_new} 条。验收查询（dbtool 替代 psql）：")
    print(f'  uv run python -m backfill.dbtool sql '
          f'"SELECT count(*) FROM comments_{suffix}"')
    print(f'  uv run python -m backfill.dbtool sql '
          f'"SELECT count(*) FROM posts_{suffix}"')
    print(f'  uv run python -m backfill.dbtool sql "SELECT id, author, subreddit, '
          f'score, left(body, 50) FROM comments_{suffix} LIMIT 3"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
