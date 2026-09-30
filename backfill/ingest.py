# -*- coding: utf-8 -*-
"""zst → PostgreSQL COPY 入库（整月单事务，分区级幂等）。

数据形态（Arctic Shift zst dump，逐行 JSON）:
- 每行一条记录，字段与官方 API 同构（107 字段，随年代增减）
- kind: submissions | comments → 目标表 posts | comments（月度分区）

幂等策略:
- 先 DROP 目标月分区再建（pg_schema.sql 的幂等 DDL），整月重灌不残留旧数据
- COPY FROM STDIN 单事务，失败整月回滚，import_log 状态不变可重试

用法（通常由 pipeline 驱动）:
    from backfill.ingest import ingest_month
    rows, dur = ingest_month(dsn, Path("RC_2020-01.zst"), "comments", "2020-01")
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import zstandard
from psycopg.types.json import Json

# ── 列映射（zst JSON 字段 → 表列；None = 列必须存在但源可缺）─────

POST_COLUMNS = [
    "id", "author", "subreddit", "title", "selftext", "url", "domain",
    "permalink", "score", "upvote_ratio", "num_comments", "created_utc",
    "edited", "over_18", "spoiler", "locked", "archived", "pinned",
    "quarantine", "is_self", "is_video", "distinguished", "gilded",
    "total_awards_received", "removed_by_category", "link_flair_text",
    "author_flair_text", "media", "preview", "gallery_data", "all_awardings",
    "raw",
]

COMMENT_COLUMNS = [
    "id", "author", "subreddit", "body", "score", "created_utc", "edited",
    "link_id", "parent_id", "is_submitter", "stickied", "locked",
    "controversiality", "distinguished", "gilded", "total_awards_received",
    "author_flair_text", "removed_by_category", "raw",
]

# JSON 里带 "t3_xxx"/"t1_xxx" 前缀的 id 字段 → 存裸 id
STRIP_PREFIX = ("id", "link_id", "parent_id")

# edited 可能是 bool 或 epoch 秒（Reddit 历史口径变化），统一转 timestamptz
TS_FIELDS = ("edited",)

# JSONB 列：dict/list 需要 Json() 包装（COPY TEXT 格式不能直接适应 dict）
JSONB_COLUMNS = ("media", "preview", "gallery_data", "all_awardings", "raw")

# int 列：源偶发 float（如 score 1.0），统一截断成 int 避免 COPY 类型错
INT_COLUMNS = ("score", "num_comments", "created_utc", "gilded",
               "total_awards_received", "controversiality")


def _adapt(row: dict, columns: list) -> list:
    """源 dict → COPY 行（顺序与 columns 一致）。"""
    out = []
    for col in columns:
        if col == "raw":
            out.append(Json(row))          # 整行原文兜底（JSONB）
            continue
        v = row.get(col)
        if col in STRIP_PREFIX and isinstance(v, str) and "_" in v:
            v = v.split("_", 1)[1]
        if col == "edited":
            # bool 是 int 子类，必须先判：True=编辑过但时间未知（raw 原文完整保留）
            if isinstance(v, bool) or not v:
                v = None
            elif isinstance(v, (int, float)):
                v = datetime.fromtimestamp(v, tz=timezone.utc)
        if col in INT_COLUMNS and isinstance(v, float):
            v = int(v)
        if col in JSONB_COLUMNS and v is not None:
            v = Json(v)                   # dict/list → JSONB 文本
        out.append(v)
    return out


def iter_zst_lines(path: Path):
    """流式逐行产出 dict（月文件 ~70GB，绝不全量载入）。"""
    with open(path, "rb") as f:
        reader = zstandard.ZstdDecompressor().stream_reader(f)
        buf = b""
        for chunk in iter(lambda: reader.read(1 << 20), b""):
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if line.strip():
                    yield json.loads(line)
        if buf.strip():
            yield json.loads(buf)


def _copy_stream(cur, partition, columns, zst_path, lo, hi,
                 batch_rows, t0, stats, log):
    """把 zst 行流 COPY 进目标表；返回行数（越界行被跳过并计数）。

    越界过滤（lo <= created_utc < hi）同时解决两件事:
    - RANGE 分区拒绝越界行 → 一条脏数据毁掉整月 COPY
    - 重灌幂等：TRUNCATE 月分区即可，无跨月残留
    """
    cols = ", ".join(columns)
    with cur.copy(f"COPY {partition} ({cols}) FROM STDIN") as cp:
        for rec in iter_zst_lines(zst_path):
            cu = rec.get("created_utc")
            if not isinstance(cu, (int, float)) or not (lo <= cu < hi):
                stats["skipped_range"] += 1
                continue
            cp.write_row(_adapt(rec, columns))
            stats["rows"] += 1
            if stats["rows"] % batch_rows == 0:
                log(f"  {partition}: {stats['rows']:,} 行 "
                    f"({stats['rows'] / (time.time() - t0):,.0f} 行/s)")
    return stats["rows"]


def ingest_month(dsn: str, zst_path: Path, kind: str, month: str,
                 batch_rows: int = 500_000, log=print) -> dict:
    """整月入库（COPY 协议，比 executemany 快一个量级，82 亿条的必选项）。

    快路径：COPY 直指月分区（全速）。
    慢路径（自动降级）：直接 COPY 因 PK 冲突等失败时，回滚后改走
    UNLOGGED 临时表 + INSERT ... ON CONFLICT DO NOTHING（约 2 倍耗时，
    但保证 zst 内部重复 id 不再让整月死循环失败）。

    返回统计 dict：rows（入库行数）/ skipped_range（越界行数）/
    deduped（慢路径去重行数）/ fallback（是否走了慢路径）/ seconds。
    失败抛异常（整月单事务自动回滚，分区不动，import_log 可重试）。

    幂等：入库前 TRUNCATE 目标月分区（分区 DDL 由 pg_schema.sql 保证存在）。
    """
    if kind not in ("submissions", "comments"):
        raise ValueError(kind)
    table = "posts" if kind == "submissions" else "comments"
    columns = POST_COLUMNS if kind == "submissions" else COMMENT_COLUMNS
    y, m = month.split("-")
    partition = f"{table}_{y}_{m}"
    from backfill.gen_schema import month_bounds
    lo, hi = month_bounds(int(y), int(m))

    t0 = time.time()
    stats = {"rows": 0, "skipped_range": 0, "deduped": 0,
             "fallback": False, "seconds": 0.0}
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {partition}")
            try:
                _copy_stream(cur, partition, columns, zst_path, lo, hi,
                             batch_rows, t0, stats, log)
                conn.commit()
            except psycopg.Error as e:
                # 冲突/中断等：回滚半个 COPY（TRUNCATE 一并回滚），降级慢路径
                conn.rollback()
                stats["fallback"] = True
                stats["rows"] = 0
                stats["skipped_range"] = 0   # 重灌时重新计，避免双重计数
                log(f"  {partition}: 直接 COPY 失败（{type(e).__name__}），"
                    f"降级 ON CONFLICT 路径重灌")
                t1 = time.time()
                cur.execute(f"TRUNCATE {partition}")
                cur.execute(
                    f"CREATE TEMP TABLE tmp_ingest "
                    f"(LIKE {partition} INCLUDING DEFAULTS) ON COMMIT DROP")
                _copy_stream(cur, "tmp_ingest", columns, zst_path, lo, hi,
                             batch_rows, t1, stats, log)
                cols = ", ".join(columns)
                cur.execute(
                    f"INSERT INTO {partition} ({cols}) "
                    f"SELECT {cols} FROM tmp_ingest "
                    f"ON CONFLICT (id, created_utc) DO NOTHING")
                stats["deduped"] = stats["rows"] - cur.rowcount
                stats["rows"] = cur.rowcount
                conn.commit()
    stats["seconds"] = time.time() - t0
    return stats
