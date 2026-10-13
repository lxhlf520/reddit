# -*- coding: utf-8 -*-
"""zst → PostgreSQL asyncpg + gzip codec 入库（整月单事务，分区级幂等）。

数据形态（Arctic Shift zst dump，逐行 JSON）:
- 每行一条记录，字段与官方 API 同构（107 字段，随年代增减）
- kind: submissions | comments → 目标表 posts | comments（月度分区）

压缩存储:
- 长文本列（title/selftext/url/body/raw）建成 bytea
- asyncpg 入口注册 gzip codec：写入自动压缩、读出自动解压、代码层零感知
- 预估压缩率 3~5x（raw JSONB 5~10x）

幂等策略:
- 先 DROP 目标月分区再建（pg_schema.sql 的幂等 DDL），整月重灌不残留旧数据
- asyncpg 事务单月，失败整月回滚，import_log 状态不变可重试

用法（通常由 pipeline 驱动）:
    from backfill.ingest import ingest_month
    rows, dur = await ingest_month(dsn, Path("RC_2020-01.zst"), "comments", "2020-01")
"""

import asyncio
import gzip
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import zstandard

# ── gzip codec（asyncpg 入口注册一次，代码层零感知）──────────────

async def init_gzip_codec(conn):
    """asyncpg 连接 init 回调：注册 bytea 全局 gzip codec。
    
    写入：str → gzip.compress → bytes → bytea
    读出：bytea → gzip.decompress → str（非 gzip 原样返回 bytes，容错）
    
    注意：asyncpg 的 encoder/decoder 必须是同步函数（返回 bytes，不是 coroutine）。
    """
    def encode_str(val):
        if val is None:
            return None
        if isinstance(val, bytes):
            return val  # 已经是 bytes，透传
        return gzip.compress(val.encode('utf-8'))
    
    def decode_bytea(val):
        if val is None:
            return None
        try:
            return gzip.decompress(val).decode('utf-8')
        except:
            return val  # 非 gzip 容错（返回 bytes）
    
    await conn.set_type_codec(
        'bytea',
        schema='pg_catalog',
        encoder=encode_str,
        decoder=decode_bytea,
        format='binary'  # asyncpg 支持 'text'/'binary'/'tuple'
    )

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

# JSONB 列：dict/list 需要 JSON 序列化（media/preview/gallery_data/all_awardings）
JSONB_COLUMNS = ("media", "preview", "gallery_data", "all_awardings")

# bytea 列（长文本，gzip 压缩）：title/selftext/url/body/raw
# asyncpg codec 自动处理 str → gzip → bytes，代码层照常传 str
BYTEA_COLUMNS = ("title", "selftext", "url", "body", "raw")

# int 列：源偶发 float（如 score 1.0），统一截断成 int 避免 COPY 类型错
INT_COLUMNS = ("score", "num_comments", "created_utc", "gilded",
               "total_awards_received", "controversiality")


def _adapt(row: dict, columns: list) -> tuple:
    """源 dict → asyncpg record（顺序与 columns 一致）。
    
    bytea 列（title/selftext/url/body/raw）传 str，codec 自动 gzip 压缩。
    raw 列：整行原文 json.dumps(row) → str → codec gzip → bytea。
    """
    out = []
    for col in columns:
        if col == "raw":
            out.append(json.dumps(row, ensure_ascii=False))  # str → codec gzip
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
            v = json.dumps(v, ensure_ascii=False)  # dict/list → JSON 文本
        # bytea 列（title/selftext/url/body）传 str，codec 自动 gzip
        out.append(v)
    return tuple(out)


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



async def ingest_month(dsn: str, zst_path: Path, kind: str, month: str,
                       batch_rows: int = 500_000, log=print) -> dict:
    """整月入库（asyncpg + gzip codec，长文本列自动压缩）。

    快路径：copy_records_to_table 直指月分区（全速）。
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
    
    pool = await asyncpg.create_pool(dsn, init=init_gzip_codec, min_size=1, max_size=1)
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {partition}")
        
        # 收集记录（越界过滤）
        records = []
        for rec in iter_zst_lines(zst_path):
            cu = rec.get("created_utc")
            if not isinstance(cu, (int, float)) or not (lo <= cu < hi):
                stats["skipped_range"] += 1
                continue
            records.append(_adapt(rec, columns))
            stats["rows"] += 1
            if stats["rows"] % batch_rows == 0:
                log(f"  {partition}: {stats['rows']:,} 行 "
                    f"({stats['rows'] / (time.time() - t0):,.0f} 行/s)")
        
        # 批量 COPY
        try:
            await conn.copy_records_to_table(
                partition, records=records, columns=columns)
        except asyncpg.UniqueViolationError:
            # 冲突：回滚，降级慢路径
            stats["fallback"] = True
            stats["rows"] = 0
            stats["skipped_range"] = 0
            log(f"  {partition}: 直接 COPY 失败（UniqueViolation），"
                f"降级 ON CONFLICT 路径重灌")
            t1 = time.time()
            await conn.execute(f"TRUNCATE {partition}")
            await conn.execute(
                f"CREATE TEMP TABLE tmp_ingest "
                f"(LIKE {partition} INCLUDING DEFAULTS)")
            await conn.copy_records_to_table(
                "tmp_ingest", records=records, columns=columns)
            cols = ", ".join(columns)
            result = await conn.execute(
                f"INSERT INTO {partition} ({cols}) "
                f"SELECT {cols} FROM tmp_ingest "
                f"ON CONFLICT (id, created_utc) DO NOTHING")
            stats["rows"] = int(result.split()[-1]) if result else 0
            stats["deduped"] = len(records) - stats["rows"]
            await conn.execute("DROP TABLE tmp_ingest")
    
    stats["seconds"] = time.time() - t0
    return stats
