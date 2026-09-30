# -*- coding: utf-8 -*-
"""生成 pg_schema.sql：posts/comments 月度 RANGE 分区 + import_log 状态机表。

用法:
    uv run python -m backfill.gen_schema [--from-month 2020-01 --to-month 2026-08]

产物 backfill/pg_schema.sql（幂等：CREATE TABLE IF NOT EXISTS + 分区 DROP 再建，
重跑只会补缺分区，不动已有数据——重灌单月靠 DROP TABLE <分区> 后重 COPY）。

表、字段均带 COMMENT ON 中文注释（口径说明），建库后用 psql 查看表结构即可读。
"""

import argparse
import calendar
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).parent

# ── 核心列（zst 全字段里的高价值子集；raw JSONB 兜底全量）───────

POST_COLS = """
    id           text        NOT NULL,
    author       text,
    subreddit    text        NOT NULL,
    title        text,
    selftext     text,
    url          text,
    domain       text,
    permalink    text,
    score        bigint,
    upvote_ratio real,
    num_comments bigint,
    created_utc  bigint      NOT NULL,
    edited       timestamptz,
    over_18      boolean,
    spoiler      boolean,
    locked       boolean,
    archived     boolean,
    pinned       boolean,
    quarantine   boolean,
    is_self      boolean,
    is_video     boolean,
    distinguished text,
    gilded       bigint,
    total_awards_received bigint,
    removed_by_category text,
    link_flair_text text,
    author_flair_text text,
    media        jsonb,
    preview      jsonb,
    gallery_data jsonb,
    all_awardings jsonb,
    raw          jsonb        NOT NULL,
    ingested_at  timestamptz  DEFAULT now(),
    PRIMARY KEY (id, created_utc)
"""

COMMENT_COLS = """
    id           text        NOT NULL,
    author       text,
    subreddit    text        NOT NULL,
    body         text,
    score        bigint,
    created_utc  bigint      NOT NULL,
    edited       timestamptz,
    link_id      text,          -- dump 偶发缺失，异常行容忍（raw 原文完整保留）而非毁整月
    parent_id    text,
    is_submitter boolean,
    stickied     boolean,
    locked       boolean,
    controversiality integer,
    distinguished text,
    gilded       bigint,
    total_awards_received bigint,
    author_flair_text text,
    removed_by_category text,
    raw          jsonb        NOT NULL,
    ingested_at  timestamptz  DEFAULT now(),
    PRIMARY KEY (id, created_utc)
"""

# ── 表/字段注释（COMMENT ON；中文口径说明，勿含 ASCII 单引号）────────

POST_TABLE_COMMENT = (
    "Reddit 帖子（submissions）主表；源为 Arctic Shift BT zst dump 归档，"
    "按 created_utc 月度 RANGE 分区，raw 列为源记录全文（唯一权威口径）"
)

POST_COL_COMMENTS = {
    "id": "Reddit base36 帖子 id（t3_ 前缀已剥离）",
    "author": "作者用户名；[deleted] = 账号已删",
    "subreddit": "板块名（无 r/ 前缀）",
    "title": "标题",
    "selftext": "自帖正文；[removed]/[deleted] 为占位符",
    "url": "帖子链接（自帖为原文链接）",
    "domain": "url 域名",
    "permalink": "站内相对永久链接",
    "score": "归档时刻分数快照（非实时值）",
    "upvote_ratio": "归档时刻赞踩比快照（非实时值）",
    "num_comments": "归档时刻评论数快照（非实时值）",
    "created_utc": "发帖时间（epoch 秒，UTC）；分区键",
    "edited": "编辑时间；NULL = 未编辑或源为 True（编辑过但时间未知，全文在 raw）",
    "over_18": "NSFW 标记",
    "spoiler": "剧透标记",
    "locked": "锁定标记",
    "archived": "归档标记",
    "pinned": "置顶标记",
    "quarantine": "板块隔离标记",
    "is_self": "是否自帖",
    "is_video": "是否视频帖",
    "distinguished": "官方身份（moderator/admin）",
    "gilded": "gold 计数（旧口径）",
    "total_awards_received": "礼物总数",
    "removed_by_category": "移除原因（如 moderator/spam）；NULL = 未移除或归档前未知",
    "link_flair_text": "链接 flair 文本",
    "author_flair_text": "作者 flair 文本",
    "media": "媒体对象（JSONB 原样）",
    "preview": "预览图对象（JSONB 原样）",
    "gallery_data": "画廊对象（JSONB 原样）",
    "all_awardings": "礼物明细（JSONB 原样）",
    "raw": "源记录 107 字段 JSON 全文（唯一权威口径；结构化列缺失时以此兜底）",
    "ingested_at": "入库时间",
}

COMMENT_TABLE_COMMENT = (
    "Reddit 评论（comments）主表；源为 Arctic Shift BT zst dump 归档，"
    "按 created_utc 月度 RANGE 分区，raw 列为源记录全文（唯一权威口径）"
)

COMMENT_COL_COMMENTS = {
    "id": "Reddit base36 评论 id（t1_ 前缀已剥离）",
    "author": "作者用户名；[deleted] = 账号已删",
    "subreddit": "板块名（无 r/ 前缀）",
    "body": "评论正文；[removed]/[deleted] 为占位符",
    "score": "归档时刻分数快照（非实时值）",
    "created_utc": "评论时间（epoch 秒，UTC）；分区键",
    "edited": "编辑时间；NULL = 未编辑或源为 True（编辑过但时间未知，全文在 raw）",
    "link_id": "所属帖子 id（t3_ 前缀已剥离）",
    "parent_id": "父对象 id（t3_ 帖子或 t1_ 评论，前缀已剥离）",
    "is_submitter": "是否楼主本人",
    "stickied": "是否置顶",
    "locked": "锁定标记",
    "controversiality": "争议度（0/1）",
    "distinguished": "官方身份（moderator/admin）",
    "gilded": "gold 计数（旧口径）",
    "total_awards_received": "礼物总数",
    "author_flair_text": "作者 flair 文本",
    "removed_by_category": "移除原因（如 moderator/spam）；NULL = 未移除或归档前未知",
    "raw": "源记录 JSON 全文（唯一权威口径；结构化列缺失时以此兜底）",
    "ingested_at": "入库时间",
}

# 建议仅对常用过滤列建本地索引（每分区独立，避免全局索引膨胀）
POST_IDX = [
    "CREATE INDEX ON {t} (subreddit, created_utc);",
    "CREATE INDEX ON {t} (author);",
]
COMMENT_IDX = [
    "CREATE INDEX ON {t} (subreddit, created_utc);",
    "CREATE INDEX ON {t} (link_id);",
    "CREATE INDEX ON {t} (author);",
]

IMPORT_LOG = """
-- 月度导入状态机（pipeline 的断点依据；重跑跳过 done）
CREATE TABLE IF NOT EXISTS import_log (
    type        text NOT NULL,          -- submissions | comments
    month       text NOT NULL,          -- 'YYYY-MM'
    status      text NOT NULL,          -- pending|downloading|downloaded|
                                        -- ingesting|verifying|done|mismatch|failed
    infohash    text,
    zst_path    text,
    zst_bytes   bigint,
    declared_bytes bigint,              -- 种子声明大小（完成检测基准）
    rows        bigint,                 -- COPY 实际行数
    expected_rows bigint,               -- stats.csv 基准
    skipped_rows bigint DEFAULT 0,      -- created_utc 越界/缺失被过滤的行数（应为 0）
    deduped_rows bigint DEFAULT 0,      -- ON CONFLICT 降级路径去重掉的行数
    retries     integer DEFAULT 0,
    error       text,
    started_at  timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (type, month)
);
-- 已建库升级（CREATE IF NOT EXISTS 不会补列，幂等 ALTER 兼容旧表）
ALTER TABLE import_log ADD COLUMN IF NOT EXISTS skipped_rows bigint DEFAULT 0;
ALTER TABLE import_log ADD COLUMN IF NOT EXISTS deduped_rows bigint DEFAULT 0;
CREATE INDEX IF NOT EXISTS import_log_status ON import_log (status);
COMMENT ON TABLE import_log IS '月度导入状态机（pipeline 断点依据；重跑跳过 status=done 的月份）';
COMMENT ON COLUMN import_log.type IS '数据类型：submissions 帖子 | comments 评论';
COMMENT ON COLUMN import_log.month IS '数据月份，YYYY-MM';
COMMENT ON COLUMN import_log.status IS '状态机：pending|downloading|downloaded|ingesting|verifying|done|mismatch|failed';
COMMENT ON COLUMN import_log.infohash IS 'BT 种子 infohash';
COMMENT ON COLUMN import_log.zst_path IS '本地 zst 文件路径';
COMMENT ON COLUMN import_log.zst_bytes IS '本地 zst 实际字节数';
COMMENT ON COLUMN import_log.declared_bytes IS '种子声明大小（完成检测基准）';
COMMENT ON COLUMN import_log.rows IS 'COPY 实际入库行数';
COMMENT ON COLUMN import_log.expected_rows IS 'stats.csv 基准行数（对账用）';
COMMENT ON COLUMN import_log.skipped_rows IS 'created_utc 越界/缺失被过滤的行数（应为 0）';
COMMENT ON COLUMN import_log.deduped_rows IS 'ON CONFLICT 降级路径去重掉的行数';
COMMENT ON COLUMN import_log.retries IS '重试次数';
COMMENT ON COLUMN import_log.error IS '失败原因（截断存储）';
COMMENT ON COLUMN import_log.started_at IS '开始时间';
COMMENT ON COLUMN import_log.finished_at IS '完成时间（done/mismatch 时写入）';
"""


def month_bounds(year: int, month: int) -> tuple:
    """月度 [start, end) epoch 秒（UTC）。"""
    start = datetime(year, month, 1)
    ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
    end = datetime(ny, nm, 1)
    epoch = datetime(1970, 1, 1)
    return int((start - epoch).total_seconds()), int((end - epoch).total_seconds())


def gen_partitions(table: str, cols: str, idx_sqls: list,
                   begin: tuple, end: tuple,
                   table_comment: str, col_comments: dict) -> list:
    """母表 + 每月分区的 DDL 列表（含表/字段 COMMENT ON 中文注释）。"""
    out = [f"CREATE TABLE IF NOT EXISTS {table} ({cols}) PARTITION BY RANGE (created_utc);",
           f"COMMENT ON TABLE {table} IS '{table_comment}';"]
    for col, cmt in col_comments.items():
        out.append(f"COMMENT ON COLUMN {table}.{col} IS '{cmt}';")
    y, m = begin
    while (y, m) <= end:
        lo, hi = month_bounds(y, m)
        p = f"{table}_{y:04d}_{m:02d}"
        out.append(
            f"CREATE TABLE IF NOT EXISTS {p} PARTITION OF {table} "
            f"FOR VALUES FROM ({lo}) TO ({hi});")
        out.append(f"COMMENT ON TABLE {p} IS '{table} {y:04d}-{m:02d} 月分区';")
        for tpl in idx_sqls:
            out.append(tpl.format(t=p))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-month", default="2020-01")
    ap.add_argument("--to-month", default="2026-08")
    args = ap.parse_args()
    begin = tuple(int(x) for x in args.from_month.split("-"))
    end = tuple(int(x) for x in args.to_month.split("-"))

    stmts = ["-- -*- sql -*- 自动生成（backfill/gen_schema.py），重跑幂等",
             "-- 生成范围: %04d-%02d ~ %04d-%02d" % (*begin, *end), ""]
    stmts += gen_partitions("posts", POST_COLS, POST_IDX, begin, end,
                            POST_TABLE_COMMENT, POST_COL_COMMENTS)
    stmts += ["", *gen_partitions("comments", COMMENT_COLS, COMMENT_IDX, begin, end,
                                  COMMENT_TABLE_COMMENT, COMMENT_COL_COMMENTS)]
    stmts += ["", IMPORT_LOG]

    out = HERE / "pg_schema.sql"
    out.write_text("\n".join(stmts) + "\n", encoding="utf-8")
    n = sum(1 for s in stmts if "PARTITION OF" in s)
    print(f"已生成 {out.name}：{n} 个月度分区（posts/comments 各半）+ import_log + 表/字段注释")


if __name__ == "__main__":
    main()
