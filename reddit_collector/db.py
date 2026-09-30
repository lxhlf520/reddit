# -*- coding: utf-8 -*-
"""SQLite 持久化层：建库建表、自然键幂等 upsert、断点续传进度。

数据模型按 Reddit thing kind 建表：
- posts        (t3)  帖子/链接
- comments     (t1)  评论（含 parent_id 树关系、depth）
- users        (t2)  用户资料
- subreddits   (t5)  子版块（种子校验用，可选）
- scrape_progress    进度（scope 粒度断点续传，幂等重跑）

所有 upsert 以自然键（Reddit base36 id / name）去重，重复采集不产生脏数据。
原始 JSON 完整保存在各表 raw_json 字段，便于后续补字段。
"""
import json
import sqlite3
import time
from typing import Iterable, Optional

from . import config


def get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    """打开（并按需初始化）SQLite 连接。启用 WAL 提升并发读写。"""
    path = db_path or config.DB_PATH
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id              TEXT PRIMARY KEY,     -- t3 base36 id（不含 t3_ 前缀）
    name            TEXT,                 -- 全名 t3_xxx
    subreddit       TEXT,
    author          TEXT,
    title           TEXT,
    selftext        TEXT,
    score           INTEGER,
    upvote_ratio    REAL,
    num_comments    INTEGER,
    created_utc     REAL,
    permalink       TEXT,
    url             TEXT,
    domain          TEXT,
    link_flair_text TEXT,
    is_gallery      INTEGER DEFAULT 0,
    over_18         INTEGER DEFAULT 0,
    stickied        INTEGER DEFAULT 0,
    media_metadata  TEXT,                 -- 图集元数据 JSON 串
    raw_json        TEXT,
    collected_at    REAL
);
CREATE INDEX IF NOT EXISTS idx_posts_subreddit   ON posts(subreddit);
CREATE INDEX IF NOT EXISTS idx_posts_author      ON posts(author);
CREATE INDEX IF NOT EXISTS idx_posts_created_utc ON posts(created_utc);

CREATE TABLE IF NOT EXISTS comments (
    id           TEXT PRIMARY KEY,        -- t1 base36 id
    name         TEXT,                    -- 全名 t1_xxx
    link_id      TEXT,                    -- 所属帖 t3_xxx
    parent_id    TEXT,                    -- 父节点 t3_xxx 或 t1_xxx
    author       TEXT,
    body         TEXT,
    score        INTEGER,
    created_utc  REAL,
    depth        INTEGER,
    subreddit    TEXT,
    raw_json     TEXT,
    collected_at REAL
);
CREATE INDEX IF NOT EXISTS idx_comments_link_id ON comments(link_id);
CREATE INDEX IF NOT EXISTS idx_comments_author  ON comments(author);

CREATE TABLE IF NOT EXISTS users (
    name               TEXT PRIMARY KEY,  -- 用户名（不含 u/ 前缀）
    id                 TEXT,              -- t2 base36 id
    link_karma         INTEGER,
    comment_karma      INTEGER,
    created_utc        REAL,
    is_employee        INTEGER DEFAULT 0,
    has_verified_email INTEGER DEFAULT 0,
    raw_json           TEXT,
    collected_at       REAL
);

CREATE TABLE IF NOT EXISTS subreddits (
    display_name       TEXT PRIMARY KEY,
    id                 TEXT,              -- t5 base36 id
    subscribers        INTEGER,
    title              TEXT,
    public_description TEXT,
    raw_json           TEXT,
    collected_at       REAL
);

CREATE TABLE IF NOT EXISTS scrape_progress (
    scope      TEXT PRIMARY KEY,          -- 如 list:{sub}:{sort} / comments:{t3_name} / user:{name}
    after      TEXT,                      -- 列表分页游标
    state      TEXT,                      -- pending/working/done/truncated/failed
    detail     TEXT,                      -- 附加信息（计数、错误摘要等）
    updated_at REAL
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    """幂等建表建索引。"""
    conn.executescript(SCHEMA)
    conn.commit()


# ---------------------------------------------------------------------------
# 通用 upsert
# ---------------------------------------------------------------------------
def _existing_pks(conn: sqlite3.Connection, table: str, pk: str,
                  values: list) -> set:
    """分块查询已存在的主键集合（用于统计 inserted/updated）。"""
    existing: set = set()
    vals = [v for v in values if v is not None]
    chunk_size = 400  # 低于 SQLite 变量上限，兼容大批量评论
    for i in range(0, len(vals), chunk_size):
        chunk = vals[i:i + chunk_size]
        qmarks = ",".join("?" * len(chunk))
        cur = conn.execute(
            f"SELECT {pk} FROM {table} WHERE {pk} IN ({qmarks})", chunk)
        existing.update(row[0] for row in cur.fetchall())
    return existing


def _upsert(conn: sqlite3.Connection, table: str, pk: str,
            columns: list[str], rows: list[tuple]) -> tuple[int, int]:
    """INSERT ... ON CONFLICT(pk) DO UPDATE。返回 (inserted, updated)。

    inserted/updated 通过 upsert 前查询已存在主键区分，供幂等重跑核对：
    同一批数据重跑应得到 inserted=0、updated=N。
    """
    rows = list(rows)
    if not rows:
        return (0, 0)
    pk_idx = columns.index(pk)
    pk_values = [r[pk_idx] for r in rows]
    existing = _existing_pks(conn, table, pk, pk_values)
    cols = ", ".join(columns)
    placeholders = ", ".join("?" * len(columns))
    updates = ", ".join(f"{c}=excluded.{c}" for c in columns if c != pk)
    sql = (f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
           f"ON CONFLICT({pk}) DO UPDATE SET {updates}")
    conn.executemany(sql, rows)
    conn.commit()
    inserted = sum(1 for v in pk_values if v not in existing)
    return (inserted, len(pk_values) - inserted)


def upsert_posts(conn: sqlite3.Connection, rows: Iterable[dict]) -> tuple[int, int]:
    cols = ["id", "name", "subreddit", "author", "title", "selftext", "score",
            "upvote_ratio", "num_comments", "created_utc", "permalink", "url",
            "domain", "link_flair_text", "is_gallery", "over_18", "stickied",
            "media_metadata", "raw_json", "collected_at"]
    now = time.time()
    data = [tuple(r.get(c) if c != "collected_at" else now for c in cols)
            for r in rows]
    return _upsert(conn, "posts", "id", cols, data)


def upsert_comments(conn: sqlite3.Connection, rows: Iterable[dict]) -> tuple[int, int]:
    cols = ["id", "name", "link_id", "parent_id", "author", "body", "score",
            "created_utc", "depth", "subreddit", "raw_json", "collected_at"]
    now = time.time()
    data = [tuple(r.get(c) if c != "collected_at" else now for c in cols)
            for r in rows]
    return _upsert(conn, "comments", "id", cols, data)


def upsert_users(conn: sqlite3.Connection, rows: Iterable[dict]) -> tuple[int, int]:
    cols = ["name", "id", "link_karma", "comment_karma", "created_utc",
            "is_employee", "has_verified_email", "raw_json", "collected_at"]
    now = time.time()
    data = [tuple(r.get(c) if c != "collected_at" else now for c in cols)
            for r in rows]
    return _upsert(conn, "users", "name", cols, data)


def upsert_subreddits(conn: sqlite3.Connection, rows: Iterable[dict]) -> tuple[int, int]:
    cols = ["display_name", "id", "subscribers", "title", "public_description",
            "raw_json", "collected_at"]
    now = time.time()
    data = [tuple(r.get(c) if c != "collected_at" else now for c in cols)
            for r in rows]
    return _upsert(conn, "subreddits", "display_name", cols, data)


# ---------------------------------------------------------------------------
# 断点续传进度
# ---------------------------------------------------------------------------
def get_progress(conn: sqlite3.Connection, scope: str) -> Optional[sqlite3.Row]:
    cur = conn.execute("SELECT * FROM scrape_progress WHERE scope=?", (scope,))
    return cur.fetchone()


def set_progress(conn: sqlite3.Connection, scope: str, state: str,
                 after: Optional[str] = None, detail: Optional[str] = None) -> None:
    conn.execute(
        """INSERT INTO scrape_progress (scope, after, state, detail, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(scope) DO UPDATE SET
             after=excluded.after, state=excluded.state,
             detail=excluded.detail, updated_at=excluded.updated_at""",
        (scope, after, state, detail, time.time()))
    conn.commit()


def list_posts_missing_comments(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    """取尚未成功采集评论的帖子（scrape_progress 中 comments:{name} 非 done）。"""
    cur = conn.execute(
        """SELECT p.* FROM posts p
           LEFT JOIN scrape_progress sp
             ON sp.scope = 'comments:' || p.name
           WHERE sp.state IS NULL OR sp.state NOT IN ('done')
           ORDER BY p.num_comments DESC
           LIMIT ?""", (limit,))
    return cur.fetchall()


def list_post_ids(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    cur = conn.execute(
        "SELECT id, name, permalink, num_comments FROM posts "
        "ORDER BY num_comments DESC LIMIT ?", (limit,))
    return cur.fetchall()


def list_distinct_authors(conn: sqlite3.Connection, limit: int) -> list[str]:
    """从 posts + comments 汇总去重作者名，供 users 阶段采集。"""
    cur = conn.execute(
        """SELECT DISTINCT author FROM (
             SELECT author FROM posts WHERE author IS NOT NULL AND author != ''
             UNION
             SELECT author FROM comments WHERE author IS NOT NULL AND author != ''
           ) WHERE author != '[deleted]'
           LIMIT ?""", (limit,))
    return [r["author"] for r in cur.fetchall()]


def is_user_collected(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT 1 FROM users WHERE name=?", (name,))
    return cur.fetchone() is not None


def stats(conn: sqlite3.Connection) -> dict:
    """库汇总统计。"""
    out = {}
    for table in ("posts", "comments", "users", "subreddits"):
        out[table] = conn.execute(f"SELECT COUNT(*) c FROM {table}").fetchone()["c"]
    prog = {}
    for row in conn.execute("SELECT state, COUNT(*) c FROM scrape_progress GROUP BY state"):
        prog[row["state"]] = row["c"]
    out["progress"] = prog
    return out


def _json_dumps(obj) -> Optional[str]:
    if obj is None:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False)
    except (TypeError, ValueError):
        return None
