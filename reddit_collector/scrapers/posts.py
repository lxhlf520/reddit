# -*- coding: utf-8 -*-
"""posts 采集器：帖子详情 + 评论树（含 morechildren 展开与截断标记）。

端点：
- 帖+评论：`{permalink}.json`（limit<=500, depth, sort）→ `[Listing(t3), Listing(t1)]`；
  评论树递归展开 t1.replies；kind=="more" 占位收集 base36 id 交 morechildren。
- 展开更多：`/api/morechildren.json`（api_type=json, link_id=t3_xxx, children=逗号串, sort）
  → `{json:{errors:[],data:{things:[t1]}}}`（注意 things 在 json.data 下）。

实测坑（见 plan）：**登出态 morechildren 返回 things 为空**，无法展开深层评论。
此时把该帖进度标记为 `truncated`（而非 done），OAuth 模式下可重采补全。
"""
import asyncio
import logging
from typing import Optional

from .. import config, db, parsers
from ..client import RedditAuthError, RedditClient, RedditClientError, RedditNotFound

log = logging.getLogger("reddit.posts")


def _chunks(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


async def expand_morechildren(client: RedditClient, link_id: str,
                              more_ids: list[str], sort: Optional[str],
                              referer: Optional[str]) -> tuple[list[dict], int, bool]:
    """分批调用 /api/morechildren 展开更多评论。

    返回 (rows, requested_ids, returned_empty)：
    - returned_empty=True 表示请求了 id 但服务端返回空 things（登出态限制）。
    """
    uniq: list[str] = []
    seen = set()
    for i in more_ids:
        if i and i not in seen:
            seen.add(i)
            uniq.append(i)
    rows: list[dict] = []
    requested = 0
    returned_empty = False
    for batch in _chunks(uniq, config.MORECHILDREN_BATCH):
        requested += len(batch)
        params = {
            "api_type": "json",
            "link_id": link_id,
            "children": ",".join(batch),
            "sort": sort or config.COMMENT_SORT,
            "raw_json": 1,
        }
        data = await client.get_json("/api/morechildren.json",
                                     params=params, referer=referer)
        things = ((((data or {}).get("json") or {}).get("data")
                   or {}).get("things")) or []
        if not things:
            # 登出态 morechildren 恒返回空；早停避免空转烧配额（该帖标记 truncated）
            returned_empty = True
            break
        for th in things:
            if th.get("kind") != "t1":
                continue
            d = th.get("data") or {}
            base_depth = d.get("depth") or 0
            rows.append(parsers.parse_comment(d, base_depth))
            rep = d.get("replies")
            if isinstance(rep, dict):
                sub = (rep.get("data") or {}).get("children") or []
                sub_rows, _, _ = parsers.walk_comments(sub, base_depth + 1)
                rows.extend(sub_rows)
    return rows, requested, returned_empty


async def scrape_post(client: RedditClient, conn, name: str,
                      permalink: Optional[str] = None,
                      num_comments: Optional[int] = None,
                      sort: Optional[str] = None, depth: Optional[int] = None,
                      limit: Optional[int] = None, expand_more: bool = True,
                      force: bool = False) -> dict:
    """采集单帖详情 + 评论树，幂等入库。

    进度 scope=`comments:{name}`，state ∈ done/truncated/failed。
    """
    scope = f"comments:{name}"
    prog = db.get_progress(conn, scope)
    if prog and not force and prog["state"] == "done":
        log.info("跳过已完成 %s（--force 可重采）", scope)
        return {"scope": scope, "skipped": True, "comments": 0,
                "inserted": 0, "updated": 0, "truncated": False}

    link_id = name if name.startswith("t3_") else f"t3_{name}"
    b36 = link_id[3:]
    if permalink:
        path = permalink.rstrip("/") + ".json"
        referer = config.BASE_URL + permalink
    else:
        path = f"/comments/{b36}.json"
        referer = config.BASE_URL + f"/comments/{b36}/"

    params = {
        "limit": limit or config.COMMENT_LIMIT,
        "depth": depth or config.COMMENT_DEPTH,
        "sort": sort or config.COMMENT_SORT,
        "raw_json": 1,
    }
    db.set_progress(conn, scope, "working")
    try:
        data = await client.get_json(path, params=params, referer=referer)
        if not isinstance(data, list) or len(data) < 2:
            raise RedditClientError(f"帖子详情返回结构异常：{path}")

        # 1) 帖子正文（selftext/media 等比列表更全）
        post_children, _ = parsers.listing_children(data[0])
        post_rows = parsers.parse_posts(post_children)
        if post_rows:
            db.upsert_posts(conn, post_rows)

        # 2) 评论树
        comment_children, _ = parsers.listing_children(data[1])
        rows, more_ids, blind_more = parsers.walk_comments(comment_children, 0)
        ins, upd = db.upsert_comments(conn, rows)
        stored = len(rows)

        # 3) 展开 morechildren（登出态可能返回空）
        truncated = blind_more > 0
        requested = 0
        if more_ids and expand_more:
            extra, requested, empty = await expand_morechildren(
                client, link_id, more_ids, params["sort"], referer)
            if extra:
                ei, eu = db.upsert_comments(conn, extra)
                ins += ei
                upd += eu
                stored += len(extra)
            if empty:
                truncated = True

        state = "truncated" if truncated else "done"
        detail = (f"comments={stored},inserted={ins},updated={upd},"
                  f"more_ids={len(more_ids)},requested={requested},"
                  f"blind={blind_more},num_comments={num_comments}")
        db.set_progress(conn, scope, state, after=None, detail=detail)
        if truncated:
            log.warning("%s 评论截断（登出态 morechildren 受限）：%s", scope, detail)
        else:
            log.info("完成 %s：%s", scope, detail)
        return {"scope": scope, "comments": stored, "inserted": ins,
                "updated": upd, "truncated": truncated}
    except (RedditAuthError, RedditNotFound, RedditClientError) as e:
        db.set_progress(conn, scope, "failed", after=None, detail=str(e)[:200])
        log.error("%s 失败：%s", scope, e)
        raise


async def run(client: RedditClient, conn, limit: int = 10,
              expand_more: bool = True, force: bool = False,
              sort: Optional[str] = None,
              concurrency: Optional[int] = None) -> dict:
    """对尚未采评论的帖子（按 num_comments 降序）并发采集详情+评论。"""
    rows = db.list_posts_missing_comments(conn, limit)
    totals = {"posts": 0, "skipped": 0, "failed": 0, "truncated": 0,
              "comments": 0, "inserted": 0, "updated": 0}
    if not rows:
        log.info("没有待采评论的帖子（先跑 listings 阶段）")
        return totals

    sem = asyncio.Semaphore(concurrency or config.CONCURRENCY)

    async def one(r):
        async with sem:
            return await scrape_post(client, conn, r["name"], r["permalink"],
                                     r["num_comments"], sort=sort,
                                     expand_more=expand_more, force=force)

    results = await asyncio.gather(*[one(r) for r in rows],
                                   return_exceptions=True)
    auth_err = None
    for res in results:
        if isinstance(res, RedditAuthError):
            auth_err = auth_err or res
            continue
        if isinstance(res, BaseException):
            totals["failed"] += 1
            log.warning("帖子任务失败：%s: %s", type(res).__name__, str(res)[:160])
            continue
        totals["posts"] += 1
        if res.get("skipped"):
            totals["skipped"] += 1
        if res.get("truncated"):
            totals["truncated"] += 1
        totals["comments"] += res.get("comments", 0)
        totals["inserted"] += res.get("inserted", 0)
        totals["updated"] += res.get("updated", 0)

    if auth_err:
        raise auth_err
    log.info("posts 汇总：%s", totals)
    return totals
