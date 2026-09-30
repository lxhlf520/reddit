# -*- coding: utf-8 -*-
"""users 采集器：对帖子/评论作者去重后取用户资料入库。

端点：`/user/{name}/about.json` → `{kind:"t2", data:{...}}`。
已注销/被封禁用户可能 404 或返回空 data，用 not_found_ok 容错跳过。
断点续传：scope=`user:{name}`；已入库用户默认跳过（--force 重采）。
"""
import asyncio
import logging
from typing import Optional

from .. import config, db, parsers
from ..client import RedditAuthError, RedditClient, RedditClientError, RedditNotFound

log = logging.getLogger("reddit.users")


async def scrape_user(client: RedditClient, conn, name: str,
                      force: bool = False) -> dict:
    """采集单个用户资料，幂等入库。"""
    scope = f"user:{name}"
    prog = db.get_progress(conn, scope)
    if prog and not force and prog["state"] == "done":
        return {"scope": scope, "skipped": True, "inserted": 0, "updated": 0}
    if not force and db.is_user_collected(conn, name):
        db.set_progress(conn, scope, "done", detail="already-collected")
        return {"scope": scope, "skipped": True, "inserted": 0, "updated": 0}

    path = f"/user/{name}/about.json"
    referer = f"{config.BASE_URL}/user/{name}/"
    db.set_progress(conn, scope, "working")
    try:
        data = await client.get_json(path, params={"raw_json": 1},
                                     referer=referer, not_found_ok=True)
        d = (data or {}).get("data") or {}
        if not d or not d.get("name"):
            db.set_progress(conn, scope, "failed", detail="empty/404 (注销或封禁)")
            log.info("用户 %s 无资料（注销/封禁/404），跳过", name)
            return {"scope": scope, "skipped": True, "inserted": 0, "updated": 0}
        row = parsers.parse_user(d)
        row["name"] = row.get("name") or name
        ins, upd = db.upsert_users(conn, [row])
        db.set_progress(conn, scope, "done",
                        detail=f"link_karma={d.get('link_karma')},"
                               f"comment_karma={d.get('comment_karma')}")
        return {"scope": scope, "inserted": ins, "updated": upd}
    except (RedditAuthError, RedditNotFound, RedditClientError) as e:
        db.set_progress(conn, scope, "failed", detail=str(e)[:200])
        log.error("%s 失败：%s", scope, e)
        raise


async def run(client: RedditClient, conn, limit: int = 50, force: bool = False,
              concurrency: Optional[int] = None) -> dict:
    """对去重作者并发采集用户资料。"""
    authors = db.list_distinct_authors(conn, limit)
    totals = {"authors": 0, "skipped": 0, "failed": 0,
              "inserted": 0, "updated": 0}
    if not authors:
        log.info("没有可采集的作者（先跑 listings/posts 阶段）")
        return totals

    # 已入库用户默认不重复请求（省配额）
    if not force:
        pending = [a for a in authors if not db.is_user_collected(conn, a)]
        totals["skipped"] += len(authors) - len(pending)
        authors = pending
    if not authors:
        log.info("全部作者已采集（--force 可重采）")
        return totals

    sem = asyncio.Semaphore(concurrency or config.CONCURRENCY)

    async def one(name: str):
        async with sem:
            return await scrape_user(client, conn, name, force=force)

    results = await asyncio.gather(*[one(a) for a in authors],
                                   return_exceptions=True)
    auth_err = None
    for res in results:
        if isinstance(res, RedditAuthError):
            auth_err = auth_err or res
            continue
        if isinstance(res, BaseException):
            totals["failed"] += 1
            log.warning("用户任务失败：%s: %s", type(res).__name__, str(res)[:160])
            continue
        totals["authors"] += 1
        if res.get("skipped"):
            totals["skipped"] += 1
        totals["inserted"] += res.get("inserted", 0)
        totals["updated"] += res.get("updated", 0)

    if auth_err:
        raise auth_err
    log.info("users 汇总：%s", totals)
    return totals
