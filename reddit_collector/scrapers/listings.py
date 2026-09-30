# -*- coding: utf-8 -*-
"""listings 采集器：按种子子版块翻 hot/new/top 列表，解析 t3 幂等入库。

端点：`/r/{sub}/{sort}.json`（sort=hot/new/top/rising；top 需 t=hour/day/...）。
分页：响应 `data.after` 作为下一页游标；`limit<=100`。
断点续传：scope=`list:{sub}:{sort}`，进度写 scrape_progress（working/done/failed）。
"""
import asyncio
import logging
from typing import Optional

from .. import config, db, parsers
from ..client import RedditAuthError, RedditClient, RedditClientError, RedditNotFound

log = logging.getLogger("reddit.listings")


async def scrape_listing(client: RedditClient, conn, subreddit: str,
                         sort: str = "hot", limit: int = 100,
                         time_filter: Optional[str] = None,
                         force: bool = False) -> dict:
    """采集单个 r/{sub}/{sort} 列表，翻页至 limit 条或无 after 游标。

    返回 {scope, collected, inserted, updated, skipped?}。
    """
    scope = f"list:{subreddit}:{sort}"
    after: Optional[str] = None
    prog = db.get_progress(conn, scope)
    if prog and not force:
        if prog["state"] == "done":
            log.info("跳过已完成 scope=%s（--force 可重采）", scope)
            return {"scope": scope, "skipped": True,
                    "collected": 0, "inserted": 0, "updated": 0}
        if prog["state"] == "working" and prog["after"]:
            after = prog["after"]
            log.info("续采 scope=%s，after=%s", scope, after)

    path = f"/r/{subreddit}/{sort}.json"
    referer = f"{config.BASE_URL}/r/{subreddit}/"
    ins = upd = got = 0
    db.set_progress(conn, scope, "working", after=after)
    try:
        while got < limit:
            page_limit = min(config.LISTING_LIMIT, limit - got)
            params = {"limit": page_limit, "raw_json": 1}
            if after:
                params["after"] = after
            if sort == "top":
                params["t"] = time_filter or config.TOP_TIME_FILTER
            data = await client.get_json(path, params=params, referer=referer)
            children, next_after = parsers.listing_children(data)
            rows = parsers.parse_posts(children)
            if rows:
                i, u = db.upsert_posts(conn, rows)
                ins += i
                upd += u
                got += len(rows)
            after = next_after
            db.set_progress(conn, scope, "working", after=after,
                            detail=f"collected={got}")
            if not children or not after:
                break
        db.set_progress(conn, scope, "done", after=None,
                        detail=f"collected={got},inserted={ins},updated={upd}")
        log.info("完成 scope=%s：collected=%d inserted=%d updated=%d",
                 scope, got, ins, upd)
        return {"scope": scope, "collected": got, "inserted": ins, "updated": upd}
    except (RedditAuthError, RedditNotFound, RedditClientError) as e:
        db.set_progress(conn, scope, "failed", after=after, detail=str(e)[:200])
        log.error("scope=%s 失败：%s", scope, e)
        raise


async def run(client: RedditClient, conn, subreddits: list[str],
              sorts: Optional[list[str]] = None, limit: int = 100,
              time_filter: Optional[str] = None, force: bool = False,
              concurrency: Optional[int] = None) -> dict:
    """并发采集多个 子版块 × 排序 列表，汇总统计。

    cookie 失效（RedditAuthError）视为全局故障，立即向上抛出终止整批。
    """
    sorts = sorts or list(config.LISTING_SORTS)
    sem = asyncio.Semaphore(concurrency or config.CONCURRENCY)
    totals = {"scopes": 0, "skipped": 0, "failed": 0,
              "collected": 0, "inserted": 0, "updated": 0}

    async def one(sub: str, sort: str):
        async with sem:
            return await scrape_listing(client, conn, sub, sort, limit,
                                        time_filter, force)

    tasks = [one(sub, sort) for sub in subreddits for sort in sorts]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    auth_err = None
    for r in results:
        if isinstance(r, RedditAuthError):
            auth_err = auth_err or r
            continue
        if isinstance(r, BaseException):
            totals["failed"] += 1
            log.warning("列表任务失败：%s: %s", type(r).__name__, str(r)[:160])
            continue
        totals["scopes"] += 1
        if r.get("skipped"):
            totals["skipped"] += 1
        totals["collected"] += r.get("collected", 0)
        totals["inserted"] += r.get("inserted", 0)
        totals["updated"] += r.get("updated", 0)

    if auth_err:
        raise auth_err
    log.info("listings 汇总：%s", totals)
    return totals
