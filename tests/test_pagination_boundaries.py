# -*- coding: utf-8 -*-
"""翻页极限边界（离线）：after 终止、limit 钳制、零请求、断点续采、done 跳过、空 Listing。"""
import asyncio

import pytest

from reddit_collector import db as dbm
from reddit_collector.client import RedditAuthError, RedditNotFound
from reddit_collector.scrapers import listings

from tests.helpers import FakeClient, listing_json, raise_error

SUB, SORT = "s", "hot"
SCOPE = f"list:{SUB}:{SORT}"


def _route(pages):
    """按 after 游标路由到预置页。pages: {after_or_None: listing}"""
    def handler(path, params):
        assert path == f"/r/{SUB}/{SORT}.json"
        after = params.get("after")
        assert after in pages, f"unexpected after={after!r}"
        return pages[after]
    return handler


# ---------------------------------------------------------------------------
# 翻页终止
# ---------------------------------------------------------------------------
def test_after_none_terminates_pagination(conn):
    """after=None（末页）必须终止翻页，不得死循环。"""
    pages = {
        None: listing_json(["a1", "a2"], after="t3_a2"),
        "t3_a2": listing_json(["a3"], after=None),
    }
    client = FakeClient(_route(pages))
    summary = asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=100))
    assert len(client.calls) == 2
    assert summary["collected"] == 3 and summary["inserted"] == 3
    assert dbm.get_progress(conn, SCOPE)["state"] == "done"
    assert conn.execute("SELECT COUNT(*) c FROM posts").fetchone()["c"] == 3


def test_empty_children_page_stops_immediately(conn):
    """实测契约：不存在的子版块返回 200 + 空 Listing（dist=0, after=None）。
    必须干净终止（1 次请求、collected=0、done），而不是空转或报错。"""
    client = FakeClient(lambda path, params: listing_json([], after=None))
    summary = asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=100))
    assert len(client.calls) == 1
    assert summary["collected"] == 0
    assert dbm.get_progress(conn, SCOPE)["state"] == "done"
    assert conn.execute("SELECT COUNT(*) c FROM posts").fetchone()["c"] == 0


def test_children_with_rows_but_no_after_stops(conn):
    """有数据但无 after（如只有置顶帖的极小版块）也应终止。"""
    client = FakeClient(lambda path, params: listing_json(["x1", "x2"], after=None))
    summary = asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=100))
    assert len(client.calls) == 1 and summary["collected"] == 2


# ---------------------------------------------------------------------------
# limit 钳制（服务端实测上限 100，客户端必须先钳再发）
# ---------------------------------------------------------------------------
def test_page_limit_clamped_to_100(conn):
    """limit=250 → 每页请求参数依次 100/100/50（不超 LISTING_LIMIT），共 3 页。"""
    seen_limits = []

    def handler(path, params):
        n = params["limit"]
        seen_limits.append(n)
        page = len(seen_limits)
        after = f"t3_c{page}" if page < 3 else None
        return listing_json([f"p{page}_{i}" for i in range(n)], after=after)

    client = FakeClient(handler)
    summary = asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=250))
    assert seen_limits == [100, 100, 50]
    assert len(client.calls) == 3
    assert summary["collected"] == 250 and summary["inserted"] == 250


def test_zero_limit_makes_no_requests(conn):
    """limit=0 直接完成，不发出任何请求。"""
    client = FakeClient(raise_error(AssertionError("limit=0 不应发请求")))
    summary = asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=0))
    assert client.calls == []
    assert summary["collected"] == 0
    assert dbm.get_progress(conn, SCOPE)["state"] == "done"


# ---------------------------------------------------------------------------
# 断点续传 / 幂等
# ---------------------------------------------------------------------------
def test_resume_from_working_cursor(conn):
    """working 状态存有游标时，重跑必须从 after 续采而非从头开始。"""
    dbm.set_progress(conn, SCOPE, "working", after="t3_seed")
    seen = {}

    def handler(path, params):
        seen["first_after"] = params.get("after")
        return listing_json(["n1"], after=None)

    client = FakeClient(handler)
    asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=100))
    assert seen["first_after"] == "t3_seed"


def test_done_scope_skipped_unless_force(conn):
    """done 的 scope 默认跳过（0 请求）；--force 才重采。"""
    dbm.set_progress(conn, SCOPE, "done", after=None, detail="x")
    client = FakeClient(lambda path, params: listing_json(["f1"], after=None))

    r = asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=100))
    assert r["skipped"] is True and client.calls == []

    r2 = asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=100, force=True))
    assert "skipped" not in r2 and len(client.calls) == 1


# ---------------------------------------------------------------------------
# 失败边界
# ---------------------------------------------------------------------------
def test_listing_404_marks_failed_and_reraises(conn):
    client = FakeClient(raise_error(RedditNotFound("HTTP 404")))
    with pytest.raises(RedditNotFound):
        asyncio.run(listings.scrape_listing(client, conn, SUB, SORT, limit=100))
    prog = dbm.get_progress(conn, SCOPE)
    assert prog["state"] == "failed" and "404" in (prog["detail"] or "")


def test_run_stops_all_on_auth_error(conn):
    """cookie 失效（403）是全局故障，run() 必须向上抛终止整批。"""
    client = FakeClient(raise_error(RedditAuthError("HTTP 403")))
    with pytest.raises(RedditAuthError):
        asyncio.run(listings.run(client, conn, [SUB], ["hot"], limit=10))


def test_run_counts_non_auth_failures(conn):
    """非鉴权失败不终止整批，只计入 failed。"""
    client = FakeClient(raise_error(RedditNotFound("HTTP 404")))
    totals = asyncio.run(listings.run(client, conn, [SUB], ["hot"], limit=10))
    assert totals["failed"] == 1 and totals["scopes"] == 0


def test_run_concurrent_scopes_share_conn(conn):
    """多 scope 并发共享同一连接：数据完整、无交叉污染。"""

    def handler(path, params):
        sub = path.split("/r/")[1].split("/")[0]
        return listing_json([f"{sub}_1", f"{sub}_2"], after=None)

    client = FakeClient(handler)
    totals = asyncio.run(listings.run(client, conn, ["s1", "s2"], ["hot"],
                                      limit=10, concurrency=2))
    assert totals["scopes"] == 2 and totals["collected"] == 4
    assert conn.execute("SELECT COUNT(*) c FROM posts").fetchone()["c"] == 4
