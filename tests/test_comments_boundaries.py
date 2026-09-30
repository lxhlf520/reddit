# -*- coding: utf-8 -*-
"""评论边界（离线）：morechildren 批量钳制/去重/早停、截断标记、404 帖、坏结构。"""
import asyncio

import pytest

from reddit_collector import config, db as dbm, parsers
from reddit_collector.client import RedditClientError, RedditNotFound
from reddit_collector.scrapers import posts as posts_scraper

from tests.helpers import (FakeClient, comment, detail_json, more,
                           morechildren_json, post_data, raise_error)

NAME = "t3_p1"
SCOPE = f"comments:{NAME}"
PERMALINK = "/r/s/comments/p1/t/"


# ---------------------------------------------------------------------------
# morechildren 展开边界
# ---------------------------------------------------------------------------
def test_morechildren_batch_cap_and_dedup_no_early_stop():
    """不早停路径：200 个 id（含 50 重复）→ 去重 150 → 按 100/50 分批全部发完。"""
    raw_ids = [f"m{i}" for i in range(150)] + [f"m{i}" for i in range(50)]
    batches = []

    def handler(path, params):
        ids = params["children"].split(",")
        assert len(ids) <= config.MORECHILDREN_BATCH
        batches.append(ids)
        return morechildren_json(ids)  # 每批都返回 → 不触发早停

    client = FakeClient(handler)
    rows, requested, empty = asyncio.run(
        posts_scraper.expand_morechildren(client, NAME, raw_ids, "top", None))
    assert [len(b) for b in batches] == [100, 50]       # 去重后分批
    assert requested == 150 and len(rows) == 150 and empty is False


def test_morechildren_early_stop_on_empty_things():
    """200 个 id（含 50 重复）→ 去重 150 → 按 100/50 分批；
    首批返回空 things 立即早停（实测登出态恒空，省配额）。"""
    raw_ids = [f"m{i}" for i in range(150)] + [f"m{i}" for i in range(50)]
    batches = []

    def handler(path, params):
        assert path == "/api/morechildren.json"
        ids = params["children"].split(",")
        assert len(ids) <= config.MORECHILDREN_BATCH
        batches.append(ids)
        return morechildren_json([])  # 空 things → 触发早停

    client = FakeClient(handler)
    rows, requested, empty = asyncio.run(
        posts_scraper.expand_morechildren(client, NAME, raw_ids, "top", None))
    assert [len(b) for b in batches] == [100]           # 首批空即早停，第二批不发出
    assert len(set(batches[0])) == 100                  # 去重后批内无重复
    assert requested == 100
    assert empty is True and rows == []


def test_morechildren_never_exceeds_batch_cap():
    """单批 children 数量绝不超过 MORECHILDREN_BATCH（服务端上限 ~100）。"""
    raw_ids = [f"m{i}" for i in range(357)]  # 4 批：100/100/100/57
    sizes = []

    def handler(path, params):
        ids = params["children"].split(",")
        sizes.append(len(ids))
        return morechildren_json(ids)  # 每批都返回 → 走完全部批次

    client = FakeClient(handler)
    rows, requested, empty = asyncio.run(
        posts_scraper.expand_morechildren(client, NAME, raw_ids, "top", None))
    assert sizes == [100, 100, 100, 57]
    assert requested == 357 and len(rows) == 357 and empty is False


def test_morechildren_parses_depth_and_parent():
    def handler(path, params):
        return morechildren_json(params["children"].split(","))

    client = FakeClient(handler)
    rows, _, _ = asyncio.run(
        posts_scraper.expand_morechildren(client, NAME, ["m1", "m2"], "top", None))
    assert all(r["parent_id"] == "t1_top" and r["depth"] == 1 for r in rows)


# ---------------------------------------------------------------------------
# 帖子详情 + 评论树边界
# ---------------------------------------------------------------------------
def test_scrape_post_truncated_when_morechildren_empty(conn):
    """实测坑：登出态 morechildren 返回空 things → 标记 truncated（而非 done）。"""
    tree = [comment("c1", "t3_p1"), comment("c2", "t3_p1"),
            more(["m1", "m2"]),          # 可展开占位
            more([], count=5)]           # blind 占位（无 id，登出态无法展开）

    def handler(path, params):
        if "/comments/" in path and path.endswith(".json"):
            return detail_json(tree)
        if path == "/api/morechildren.json":
            return morechildren_json([])  # 登出态空返回
        raise AssertionError(f"unexpected path {path}")

    client = FakeClient(handler)
    summary = asyncio.run(
        posts_scraper.scrape_post(client, conn, NAME, permalink=PERMALINK))
    assert summary["truncated"] is True
    assert summary["comments"] == 2
    prog = dbm.get_progress(conn, SCOPE)
    assert prog["state"] == "truncated"
    assert "more_ids=2" in prog["detail"] and "blind=1" in prog["detail"]
    assert len(client.calls) == 2  # 详情 1 + morechildren 1（首批空即早停）
    assert conn.execute("SELECT COUNT(*) c FROM comments").fetchone()["c"] == 2


def test_scrape_post_done_when_no_more(conn):
    """无 more 占位 → done；默认参数 limit=500/depth=10/sort=top 必传。"""
    tree = [comment("c1", "t3_p1", replies=[comment("c1a", "t1_c1", 1)])]

    def handler(path, params):
        assert params["limit"] == config.COMMENT_LIMIT == 500
        assert params["depth"] == config.COMMENT_DEPTH == 10
        assert params["sort"] == config.COMMENT_SORT == "top"
        return detail_json(tree)

    client = FakeClient(handler)
    summary = asyncio.run(
        posts_scraper.scrape_post(client, conn, NAME, permalink=PERMALINK))
    assert summary["truncated"] is False and summary["comments"] == 2
    assert dbm.get_progress(conn, SCOPE)["state"] == "done"
    depths = {r["id"]: r["depth"] for r in conn.execute(
        "SELECT id, depth FROM comments").fetchall()}
    assert depths == {"c1": 0, "c1a": 1}   # 树深度递归正确


def test_scrape_post_404_marks_failed(conn):
    client = FakeClient(raise_error(RedditNotFound("HTTP 404")))
    with pytest.raises(RedditNotFound):
        asyncio.run(posts_scraper.scrape_post(client, conn, NAME, permalink=PERMALINK))
    assert dbm.get_progress(conn, SCOPE)["state"] == "failed"


def test_scrape_post_malformed_payload_fails(conn):
    """详情返回结构异常（非 [t3, t1] 数组）→ 报错且进度 failed。"""
    client = FakeClient(lambda path, params: {"kind": "Listing"})
    with pytest.raises(RedditClientError):
        asyncio.run(posts_scraper.scrape_post(client, conn, NAME, permalink=PERMALINK))
    assert dbm.get_progress(conn, SCOPE)["state"] == "failed"


def test_scrape_post_done_scope_skipped(conn):
    dbm.set_progress(conn, SCOPE, "done")
    client = FakeClient(raise_error(AssertionError("done scope 不应发请求")))
    summary = asyncio.run(
        posts_scraper.scrape_post(client, conn, NAME, permalink=PERMALINK))
    assert summary["skipped"] is True and client.calls == []


def test_posts_run_counts_failures(conn):
    """run() 层：404 帖计入 failed，不中断也不崩溃。"""
    dbm.upsert_posts(conn, [parsers.parse_post(post_data())])
    client = FakeClient(raise_error(RedditNotFound("HTTP 404")))
    totals = asyncio.run(posts_scraper.run(client, conn, limit=5))
    assert totals["failed"] == 1 and totals["posts"] == 0


def test_posts_run_skips_already_done(conn):
    """已 done 的帖子重跑不消耗请求。"""
    dbm.upsert_posts(conn, [parsers.parse_post(post_data())])
    dbm.set_progress(conn, SCOPE, "done")
    client = FakeClient(raise_error(AssertionError("done 帖不应发请求")))
    totals = asyncio.run(posts_scraper.run(client, conn, limit=5))
    assert totals["posts"] == 0 and client.calls == []
