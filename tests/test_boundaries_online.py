# -*- coding: utf-8 -*-
"""触网边界探针（默认跳过）：固化 2026-09-25 实测的 Reddit 边界响应契约。

运行（PowerShell）：
  $env:RUN_ONLINE='1'; uv run pytest -m online -v

前提：cookies.json 有效（loid/edgebucket）。全套约消耗 6 个请求配额。
实测契约（curl_cffi chrome 指纹 + 会话 cookie）：
- 不存在的子版块  → 200 + 空 Listing（dist=0, after=None）——不是 404
- 不存在的用户    → 404 JSON {"message", "error"}
- 不存在的帖子    → 404 JSON
- limit=105      → 服务端钳到 100（dist=100）
- morechildren 垃圾 children → 200 {json:{errors:[],data:{things:[]}}}
（things 在 json.data 下，与登出态同形）
"""
import asyncio
import os

import pytest

from reddit_collector import cookies as cookies_mod, db as dbm, parsers
from reddit_collector.client import RedditClient, RedditNotFound
from reddit_collector.scrapers import listings, posts, users

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(not os.environ.get("RUN_ONLINE"),
                       reason="需 RUN_ONLINE=1（消耗请求配额）"),
    pytest.mark.skipif(not cookies_mod.has_cookies(),
                       reason="缺少有效 cookies.json（loid/edgebucket）"),
]

NONEXIST_SUB = "zzz_no_such_sub_9x7q2"
NONEXIST_USER = "zzz_no_user_9x7q2"
NONEXIST_POST = "t3_zzzzzzz"


def test_online_nonexistent_sub_returns_200_empty_listing():
    """契约：不存在的子版块不是 404，而是 200 空 Listing。"""
    async def m():
        async with RedditClient() as client:
            data = await client.get_json(f"/r/{NONEXIST_SUB}/hot.json",
                                         params={"limit": 5, "raw_json": 1})
            children, after = parsers.listing_children(data)
            assert children == []
            assert after is None
    asyncio.run(m())


def test_online_nonexistent_sub_listing_collects_zero(tmp_path):
    """采集器层：空 Listing → 1 次请求、collected=0、进度 done（不死循环）。"""
    conn = dbm.get_conn(str(tmp_path / "t.db"))
    dbm.init_db(conn)
    try:
        async def m():
            async with RedditClient() as client:
                return await listings.scrape_listing(client, conn, NONEXIST_SUB,
                                                     "hot", limit=10)
        summary = asyncio.run(m())
        assert summary["collected"] == 0
        assert dbm.get_progress(conn, f"list:{NONEXIST_SUB}:hot")["state"] == "done"
    finally:
        conn.close()


def test_online_nonexistent_user_soft_skip(tmp_path):
    """契约：不存在用户 → 404；not_found_ok 软跳过，进度 failed。"""
    conn = dbm.get_conn(str(tmp_path / "t.db"))
    dbm.init_db(conn)
    try:
        async def m():
            async with RedditClient() as client:
                # 原始 404 语义
                with pytest.raises(RedditNotFound):
                    await client.get_json(f"/user/{NONEXIST_USER}/about.json",
                                          params={"raw_json": 1})
                # not_found_ok 语义
                assert await client.get_json(
                    f"/user/{NONEXIST_USER}/about.json",
                    params={"raw_json": 1}, not_found_ok=True) is None
                return await users.scrape_user(client, conn, NONEXIST_USER)
        summary = asyncio.run(m())
        assert summary["skipped"] is True
        assert dbm.get_progress(conn, f"user:{NONEXIST_USER}")["state"] == "failed"
        assert conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0
    finally:
        conn.close()


def test_online_nonexistent_post_404(tmp_path):
    """契约：不存在帖子 → 404 → RedditNotFound，进度 failed。"""
    conn = dbm.get_conn(str(tmp_path / "t.db"))
    dbm.init_db(conn)
    try:
        async def m():
            async with RedditClient() as client:
                return await posts.scrape_post(client, conn, NONEXIST_POST)
        with pytest.raises(RedditNotFound):
            asyncio.run(m())
        assert dbm.get_progress(conn, f"comments:{NONEXIST_POST}")["state"] == "failed"
    finally:
        conn.close()


def test_online_server_caps_listing_limit_at_100():
    """契约：limit=105 服务端钳到 100（客户端此前已自行钳制）。"""
    async def m():
        async with RedditClient() as client:
            data = await client.get_json("/r/popular/hot.json",
                                         params={"limit": 105, "raw_json": 1})
            children, _ = parsers.listing_children(data)
            assert 0 < len(children) <= 100
    asyncio.run(m())


def test_online_morechildren_bogus_ids_return_empty():
    """契约：morechildren 传垃圾 children → 200 且 things 为空（与登出态同形）。
    结构为 {json:{errors:[],data:{things:[...]}}} —— things 在 json.data 下。"""
    async def m():
        async with RedditClient() as client:
            data = await client.get_json("/api/morechildren.json", params={
                "api_type": "json", "link_id": "t3_1wpfoi4",
                "children": "aaa,bbb", "sort": "top", "raw_json": 1})
            inner = ((data or {}).get("json") or {}).get("data") or {}
            assert inner.get("things") == []
    asyncio.run(m())
