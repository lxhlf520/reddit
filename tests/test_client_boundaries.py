# -*- coding: utf-8 -*-
"""client 层边界（离线）：未启动守卫、异常层级、JSON 解析、限流头、代理池轮换。"""
import asyncio

import pytest

from reddit_collector import config
from reddit_collector.client import (RedditAuthError, RedditClient,
                                     RedditClientError, RedditNotFound,
                                     RedditRateLimited)
from reddit_collector.proxy import ProxyPool


def test_get_json_requires_started_session():
    """未 start() 就调用必须显式报错（守卫），而非 AttributeError。"""
    async def m():
        client = RedditClient()
        with pytest.raises(RedditClientError, match="未启动"):
            await client.get_json("/r/x/hot.json")
    asyncio.run(m())


def test_exception_hierarchy():
    for exc in (RedditNotFound, RedditAuthError, RedditRateLimited):
        assert issubclass(exc, RedditClientError)


def test_ratelimit_reset_header_parsing():
    assert RedditClient._ratelimit_reset({}) == config.RATE_LIMIT_429_BACKOFF
    assert RedditClient._ratelimit_reset({"x-ratelimit-reset": "120"}) == 120.0
    assert RedditClient._ratelimit_reset({"x-ratelimit-reset": "abc"}) \
        == config.RATE_LIMIT_429_BACKOFF
    assert RedditClient._ratelimit_reset({"x-ratelimit-reset": "-5"}) == 0.0


class _FakeResp:
    def __init__(self, payload=None, text="", json_raises=False):
        self._payload = payload
        self.text = text
        self._json_raises = json_raises

    def json(self):
        if self._json_raises:
            raise ValueError("expecting value")
        return self._payload


def test_parse_json_ok():
    assert RedditClient._parse_json(_FakeResp(payload={"a": 1}), "u") == {"a": 1}


def test_parse_json_html_block_page_raises():
    """200 但返回 HTML（拦截页）→ 报"非 JSON"错误且带正文片段。"""
    with pytest.raises(RedditClientError) as ei:
        RedditClient._parse_json(
            _FakeResp(json_raises=True, text="<html>blocked</html>"), "http://u")
    assert "非 JSON" in str(ei.value) and "blocked" in str(ei.value)


def test_stats_defaults():
    client = RedditClient()
    stats = client.stats
    assert stats["requests_made"] == 0 and stats["retries"] == 0
    assert stats["oauth"] is False and stats["server_remaining"] is None


def test_client_limiter_syncs_server_headers():
    client = RedditClient()
    client.limiter.sync_headers({"x-ratelimit-remaining": "7"})
    assert client.limiter.server_remaining == 7.0
    assert client.limiter._tokens <= 7.0  # 服务端配额夹紧本地令牌


def test_explicit_proxy_overrides_pool_and_system():
    pool = ProxyPool(["http://pool1", "http://pool2"])
    client = RedditClient(proxy="http://explicit", proxy_pool=pool)
    assert client.proxy == "http://explicit"
    client2 = RedditClient(proxy_pool=pool)
    assert client2.proxy == "http://pool1"


def test_proxy_pool_rotation_skips_bad_and_exhausts():
    pool = ProxyPool(["http://a", "http://b", "http://c"])
    assert pool.size == 3 and pool.current == "http://a"
    pool.mark_bad("http://a")
    assert pool.rotate() == "http://b"
    pool.mark_bad("http://b")
    assert pool.rotate() == "http://c"
    pool.mark_bad("http://c")
    assert pool.rotate() is None          # 全部标记为坏 → 无可用出口


def test_single_proxy_pool_never_rotates():
    assert ProxyPool(["http://only"]).rotate() is None
    assert ProxyPool([]).current is None
