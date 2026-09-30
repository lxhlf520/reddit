# -*- coding: utf-8 -*-
"""账号边界（离线）：404 用户、封禁/空资料、[deleted] 作者过滤、cookie 缺失、重跑省配额。"""
import asyncio
import json

from reddit_collector import cookies, db as dbm, parsers
from reddit_collector.client import RedditNotFound
from reddit_collector.scrapers import users as users_scraper

from tests.helpers import FakeClient, post_data, raise_error


def _user_payload(name="alice", **extra):
    data = {"name": name, "id": "u1", "link_karma": 10, "comment_karma": 20,
            "created_utc": 1_700_000_000.0, "is_employee": False,
            "has_verified_email": True}
    data.update(extra)
    return {"kind": "t2", "data": data}


# ---------------------------------------------------------------------------
# 用户资料边界
# ---------------------------------------------------------------------------
def test_user_404_soft_skip_marks_failed(conn):
    """实测契约：不存在用户 about.json → 404。not_found_ok 下必须软跳过：
    不抛异常、不入库、进度 failed（供后续排查）。"""
    client = FakeClient(raise_error(RedditNotFound("HTTP 404")))
    summary = asyncio.run(users_scraper.scrape_user(client, conn, "ghost"))
    assert summary["skipped"] is True
    assert client.calls[0]["not_found_ok"] is True      # 必须带容错标志
    prog = dbm.get_progress(conn, "user:ghost")
    assert prog["state"] == "failed" and "404" in (prog["detail"] or "")
    assert conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0


def test_user_suspended_empty_data_skip(conn):
    """t2 存在但 data 为空（被封禁/隐藏）→ 软跳过不入库。"""
    client = FakeClient(lambda path, params: {"kind": "t2", "data": {}})
    summary = asyncio.run(users_scraper.scrape_user(client, conn, "suspended1"))
    assert summary["skipped"] is True
    assert conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0


def test_user_missing_name_field_skip(conn):
    """data 无 name 字段（异常响应）→ 软跳过，不以入参名强行落库。"""
    client = FakeClient(lambda path, params: {"kind": "t2", "data": {"id": "u9"}})
    summary = asyncio.run(users_scraper.scrape_user(client, conn, "weird1"))
    assert summary["skipped"] is True
    assert conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0


def test_user_success_upserts_and_bools_normalized(conn):
    client = FakeClient(lambda path, params: _user_payload("alice"))
    summary = asyncio.run(users_scraper.scrape_user(client, conn, "alice"))
    assert summary["inserted"] == 1
    assert dbm.get_progress(conn, "user:alice")["state"] == "done"
    row = conn.execute("SELECT * FROM users WHERE name='alice'").fetchone()
    assert row["is_employee"] == 0 and row["has_verified_email"] == 1  # bool→0/1


def test_user_second_call_skips_request(conn):
    """同一用户重复采集默认跳过（省配额），仅发 1 次请求。"""
    client = FakeClient(lambda path, params: _user_payload("bob"))
    asyncio.run(users_scraper.scrape_user(client, conn, "bob"))
    summary = asyncio.run(users_scraper.scrape_user(client, conn, "bob"))
    assert summary["skipped"] is True and len(client.calls) == 1


def test_users_run_skips_collected_authors(conn):
    """run() 层：已入库作者不产生任何请求。"""
    dbm.upsert_posts(conn, [parsers.parse_post(post_data())])  # author=alice
    first = FakeClient(lambda path, params: _user_payload("alice"))
    asyncio.run(users_scraper.run(first, conn, limit=10))
    assert len(first.calls) == 1

    second = FakeClient(raise_error(AssertionError("已采集作者不应发请求")))
    totals = asyncio.run(users_scraper.run(second, conn, limit=10))
    assert totals["authors"] == 0 and totals["skipped"] == 1 and second.calls == []


def test_user_404_in_run_counts_failed_not_crash(conn):
    """run() 层遇到 404 用户：软跳过计入 authors/skipped、不计 failed、不崩溃，
    进度落 failed 供后续排查。"""
    dbm.upsert_posts(conn, [parsers.parse_post(post_data())])
    client = FakeClient(raise_error(RedditNotFound("HTTP 404")))
    totals = asyncio.run(users_scraper.run(client, conn, limit=10))
    assert totals["authors"] == 1 and totals["skipped"] == 1
    assert totals["failed"] == 0 and totals["inserted"] == 0
    assert dbm.get_progress(conn, "user:alice")["state"] == "failed"


# ---------------------------------------------------------------------------
# 作者名单边界
# ---------------------------------------------------------------------------
def test_deleted_empty_null_authors_excluded(conn):
    """[deleted] / 空串 / NULL 作者必须被排除，只留真实用户。"""
    p_deleted = parsers.parse_post(post_data())
    p_deleted["author"] = "[deleted]"
    p_null = parsers.parse_post(post_data("p2"))
    p_null["author"] = None
    p_real = parsers.parse_post(post_data("p3"))
    c_empty = parsers.parse_comment({"id": "c9", "name": "t1_c9",
                                     "link_id": "t3_p1", "parent_id": "t3_p1",
                                     "author": "", "body": "x"})
    dbm.upsert_posts(conn, [p_deleted, p_null, p_real])
    dbm.upsert_comments(conn, [c_empty])
    assert dbm.list_distinct_authors(conn, 50) == ["alice"]


# ---------------------------------------------------------------------------
# cookie 边界
# ---------------------------------------------------------------------------
def test_has_cookies_requires_loid_and_edgebucket(tmp_path):
    """缺 loid（绕 403 的核心）即视为无有效 cookie。"""
    p = str(tmp_path / "c.json")
    cookies.save_cookies({"edgebucket": "e"}, path=p)
    assert cookies.has_cookies(p) is False
    cookies.save_cookies({"loid": "l", "edgebucket": "e"}, path=p)
    assert cookies.has_cookies(p) is True


def test_cookie_jar_legacy_flat_format(tmp_path):
    """兼容直接存 {name: value} 的旧格式。"""
    p = tmp_path / "legacy.json"
    p.write_text(json.dumps({"loid": "x"}), encoding="utf-8")
    assert cookies.load_cookie_jar(str(p))["cookies"] == {"loid": "x"}


def test_cookie_missing_file_returns_empty():
    assert cookies.load_cookies(str(__file__ + ".not-exist")) == {}
    assert cookies.has_cookies(str(__file__ + ".not-exist")) is False


def test_parse_cookie_header_edges():
    assert cookies.parse_cookie_header("") == {}
    assert cookies.parse_cookie_header("novalue") == {}
    out = cookies.parse_cookie_header("Cookie: a=b;; c=d ;")
    assert out == {"a": "b", "c": "d"}
