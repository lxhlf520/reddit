# -*- coding: utf-8 -*-
"""Reddit thing 归一化：Listing / t1 / t2 / t3 / t5 → 数据库行字典。

Reddit 数据模型（实测确认）：
- Listing：{kind:"Listing", data:{children:[...], after, before, dist}}
- t3：帖子/链接；t1：评论；t2：用户；t5：子版块；more：评论占位（待展开 id 列表）
- 帖子详情 {permalink}.json 返回数组 [Listing(t3), Listing(t1)]
- 评论 replies 为嵌套 Listing，需递归展开；kind=="more" 收集 id 交给 /api/morechildren
"""
import json
from typing import Optional


def _dumps(obj) -> Optional[str]:
    if obj is None:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False)
    except (TypeError, ValueError):
        return None


def _b36(name: Optional[str]) -> Optional[str]:
    """去掉 t1_/t3_ 前缀，取 base36 id。"""
    if not name:
        return None
    return name.split("_", 1)[1] if "_" in name else name


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------
def listing_children(listing: dict) -> tuple[list, Optional[str]]:
    """从 Listing 取 (children, after)。"""
    data = (listing or {}).get("data") or {}
    return data.get("children") or [], data.get("after")


# ---------------------------------------------------------------------------
# t3 帖子
# ---------------------------------------------------------------------------
def parse_post(data: dict) -> dict:
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "subreddit": data.get("subreddit"),
        "author": data.get("author"),
        "title": data.get("title"),
        "selftext": data.get("selftext"),
        "score": data.get("score"),
        "upvote_ratio": data.get("upvote_ratio"),
        "num_comments": data.get("num_comments"),
        "created_utc": data.get("created_utc"),
        "permalink": data.get("permalink"),
        "url": data.get("url_overridden_by_dest") or data.get("url"),
        "domain": data.get("domain"),
        "link_flair_text": data.get("link_flair_text"),
        "is_gallery": 1 if data.get("is_gallery") else 0,
        "over_18": 1 if data.get("over_18") else 0,
        "stickied": 1 if data.get("stickied") else 0,
        "media_metadata": _dumps(data.get("media_metadata")),
        "raw_json": _dumps(data),
    }


def parse_posts(children: list) -> list[dict]:
    """从 Listing children 提取所有 t3 帖子行。"""
    return [parse_post(c["data"]) for c in children
            if c.get("kind") == "t3" and c.get("data")]


# ---------------------------------------------------------------------------
# t1 评论（递归展开 replies，收集 more 占位 id）
# ---------------------------------------------------------------------------
def parse_comment(data: dict, depth: int = 0) -> dict:
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "link_id": data.get("link_id"),
        "parent_id": data.get("parent_id"),
        "author": data.get("author"),
        "body": data.get("body"),
        "score": data.get("score"),
        "created_utc": data.get("created_utc"),
        "depth": depth,
        "subreddit": data.get("subreddit"),
        "raw_json": _dumps(data),
    }


def walk_comments(children: list, depth: int = 0) -> tuple[list[dict], list[str], int]:
    """递归遍历评论树。

    返回 (comment_rows, more_ids, blind_more)：
    - comment_rows：所有 t1 评论行（含 depth）
    - more_ids：所有 kind=="more" 占位里的 base36 评论 id（可交 /api/morechildren 展开）
    - blind_more：count>0 但未提供 children id 的 more 占位数（登出态无法展开，计为截断）
    """
    rows: list[dict] = []
    more_ids: list[str] = []
    blind = 0
    for child in children or []:
        kind = child.get("kind")
        data = child.get("data") or {}
        if kind == "t1":
            rows.append(parse_comment(data, depth))
            replies = data.get("replies")
            if isinstance(replies, dict):
                sub_children = (replies.get("data") or {}).get("children") or []
                sub_rows, sub_more, sub_blind = walk_comments(sub_children, depth + 1)
                rows.extend(sub_rows)
                more_ids.extend(sub_more)
                blind += sub_blind
        elif kind == "more":
            ids = data.get("children") or []
            if ids:
                more_ids.extend(ids)
            elif (data.get("count") or 0) > 0:
                blind += 1
    return rows, more_ids, blind


# ---------------------------------------------------------------------------
# t2 用户
# ---------------------------------------------------------------------------
def parse_user(data: dict) -> dict:
    return {
        "name": data.get("name"),
        "id": data.get("id"),
        "link_karma": data.get("link_karma"),
        "comment_karma": data.get("comment_karma"),
        "created_utc": data.get("created_utc"),
        "is_employee": 1 if data.get("is_employee") else 0,
        "has_verified_email": 1 if data.get("has_verified_email") else 0,
        "raw_json": _dumps(data),
    }


# ---------------------------------------------------------------------------
# t5 子版块
# ---------------------------------------------------------------------------
def parse_subreddit(data: dict) -> dict:
    return {
        "display_name": data.get("display_name"),
        "id": data.get("id"),
        "subscribers": data.get("subscribers"),
        "title": data.get("title"),
        "public_description": data.get("public_description"),
        "raw_json": _dumps(data),
    }
