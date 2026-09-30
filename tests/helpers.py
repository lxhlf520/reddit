# -*- coding: utf-8 -*-
"""测试公共构件：可编程假客户端 + Reddit thing 样例构造器。

FakeClient 镜像真实 client.get_json 的关键语义：
- 404 → handler 抛 RedditNotFound；调用方传 not_found_ok=True 时返回 None；
- 记录每次调用（path/params/not_found_ok/referer）供断言请求次数与参数钳制。
"""
from reddit_collector.client import RedditNotFound

TS = 1_700_000_000.0


class FakeClient:
    """handler(path, params) → JSON 的假客户端（不触网）。"""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    async def get_json(self, path, params=None, referer=None,
                       extra_headers=None, not_found_ok=False):
        self.calls.append({"path": path, "params": dict(params or {}),
                           "not_found_ok": not_found_ok, "referer": referer})
        try:
            return self.handler(path, dict(params or {}))
        except RedditNotFound:
            if not_found_ok:
                return None
            raise


def raise_error(exc):
    """构造一个抛指定异常的 handler。"""
    def handler(path, params):
        raise exc
    return handler


# ---------------------------------------------------------------------------
# Reddit thing 样例
# ---------------------------------------------------------------------------
def listing_json(ids, after=None, subreddit="popular"):
    """构造 Listing(t3)。ids 为 base36 帖 id 列表。"""
    children = [{
        "kind": "t3",
        "data": {"id": pid, "name": f"t3_{pid}", "subreddit": subreddit,
                 "author": "alice", "title": f"title-{pid}",
                 "num_comments": 0, "permalink": f"/r/{subreddit}/comments/{pid}/t/",
                 "created_utc": TS},
    } for pid in ids]
    return {"kind": "Listing",
            "data": {"children": children, "after": after, "dist": len(children)}}


def post_data(pid="p1"):
    return {"id": pid, "name": f"t3_{pid}", "subreddit": "s", "author": "alice",
            "title": "T", "num_comments": 10,
            "permalink": f"/r/s/comments/{pid}/t/", "created_utc": TS}


def comment(cid, parent, depth=0, replies=None):
    """构造 t1；replies=None 表示无回复（源里为 ""），传列表则嵌套 Listing。"""
    d = {"id": cid, "name": f"t1_{cid}", "link_id": "t3_p1",
         "parent_id": parent, "author": "bob", "body": f"b-{cid}",
         "score": 1, "created_utc": TS, "subreddit": "s"}
    if replies is None:
        d["replies"] = ""
    else:
        d["replies"] = {"data": {"children": replies, "after": None,
                                 "dist": len(replies)}}
    return {"kind": "t1", "data": d}


def more(ids, count=None):
    """构造 more 占位；ids=[] 且 count>0 即 blind more（登出态无法展开）。"""
    return {"kind": "more",
            "data": {"count": count if count is not None else len(ids),
                     "children": ids, "parent_id": "t3_p1"}}


def detail_json(comment_children, post=None):
    """构造 {permalink}.json 的返回：[Listing(t3), Listing(t1)]。"""
    p = post or post_data()
    return [
        {"kind": "Listing",
         "data": {"children": [{"kind": "t3", "data": p}], "after": None, "dist": 1}},
        {"kind": "Listing",
         "data": {"children": comment_children, "after": None,
                  "dist": len(comment_children)}},
    ]


def morechildren_json(ids, depth=1, parent="t1_top"):
    """构造 /api/morechildren.json 的返回 {json:{errors:[],data:{things:[t1]}}}。"""
    things = [{"kind": "t1",
               "data": {"id": cid, "name": f"t1_{cid}", "link_id": "t3_p1",
                        "parent_id": parent, "author": "bob", "body": f"b-{cid}",
                        "score": 0, "created_utc": TS, "depth": depth,
                        "subreddit": "s"}}
              for cid in ids]
    return {"json": {"errors": [], "data": {"things": things}}}
