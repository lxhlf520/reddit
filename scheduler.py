# -*- coding: utf-8 -*-
"""持续采集调度器：按配额窗口循环跑 r/popular，实测单账号日采集容量。

每窗口（config.RATE_LIMIT_WINDOW=600s）动作：
  1. listings 轮扫：scrape_listing(force=True, limit=800) × hot / top(day) / new
     —— popular 池每排序 ≤8 页翻穿（实测三排序合计 22 请求左右），upsert 幂等去重；
  2. posts：串行采未采评论帖（按 num_comments 降序），**逐帖检查窗口请求预算**后停
     —— 大帖 morechildren 批次弹性大（一个 5k more_ids 的帖 ≈ 58 请求），
     run() 并发无法中途限流；令牌桶本身串行放行，帖间并发无收益；
  3. users：对未采集作者取 M 个（1 请求/用户）。
请求节奏由全局令牌桶（config.RATE_LIMIT_BUDGET/WINDOW，默认 90/600s）兑底，
每窗口总请求 ≤ --request-budget（默认同令牌桶预算）→ 窗口时长 ≈ 600s；
理论日上限 144 窗口 × 90 ≈ 12,960 请求。

统计全部来自 run() 返回值与 client.stats；结束（到时/中断/cookie 失效）时写
docs/capacity_report.md，回答"单账号一天能连续采集多少数据"。

用法：
  uv run python scheduler.py --hours 24
  uv run python scheduler.py --hours 0.01 --posts-per-window 2 --users-per-window 2   # 冒烟
"""
import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import datetime

from reddit_collector import config, db, proxy
from reddit_collector.client import (RedditAuthError, RedditClient,
                                     RedditClientError, RedditNotFound)
from reddit_collector.scrapers import listings, posts, users

log = logging.getLogger("reddit.scheduler")

LISTING_PAGE_CAP = 800   # 每排序单轮翻页上限（popular 池 ~8 页即翻穿，留余量）
SWEEP_SORTS = (("hot", None), ("top", "day"), ("new", None))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="scheduler.py",
        description="r/popular 持续采集调度器（配额窗口循环，输出日容量报告）")
    p.add_argument("--hours", type=float, default=24.0,
                   help="持续采集时长（小时，默认 24）")
    p.add_argument("--subreddit", default="popular", help="板块（默认 popular）")
    p.add_argument("--posts-per-window", type=int, default=40,
                   help="每窗口采评论的帖子数上限（默认 40；实际受请求预算约束）")
    p.add_argument("--users-per-window", type=int, default=10,
                   help="每窗口采用户数（默认 10）")
    p.add_argument("--request-budget", type=int, default=None,
                   help="每窗口请求预算（默认 = config.RATE_LIMIT_BUDGET，即 90）")
    p.add_argument("--no-expand-more", action="store_true",
                   help="posts 不展开 morechildren（省请求，评论变少）")
    p.add_argument("--db", default=None, help="SQLite 路径（默认 config.DB_PATH）")
    p.add_argument("--proxy", default=None, help="显式代理 URL")
    p.add_argument("--concurrency", type=int, default=None, help="并发数")
    p.add_argument("--report", default=None,
                   help="报告输出路径（默认 docs/capacity_report.md）")
    p.add_argument("--log-file", default=None,
                   help="日志文件路径（默认不落盘；建议 logs/scheduler_NNh.log）")
    p.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    return p


def _make_client(args) -> RedditClient:
    pool = None if args.proxy else proxy.build_pool()
    return RedditClient(proxy=args.proxy, use_oauth=None, proxy_pool=pool)


# ---------------------------------------------------------------------------
# 容量累计
# ---------------------------------------------------------------------------
class Meter:
    """全程容量累计器（口径：各 run() 返回值 + client.stats）。"""

    def __init__(self, t0: float, wall0: datetime):
        self.t0 = t0
        self.wall0 = wall0
        self.windows = 0
        self.total_reqs = 0
        self.listing_ins = 0
        self.listing_upd = 0
        self.post_scraped = 0
        self.post_truncated = 0
        self.posts_reqs = 0
        self.comment_ins = 0
        self.users_ins = 0
        self.users_upd = 0
        self.window_reqs: list[int] = []
        self.min_remaining = None

    def add_window(self, w: dict, client: RedditClient) -> None:
        self.windows += 1
        self.total_reqs = client.stats["requests_made"]  # client 全程从 0 计
        self.window_reqs.append(w["reqs"])
        self.listing_ins += w["listing_ins"]
        self.listing_upd += w["listing_upd"]
        self.post_scraped += w["posts"]
        self.post_truncated += w["truncated"]
        self.posts_reqs += w["posts_reqs"]
        self.comment_ins += w["comment_ins"]
        self.users_ins += w["user_ins"]
        self.users_upd += w["user_upd"]
        rem = client.stats["server_remaining"]
        if rem is not None:
            self.min_remaining = (rem if self.min_remaining is None
                                  else min(self.min_remaining, rem))


# ---------------------------------------------------------------------------
# 窗口动作
# ---------------------------------------------------------------------------
async def run_window(client: RedditClient, conn, args, win_idx: int) -> dict:
    """跑一个配额窗口：listings 轮扫 → posts → users，返回本窗口统计。"""
    req0 = client.stats["requests_made"]
    budget = args.request_budget or config.RATE_LIMIT_BUDGET
    w = {"idx": win_idx, "reqs": 0, "listing_ins": 0, "listing_upd": 0,
         "listing_detail": "", "posts": 0, "posts_failed": 0,
         "posts_reqs": 0, "comment_ins": 0, "truncated": 0,
         "users": 0, "user_ins": 0, "user_upd": 0}

    # 1) listings 轮扫（force 重扫，upsert 幂等；重复帖只计 updated）
    parts = []
    for sort, tf in SWEEP_SORTS:
        r = await listings.scrape_listing(
            client, conn, args.subreddit, sort, limit=LISTING_PAGE_CAP,
            time_filter=tf, force=True)
        w["listing_ins"] += r.get("inserted", 0)
        w["listing_upd"] += r.get("updated", 0)
        parts.append(f"{sort}:+{r.get('inserted', 0)}/{r.get('updated', 0)}")
    w["listing_detail"] = " ".join(parts)

    # 2) posts：串行 + 窗口请求预算控制（大帖 morechildren 批次弹性大，
    #    必须逐帖检查余量；帖子 404/网络错跳过计 failed，cookie 失效上抛）。
    #    只采从未采过的帖（progress 为空）：truncated/failed 帖不重试，
    #    避免大帖每窗口重烧几十个请求——每帖一次评论快照。
    pending = conn.execute(
        """SELECT p.* FROM posts p
           LEFT JOIN scrape_progress sp ON sp.scope = 'comments:' || p.name
           WHERE sp.state IS NULL OR sp.state = 'working'
           ORDER BY p.num_comments DESC LIMIT ?""",
        (args.posts_per_window,)).fetchall()
    p_req0 = client.stats["requests_made"]
    for r in pending:
        used = client.stats["requests_made"] - req0
        # 给 users 段留足配额：users_per_window + 1 请求余量
        if used + args.users_per_window + 1 >= budget:
            log.info("窗口请求预算用尽（%d/%d），posts 提前收工", used, budget)
            break
        try:
            res = await posts.scrape_post(
                client, conn, r["name"], r["permalink"], r["num_comments"],
                expand_more=not args.no_expand_more)
            w["posts"] += 1
            w["comment_ins"] += res.get("inserted", 0)
            w["truncated"] += 1 if res.get("truncated") else 0
        except RedditNotFound:
            w["posts_failed"] += 1   # 帖子被删/不存在，跳过
        except RedditClientError as e:
            w["posts_failed"] += 1
            log.warning("帖子 %s 采集失败：%s", r["name"], str(e)[:120])
    w["posts_reqs"] = client.stats["requests_made"] - p_req0

    # 3) users：未采集作者（1 请求/用户）
    ur = await users.run(client, conn, args.users_per_window,
                         concurrency=args.concurrency)
    w["users"] = ur["authors"]
    w["user_ins"] = ur["inserted"]
    w["user_upd"] = ur["updated"]

    w["reqs"] = client.stats["requests_made"] - req0
    return w


async def _sleep_to_next_window(window_started: float) -> None:
    """睡满本窗口剩余时间（从窗口起点起算 RATE_LIMIT_WINDOW 秒）。"""
    remain = config.RATE_LIMIT_WINDOW - (time.monotonic() - window_started)
    if remain > 0:
        await asyncio.sleep(remain)


def _fmt_window_line(meter: Meter, w: dict) -> str:
    return (f"[窗{w['idx']:03d} {datetime.now():%H:%M:%S}] "
            f"req={w['reqs']:>3} | 列表 +{w['listing_ins']}新/{w['listing_upd']}更 "
            f"({w['listing_detail']}) | 评论 {w['posts']}帖 +{w['comment_ins']}条 "
            f"(截断{w['truncated']} 失败{w['posts_failed']}) | "
            f"用户 {w['users']}人 +{w['user_ins']} | 累计请求 {meter.total_reqs}")


def _fmt_hour_line(meter: Meter) -> str:
    recent = meter.window_reqs[-6:]
    avg = sum(recent) / len(recent) if recent else 0
    return (f"—— 小时聚合：近{len(recent)}窗口 平均req={avg:.0f} "
            f"累计：新帖{meter.listing_ins} 评论{meter.comment_ins} "
            f"用户{meter.users_ins} 请求{meter.total_reqs}")


# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------
def write_report(client: RedditClient, conn, meter: Meter, args,
                 stop_reason: str) -> str:
    """写 docs/capacity_report.md 并返回路径。全程同步 IO，可在收尾栈中调用。"""
    hours = max((time.monotonic() - meter.t0) / 3600.0, 1e-6)
    now = datetime.now()
    stats = client.stats
    reqs = meter.total_reqs

    def per_day(n: int) -> int:
        return round(n / hours * 24)

    budget_day = (config.RATE_LIMIT_BUDGET * 86400.0
                  / config.RATE_LIMIT_WINDOW)
    util = reqs / (budget_day * hours) * 100 if hours else 0.0
    avg_req_win = (sum(meter.window_reqs) / len(meter.window_reqs)
                   if meter.window_reqs else 0)
    req_per_post = (meter.posts_reqs / meter.post_scraped
                    if meter.post_scraped else 0.0)
    lib = db.stats(conn)
    lib_lines = "\n".join(f"- {k}: {v}" for k, v in lib.items())

    md = f"""# r/popular 单账号日容量实测报告

- 采集时段：{meter.wall0:%F %H:%M} ~ {now:%F %H:%M}（{hours:.2f}h，{meter.windows} 窗口）
- 账号形态：单 cookies.json 匿名会话（无 OAuth、无代理池）
- 板块：r/{args.subreddit}（hot / top:day / new 轮扫，posts {args.posts_per_window}/窗，users {args.users_per_window}/窗）
- 结束原因：{stop_reason}

## 核心答案：单账号一天能连续采集多少

| 指标 | {hours:.1f}h 实测 | 折算 24h |
|---|---:|---:|
| HTTP 请求 | {reqs} | {per_day(reqs)} |
| 新帖（列表 inserted） | {meter.listing_ins} | {per_day(meter.listing_ins)} |
| 帖子详情（评论树） | {meter.post_scraped} | {per_day(meter.post_scraped)} |
| 评论入库 | {meter.comment_ins} | {per_day(meter.comment_ins)} |
| 用户资料 | {meter.users_ins} | {per_day(meter.users_ins)} |
| 配额利用率（令牌桶 {config.RATE_LIMIT_BUDGET}/{config.RATE_LIMIT_WINDOW}s） | {util:.0f}% | — |

## 窗口画像

- 平均每窗口请求 {avg_req_win:.0f}（预算 {config.RATE_LIMIT_BUDGET}）
- server_remaining 全程最低：{meter.min_remaining}
- 429/网络重试：{stats.get("retries", 0)} 次
- 评论截断帖（blind more）：{meter.post_truncated}
- 评论单帖成本：{req_per_post:.1f} 请求/帖（含 morechildren 展开）

## 库存量（main.py --stage stats 交叉核对）

{lib_lines}

## 理论上限对照（预算 {budget_day:.0f} 请求/天，全部分配给单一类）

| 策略 | 极值估算 |
|---|---|
| 全 listings | r/popular 池 1,587 帖 ~22 请求即翻穿，增量只取决于日流速（见新帖实测），纯翻无意义 |
| 全 posts | 约 {round(budget_day / 3):,}~{round(budget_day / 2):,} 帖详情（2-3 请求/帖）≈ 数百万条评论 |
| 全 users | {round(budget_day):,} 用户（1 请求/用户，受作者池上限约束） |

> 本报告由 scheduler.py 自动生成。
"""
    path = args.report or os.path.join(config.PROJECT_ROOT, "docs",
                                       "capacity_report.md")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    return path


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------
async def run_async(args) -> int:
    conn = db.get_conn(args.db)
    db.init_db(conn)
    meter = Meter(time.monotonic(), datetime.now())
    code = 0
    stop_reason = f"达到 --hours {args.hours:g} 时长"
    try:
        async with _make_client(args) as client:
            print(f"调度器启动：r/{args.subreddit} 时长={args.hours:g}h "
                  f"窗口={config.RATE_LIMIT_WINDOW}s 预算="
                  f"{args.request_budget or config.RATE_LIMIT_BUDGET}req "
                  f"posts/窗≤{args.posts_per_window} users/窗={args.users_per_window} "
                  f"morechildren={'关' if args.no_expand_more else '开'}")
            deadline = time.monotonic() + args.hours * 3600.0
            win_idx = 0
            try:
                while time.monotonic() < deadline:
                    t_w0 = time.monotonic()
                    w = await run_window(client, conn, args, win_idx)
                    meter.add_window(w, client)
                    print(_fmt_window_line(meter, w))
                    if (win_idx + 1) % 6 == 0:
                        print(_fmt_hour_line(meter))
                    win_idx += 1
                    if time.monotonic() >= deadline:
                        break
                    await _sleep_to_next_window(t_w0)
            except RedditAuthError as e:
                stop_reason = f"cookie 失效（403）：{e}"
                code = 3
            except asyncio.CancelledError:
                stop_reason = "手动中断（Ctrl+C）"
                code = 130
            finally:
                path = write_report(client, conn, meter, args, stop_reason)
                print(f"\n容量报告已写入：{path}")
                print(f"HTTP 统计：{client.stats}")
        return code
    finally:
        conn.close()


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    handlers = [logging.StreamHandler()]
    if args.log_file:
        os.makedirs(os.path.dirname(os.path.abspath(args.log_file)),
                    exist_ok=True)
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else config.LOG_LEVEL,
        format=config.LOG_FORMAT, handlers=handlers)

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        return asyncio.run(run_async(args))
    except KeyboardInterrupt:
        print("\n已中断。", file=sys.stderr)
        return 130
    except RedditAuthError:
        print("\n[403] cookie 失效。重新导出后执行：\n"
              "  uv run python main.py --import-cookie-header \"<Cookie 头>\"",
              file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
