# -*- coding: utf-8 -*-
"""Reddit 纯协议采集器 CLI 编排入口。

阶段（--stage）：
  initdb    仅建库建表
  listings  按种子子版块采集帖子列表（t3）
  posts     对已采帖子采集详情 + 评论树（t1，含 morechildren 展开）
  users     对作者去重采集用户资料（t2）
  stats     打印库汇总
  all       依次跑 listings → posts → users

用法示例：
  uv run python main.py --stage initdb
  uv run python main.py --stage listings --subreddit popular --sort hot --limit 100
  uv run python main.py --stage listings --limit 100            # 用 seeds 全部子版块
  uv run python main.py --stage posts --limit 5
  uv run python main.py --stage users --limit 20
  uv run python main.py --stage stats
  uv run python main.py --import-cookie-header "loid=...; edgebucket=...; csrf_token=..."

反爬说明：匿名 .json 依赖从真实浏览器导出的会话 cookie（cookies.json）。
若冷启 403，请用 --import-cookie-header 刷新 cookie，或在 .env 配置 OAuth。
"""
import argparse
import asyncio
import logging
import sys

from reddit_collector import config, cookies, db, proxy
from reddit_collector.client import RedditAuthError, RedditClient
from reddit_collector.scrapers import listings, posts, users

log = logging.getLogger("reddit.main")


# ---------------------------------------------------------------------------
# 参数
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="main.py", description="Reddit 纯协议采集器（curl_cffi + cookie）",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stage",
                   choices=["initdb", "listings", "posts", "users", "stats", "all"],
                   help="采集阶段")
    p.add_argument("--subreddit", action="append", default=[],
                   help="子版块名（可多次；缺省用 seeds/subreddits.txt）")
    p.add_argument("--subreddits-file", default=None, help="自定义种子文件路径")
    p.add_argument("--sort", action="append", default=[],
                   help="列表排序 hot/new/top/rising（可多次；缺省用 config.LISTING_SORTS）")
    p.add_argument("--time-filter", default=None,
                   help="top 列表时间窗 hour/day/week/month/year/all")
    p.add_argument("--limit", type=int, default=None,
                   help="listings=每 scope 帖数(默认100)；posts=帖数(默认10)；users=作者数(默认50)")
    p.add_argument("--force", action="store_true",
                   help="忽略已完成进度，强制重采（用于幂等核对）")
    p.add_argument("--no-expand-more", action="store_true",
                   help="posts 阶段不调用 morechildren 展开")
    p.add_argument("--db", default=None, help="SQLite 路径（默认 config.DB_PATH）")
    p.add_argument("--proxy", default=None, help="显式代理 URL（覆盖系统代理）")
    p.add_argument("--concurrency", type=int, default=None, help="并发数（默认 config.CONCURRENCY）")
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--oauth", dest="oauth", action="store_true", default=None,
                     help="强制启用 OAuth（需 .env 凭据）")
    grp.add_argument("--no-oauth", dest="oauth", action="store_false",
                     help="强制匿名 cookie 模式")
    p.add_argument("--import-cookie-header", default=None,
                   help="从浏览器复制的 Cookie 头导入并落盘 cookies.json，然后退出")
    p.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    return p


# ---------------------------------------------------------------------------
# 编排
# ---------------------------------------------------------------------------
def _resolve_subreddits(args) -> list:
    if args.subreddit:
        return args.subreddit
    return config.load_seeds(args.subreddits_file)


def _make_client(args) -> RedditClient:
    # 未显式 --proxy 且配置了 REDDIT_PROXIES 时，启用代理池（支持 403 轮换）
    pool = None if args.proxy else proxy.build_pool()
    return RedditClient(proxy=args.proxy, use_oauth=args.oauth, proxy_pool=pool)


async def _stage_listings(client, conn, args):
    subs = _resolve_subreddits(args)
    if not subs:
        log.error("没有可用子版块（--subreddit 或 seeds 文件）")
        return
    sorts = args.sort or None
    limit = args.limit if args.limit is not None else config.LISTING_LIMIT
    log.info("listings：subs=%s sorts=%s limit=%d", subs,
             sorts or list(config.LISTING_SORTS), limit)
    summary = await listings.run(client, conn, subs, sorts, limit,
                                 time_filter=args.time_filter, force=args.force,
                                 concurrency=args.concurrency)
    print("\n=== listings 汇总 ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


async def _stage_posts(client, conn, args):
    limit = args.limit if args.limit is not None else 10
    log.info("posts：limit=%d expand_more=%s", limit, not args.no_expand_more)
    summary = await posts.run(client, conn, limit,
                              expand_more=not args.no_expand_more,
                              force=args.force, concurrency=args.concurrency)
    print("\n=== posts 汇总 ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


async def _stage_users(client, conn, args):
    limit = args.limit if args.limit is not None else 50
    log.info("users：limit=%d", limit)
    summary = await users.run(client, conn, limit, force=args.force,
                              concurrency=args.concurrency)
    print("\n=== users 汇总 ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


async def run_async(args) -> int:
    conn = db.get_conn(args.db)
    db.init_db(conn)
    try:
        if args.stage == "initdb":
            print(f"已初始化数据库：{config.DB_PATH if not args.db else args.db}")
            return 0
        if args.stage == "stats":
            s = db.stats(conn)
            print("=== 库统计 ===")
            for k, v in s.items():
                print(f"  {k}: {v}")
            return 0

        async with _make_client(args) as client:
            pool_n = client.proxy_pool.size if client.proxy_pool else 0
            print(f"客户端就绪：base={client._base} proxy={client.proxy} "
                  f"oauth={client.use_oauth} proxy_pool={pool_n}")
            try:
                if args.stage == "listings":
                    await _stage_listings(client, conn, args)
                elif args.stage == "posts":
                    await _stage_posts(client, conn, args)
                elif args.stage == "users":
                    await _stage_users(client, conn, args)
                elif args.stage == "all":
                    await _stage_listings(client, conn, args)
                    await _stage_posts(client, conn, args)
                    await _stage_users(client, conn, args)
            finally:
                print(f"\nHTTP 统计：{client.stats}")
        return 0
    finally:
        conn.close()


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else config.LOG_LEVEL,
        format=config.LOG_FORMAT)

    # cookie 头导入（独立动作，不采集）
    if args.import_cookie_header:
        cookies.import_cookie_header(args.import_cookie_header)
        loaded = cookies.load_cookies()
        print(f"已导入 cookie（{len(loaded)} 项）：{sorted(loaded)}")
        print(f"写入：{config.COOKIE_FILE}")
        return 0

    if not args.stage:
        print("请用 --stage 指定阶段（或 --import-cookie-header 导入 cookie）。"
              "用 -h 查看帮助。", file=sys.stderr)
        return 2

    # Windows：curl_cffi 异步需要 selector 事件循环，避免 Proactor add_reader 警告
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        return asyncio.run(run_async(args))
    except RedditAuthError as e:
        log.error("鉴权失败（403/401）：%s", e)
        print("\n[403] cookie 可能失效。请重新从浏览器导出 Cookie 头并执行：\n"
              "  uv run python main.py --import-cookie-header \"<粘贴 Cookie 头>\"\n"
              "或在 .env 配置 OAuth 凭据后加 --oauth 重跑。", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print("\n已中断（进度已保存，可重跑续采）。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
