# -*- coding: utf-8 -*-
"""curl_cffi 异步 HTTP 客户端：TLS 指纹伪装 + cookie 会话 + 限流退避 + 重试。

纯协议采集的核心传输层。实测结论（见 plan Step 0）：
- 裸客户端直接请求 Reddit `.json` → 403（边缘网络安全拦截 + reCAPTCHA）；
- `curl_cffi impersonate="chrome"` 复刻 Chrome TLS 指纹，配合从真实浏览器导出的
  会话 cookie（loid/edgebucket/csrf_token/session_tracker）→ 200；
- 匿名配额约 100 请求 / 600 秒，响应头 `x-ratelimit-remaining/used/reset` 实时校准。

设计：
- `RedditClient` 为 async context manager，内部持有一个 `AsyncSession`；
- 每个响应把 `x-ratelimit-*` 同步给 `RateLimiter`（令牌桶阈值低于真实配额留边际）；
- 429 → 按 `x-ratelimit-reset` 暂停限流器后重试；5xx/网络错误 → 指数退避重试；
- 403 → 抛 `RedditAuthError`（cookie 失效，需重新导出 / 换出口 IP / 上 OAuth）；
- OAuth（可选）：`OAUTH_ENABLED` 时用 script app 的 password grant 换 bearer，
  改走 `oauth.reddit.com`（配额更高、morechildren 可完整展开）。
"""
import asyncio
import logging
from typing import Optional

from curl_cffi import requests as creq

from . import config, cookies
from .ratelimit import RateLimiter

log = logging.getLogger("reddit.client")


# ---------------------------------------------------------------------------
# 异常
# ---------------------------------------------------------------------------
class RedditClientError(Exception):
    """客户端基础异常。"""


class RedditAuthError(RedditClientError):
    """403/401：cookie 失效或被网络安全拦截。

    处理链（见 plan 反爬策略）：重新导出 cookie → 换出口 IP（代理轮换）→ 启用 OAuth。
    """


class RedditRateLimited(RedditClientError):
    """429：多次重试后仍被限流。"""


class RedditNotFound(RedditClientError):
    """404：资源不存在（如被封禁的子版块 / 已注销用户）。"""


class RedditClient:
    """异步 Reddit 协议客户端。

    用法::

        async with RedditClient() as client:
            data = await client.get_json("/r/popular/hot.json",
                                         params={"limit": 100, "raw_json": 1})
    """

    def __init__(self,
                 proxy: Optional[str] = None,
                 use_oauth: Optional[bool] = None,
                 cookie_file: Optional[str] = None,
                 timeout: Optional[int] = None,
                 max_retries: Optional[int] = None,
                 limiter: Optional[RateLimiter] = None,
                 proxy_pool=None):
        # 代理：显式传入 > 代理池当前出口 > 系统代理（config 已从注册表回填环境变量）> 直连
        self.proxy_pool = proxy_pool
        if proxy:
            self.proxy = proxy
        elif proxy_pool is not None and proxy_pool.current:
            self.proxy = proxy_pool.current
        else:
            self.proxy = config.SYSTEM_PROXY or None
        self.timeout = timeout or config.REQUEST_TIMEOUT
        self.max_retries = (config.MAX_RETRIES if max_retries is None
                            else max_retries)
        self.cookie_file = cookie_file or config.COOKIE_FILE
        self.use_oauth = config.OAUTH_ENABLED if use_oauth is None else use_oauth

        # 限流器可外部注入（多客户端共享同一配额时）
        self.limiter = limiter or RateLimiter(
            config.RATE_LIMIT_BUDGET, config.RATE_LIMIT_WINDOW)

        self._base = config.OAUTH_BASE if self.use_oauth else config.BASE_URL
        self._session: Optional[creq.AsyncSession] = None
        self._headers: dict = {}
        self._cookies: dict = {}
        self._oauth_token: Optional[str] = None

        # 运行期统计
        self.requests_made = 0
        self.retries = 0

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "RedditClient":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def start(self) -> "RedditClient":
        """建立会话：装配请求头、cookie（或 OAuth bearer）。"""
        if self.use_oauth:
            self._oauth_token = await self._fetch_oauth_token()
            self._headers = dict(config.HEADERS)
            self._headers["User-Agent"] = config.OAUTH_USER_AGENT
            self._headers["Authorization"] = f"Bearer {self._oauth_token}"
            log.info("OAuth 已启用（bearer 获取成功），base=%s", self._base)
        else:
            self._headers = dict(config.HEADERS)
            self._cookies = cookies.load_cookies(self.cookie_file)
            if self._cookies:
                names = ",".join(sorted(self._cookies))
                age = cookies.cookie_age_hours(self.cookie_file)
                age_s = f"，导出于 {age:.1f} 小时前" if age is not None else ""
                log.info("已加载 cookie（%d 项：%s）%s", len(self._cookies), names, age_s)
                if not cookies.has_cookies(self.cookie_file):
                    log.warning("cookie 缺少关键字段 loid/edgebucket，很可能 403；"
                                "请重新从浏览器导出（main.py --import-cookie-header）")
            else:
                log.warning("未找到 cookie（%s），匿名冷请求预期 403；"
                            "请先导出浏览器 cookie 或启用 OAuth", self.cookie_file)

        self._session = creq.AsyncSession(
            impersonate=config.IMPERSONATE,
            headers=self._headers,
            timeout=self.timeout,
            proxy=self.proxy,
        )
        return self

    async def close(self) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            finally:
                self._session = None

    # ------------------------------------------------------------------
    # OAuth（可选）
    # ------------------------------------------------------------------
    async def _fetch_oauth_token(self) -> str:
        """script app password grant 换取 access_token。"""
        url = config.BASE_URL + "/api/v1/access_token"
        data = {
            "grant_type": "password",
            "username": config.OAUTH_USERNAME,
            "password": config.OAUTH_PASSWORD,
        }
        auth = (config.OAUTH_CLIENT_ID, config.OAUTH_CLIENT_SECRET)
        headers = {"User-Agent": config.OAUTH_USER_AGENT}
        async with creq.AsyncSession(
                impersonate=config.IMPERSONATE,
                timeout=self.timeout, proxy=self.proxy) as s:
            resp = await s.post(url, data=data, auth=auth, headers=headers)
        if resp.status_code != 200:
            raise RedditAuthError(
                f"OAuth token 获取失败 {resp.status_code}: {(resp.text or '')[:200]}")
        token = (resp.json() or {}).get("access_token")
        if not token:
            raise RedditAuthError("OAuth 响应缺少 access_token")
        return token

    # ------------------------------------------------------------------
    # 请求
    # ------------------------------------------------------------------
    async def get_json(self, path: str, params: Optional[dict] = None,
                       referer: Optional[str] = None,
                       extra_headers: Optional[dict] = None,
                       not_found_ok: bool = False):
        """GET 一个 `.json` 端点并返回解析后的 JSON。

        - path：以 http 开头视为绝对 URL，否则拼接当前 base（www / oauth）；
        - not_found_ok=True 时 404 返回 None（用于可能不存在的用户/子版块）；
        - 集成限流令牌桶、429 暂停重试、5xx/网络错误指数退避、403 抛鉴权异常。
        """
        if self._session is None:
            raise RedditClientError("客户端未启动，请用 async with 或先 await start()")

        url = path if path.startswith("http") else self._base + path
        headers = None
        if referer or extra_headers:
            headers = dict(self._headers)
            if referer:
                headers["Referer"] = referer
            if extra_headers:
                headers.update(extra_headers)

        attempt = 0
        last_code: Optional[int] = None
        last_err: Optional[Exception] = None
        while attempt <= self.max_retries:
            await self.limiter.acquire()
            try:
                resp = await self._session.get(
                    url,
                    params=params,
                    headers=headers,
                    cookies=self._cookies or None,
                    proxy=self.proxy,
                    timeout=self.timeout,
                )
            except Exception as e:  # 网络/TLS/超时
                last_err = e
                self.retries += 1
                attempt += 1
                wait = min(config.RETRY_BACKOFF * (2 ** (attempt - 1)), 30.0)
                log.warning("网络错误 %s（%s: %s），第 %d 次重试，%.1fs 后",
                            url, type(e).__name__, str(e)[:120], attempt, wait)
                await asyncio.sleep(wait)
                continue

            self.requests_made += 1
            self.limiter.sync_headers(resp.headers)
            code = resp.status_code
            last_code = code

            if code == 200:
                return self._parse_json(resp, url)

            if code == 429:
                reset = self._ratelimit_reset(resp.headers)
                pause = min(max(reset, config.RATE_LIMIT_429_BACKOFF), 600.0)
                self.retries += 1
                attempt += 1
                log.warning("429 限流 %s，暂停 %.1fs 后重试（第 %d 次）",
                            url, pause, attempt)
                self.limiter.pause(pause)
                await asyncio.sleep(pause)
                continue

            if code in (401, 403):
                # 403 升级链：先尝试换出口 IP（代理池轮换），无池/轮换失败才抛鉴权异常
                if (code == 403 and self.proxy_pool is not None
                        and self.proxy_pool.size > 1 and attempt < self.max_retries):
                    self.proxy_pool.mark_bad(self.proxy)
                    new_proxy = self.proxy_pool.rotate()
                    if new_proxy:
                        self.proxy = new_proxy
                        self.retries += 1
                        attempt += 1
                        log.warning("403 出口被拦，轮换代理后重试（第 %d 次）：%s",
                                    attempt, url)
                        continue
                hint = ("OAuth token 失效" if self.use_oauth
                        else "cookie 可能失效：重新导出浏览器 cookie / 换出口 IP / 启用 OAuth")
                raise RedditAuthError(f"HTTP {code} {url} —— {hint}")

            if code == 404:
                if not_found_ok:
                    log.info("404 跳过（not_found_ok）：%s", url)
                    return None
                raise RedditNotFound(f"HTTP 404 {url}")

            if 500 <= code < 600:
                self.retries += 1
                attempt += 1
                wait = min(config.RETRY_BACKOFF * (2 ** (attempt - 1)), 30.0)
                log.warning("%d 服务端错误 %s，第 %d 次重试，%.1fs 后",
                            code, url, attempt, wait)
                await asyncio.sleep(wait)
                continue

            # 其余 4xx：不可重试
            raise RedditClientError(
                f"HTTP {code} {url}: {(resp.text or '')[:200]}")

        # 重试耗尽
        if last_code == 429:
            raise RedditRateLimited(f"429 限流重试耗尽：{url}")
        raise RedditClientError(
            f"请求失败（last_code={last_code}）：{url}，err={last_err}")

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_json(resp, url: str):
        try:
            return resp.json()
        except Exception:
            snippet = (resp.text or "")[:200].replace("\n", " ")
            raise RedditClientError(
                f"响应非 JSON（可能是 HTML 拦截页）{url}: {snippet}")

    @staticmethod
    def _ratelimit_reset(headers) -> float:
        """读取 x-ratelimit-reset（距窗口重置秒数）；缺失则回退默认退避。"""
        try:
            val = headers.get("x-ratelimit-reset")
            if val is not None:
                return max(0.0, float(val))
        except (TypeError, ValueError):
            pass
        return config.RATE_LIMIT_429_BACKOFF

    @property
    def stats(self) -> dict:
        return {
            "requests_made": self.requests_made,
            "retries": self.retries,
            "server_remaining": self.limiter.server_remaining,
            "paused_seconds": round(self.limiter.paused_seconds, 1),
            "oauth": self.use_oauth,
        }
