# -*- coding: utf-8 -*-
"""异步令牌桶限流器，按 Reddit 的 x-ratelimit 响应头动态校准。

实测匿名会话约 100 请求 / 600 秒窗口，响应头返回：
  x-ratelimit-remaining  本窗口剩余配额
  x-ratelimit-used       本窗口已用
  x-ratelimit-reset      距窗口重置的秒数

策略：本地令牌桶阈值设在真实配额下方（默认 90/600s）留安全边际；
每次响应用 remaining 夹紧本地令牌数（服务端为准，避免超发）；
命中 429 时按 reset 暂停整个限流器再重试。
"""
import asyncio
import time
from typing import Optional


class RateLimiter:
    def __init__(self, budget: float, window: float):
        self.budget = float(budget)
        self.window = float(window)
        self.rate = self.budget / self.window if self.window > 0 else 1.0
        self._tokens = self.budget
        self._last = time.monotonic()
        self._lock = asyncio.Lock()
        self._pause_until = 0.0
        self.server_remaining: Optional[float] = None

    def sync_headers(self, headers) -> None:
        """用响应头里的服务端剩余配额夹紧本地令牌（单线程事件循环内赋值原子）。"""
        try:
            rem = headers.get("x-ratelimit-remaining")
            if rem is not None:
                self.server_remaining = float(rem)
                self._tokens = min(self._tokens, max(0.0, self.server_remaining))
        except (TypeError, ValueError):
            pass

    def pause(self, seconds: float) -> None:
        """暂停限流器 seconds 秒（用于 429 后等待窗口重置）。"""
        if seconds > 0:
            self._pause_until = max(self._pause_until, time.monotonic() + seconds)

    @property
    def paused_seconds(self) -> float:
        return max(0.0, self._pause_until - time.monotonic())

    async def acquire(self) -> None:
        """获取一个令牌，必要时等待。"""
        while True:
            async with self._lock:
                now = time.monotonic()
                if now < self._pause_until:
                    wait = self._pause_until - now
                else:
                    self._tokens = min(self.budget,
                                       self._tokens + (now - self._last) * self.rate)
                    self._last = now
                    if self._tokens >= 1.0:
                        self._tokens -= 1.0
                        return
                    wait = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(min(max(wait, 0.05), 15.0))
