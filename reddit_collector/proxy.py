# -*- coding: utf-8 -*-
"""代理解析与轮换（可选，规模化采集摊薄单 IP 压力）。

两级策略（对齐 polymarket/glassdoor 的思路）：
1. **系统代理跟随**：config.apply_system_proxy() 已在导入时把 Windows 注册表里的
   系统代理回填到 HTTP(S)_PROXY 环境变量，curl_cffi 默认读取；无系统代理则直连。
2. **代理池轮换**：若在 .env 配置 `REDDIT_PROXIES`（逗号/分号/换行分隔多个 URL），
   `build_pool()` 返回一个轮询池；命中 403（出口 IP 被拦）时客户端可 `mark_bad()`
   当前代理并 `rotate()` 到下一个重试，构成 403 升级链的"换出口 IP"环节。

未配置代理池时，`build_pool()` 返回 None，客户端行为与仅跟随系统代理完全一致。
"""
import itertools
import logging
from typing import Optional

from . import config

log = logging.getLogger("reddit.proxy")


def resolve_proxy(explicit: Optional[str] = None) -> Optional[str]:
    """确定单个出口代理：显式 --proxy > 系统代理 > None（直连）。"""
    return explicit or (config.SYSTEM_PROXY or None)


class ProxyPool:
    """线程/协程内使用的轮询代理池（单事件循环，无需加锁）。"""

    def __init__(self, proxies: list[str]):
        self._proxies = [p for p in proxies if p]
        self._cycle = itertools.cycle(self._proxies) if self._proxies else None
        self._current: Optional[str] = self._proxies[0] if self._proxies else None
        self._bad: set[str] = set()

    @property
    def size(self) -> int:
        return len(self._proxies)

    @property
    def current(self) -> Optional[str]:
        return self._current

    def mark_bad(self, proxy: Optional[str]) -> None:
        if proxy:
            self._bad.add(proxy)

    def rotate(self) -> Optional[str]:
        """切换到下一个未被标记为坏的代理；全部坏或不足 2 个则返回 None。"""
        if self.size < 2 or self._cycle is None:
            return None
        for _ in range(self.size):
            cand = next(self._cycle)
            if cand not in self._bad:
                if cand != self._current:
                    log.info("代理轮换：%s → %s（坏池 %d 个）",
                             self._current, cand, len(self._bad))
                    self._current = cand
                    return cand
                return None
        log.warning("代理池全部被标记为坏，无可用出口")
        return None


def build_pool(proxies: Optional[list[str]] = None) -> Optional[ProxyPool]:
    """从 config.PROXIES（或显式列表）构建代理池；不足 1 个返回 None。"""
    plist = proxies if proxies is not None else config.PROXIES
    if not plist:
        return None
    pool = ProxyPool(plist)
    log.info("代理池已启用：%d 个出口", pool.size)
    return pool
