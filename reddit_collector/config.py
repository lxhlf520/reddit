# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Reddit 采集配置。

集中管理端点、请求头、限流阈值、代理、OAuth 与路径。
所有敏感信息（代理凭据、OAuth secret）从 .env / 环境变量读取，不硬编码。
"""

import os

# 项目根目录（reddit/），本文件位于 reddit/reddit_collector/config.py
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_env(path: str) -> None:
    """加载 .env（已存在的环境变量优先，不覆盖）。"""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())


_load_env(os.path.join(PROJECT_ROOT, ".env"))


def _parse_proxy_server(server: str) -> str:
    """解析 Windows 注册表 ProxyServer 字段为 http:// 代理 URL。

    支持两种格式：
    - 统一:   "127.0.0.1:7890"
    - 分协议: "http=127.0.0.1:7890;https=127.0.0.1:7890;socks=..."
    """
    server = (server or "").strip()
    if not server:
        return ""
    if "=" in server:
        parts = dict(kv.split("=", 1) for kv in server.split(";") if "=" in kv)
        addr = (parts.get("https") or parts.get("http") or "").strip()
    else:
        addr = server
    if not addr:
        return ""
    if not addr.startswith("http://") and not addr.startswith("https://"):
        addr = "http://" + addr
    return addr


def apply_system_proxy() -> str:
    """自动跟随本机系统代理（Windows 注册表），回填到 HTTP(S)_PROXY 环境变量。

    curl_cffi 默认读环境变量代理；这里把注册表里的系统代理同步过去，
    使采集客户端在无显式 --proxy 时自动走本机代理（Clash/mihomo 等）。

    规则：
    - 非 Windows 直接返回（交由环境变量/直连）
    - 已显式设置 HTTP(S)_PROXY 环境变量则尊重，不覆盖
    - 系统代理关闭（或 TUN 模式）时不设置，保持直连
    返回最终生效的代理 URL（无则空串）。
    """
    existing = (
        os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
        or os.environ.get("ALL_PROXY") or os.environ.get("all_proxy")
    )
    if existing:
        return existing
    if os.name != "nt":
        return ""
    try:
        import winreg
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
        try:
            enable, _ = winreg.QueryValueEx(key, "ProxyEnable")
            if not enable:
                return ""
            server, _ = winreg.QueryValueEx(key, "ProxyServer")
            try:
                override, _ = winreg.QueryValueEx(key, "ProxyOverride")
            except OSError:
                override = ""
        finally:
            winreg.CloseKey(key)
    except OSError:
        return ""
    proxy_url = _parse_proxy_server(server)
    if not proxy_url:
        return ""
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    no_proxy = "localhost,127.0.0.1"
    if override:
        extra = ",".join(
            o.strip() for o in override.split(";")
            if o.strip() and o.strip() != "<local>"
        )
        no_proxy += "," + extra
    os.environ["NO_PROXY"] = no_proxy
    os.environ["no_proxy"] = no_proxy
    return proxy_url


# 模块加载时自动应用（早于任何 HTTP 客户端创建）
SYSTEM_PROXY = apply_system_proxy()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


# ==================== 端点 ====================

BASE_URL = "https://www.reddit.com"
OAUTH_BASE = "https://oauth.reddit.com"
# 前端内部 GraphQL（脆弱兜底，默认不用；实测 body={operation,variables,csrf_token}）
SHREDDIT_GQL = BASE_URL + "/svc/shreddit/graphql"

# curl_cffi TLS 指纹伪装目标（实测 Chrome 153；chrome/chrome131 均可）
IMPERSONATE = os.environ.get("IMPERSONATE", "chrome")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
    "Referer": BASE_URL + "/",
}

# ==================== 限流（实测匿名 ~100 请求 / 600 秒窗口） ====================

# 令牌桶阈值设在真实配额下方，留安全边际（默认 90/600s）
RATE_LIMIT_BUDGET = _env_int("RATE_LIMIT_BUDGET", 90)
RATE_LIMIT_WINDOW = _env_int("RATE_LIMIT_WINDOW", 600)
# 命中 429 时的额外固定退避（秒）；实际以 X-ratelimit-reset 为准
RATE_LIMIT_429_BACKOFF = _env_float("RATE_LIMIT_429_BACKOFF", 5.0)

# ==================== 请求 / 重试 ====================

REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 30)
MAX_RETRIES = _env_int("MAX_RETRIES", 4)
RETRY_BACKOFF = _env_float("RETRY_BACKOFF", 2.0)   # 指数退避基数（秒）
CONCURRENCY = _env_int("CONCURRENCY", 6)

# ==================== 采集分页参数 ====================

LISTING_LIMIT = 100          # 列表单页条数（Reddit 上限 100）
COMMENT_LIMIT = 500          # 帖子评论单页条数（上限 500）
COMMENT_DEPTH = 10           # 评论树深度
COMMENT_SORT = "top"         # 评论排序：top/new/controversial/old
MORECHILDREN_BATCH = 100     # /api/morechildren 单次展开的评论 id 数（上限约 100）
LISTING_SORTS = ("hot", "new", "top")   # 默认采集的列表类型
TOP_TIME_FILTER = "day"      # top 列表时间窗：hour/day/week/month/year/all

# ==================== OAuth（可选升级） ====================

OAUTH_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "").strip()
OAUTH_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "").strip()
OAUTH_USERNAME = os.environ.get("REDDIT_USERNAME", "").strip()
OAUTH_PASSWORD = os.environ.get("REDDIT_PASSWORD", "").strip()
# OAuth 模式建议用描述性 app UA（Reddit API 规范）
OAUTH_USER_AGENT = os.environ.get(
    "REDDIT_USER_AGENT", "reddit-collector:v0.1.0 (pure-protocol scraper)"
).strip()
OAUTH_ENABLED = bool(OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET
                     and OAUTH_USERNAME and OAUTH_PASSWORD)

# ==================== 路径 ====================

DB_PATH = os.environ.get("DB_PATH", "").strip() or os.path.join(PROJECT_ROOT, "reddit.db")
COOKIE_FILE = os.path.join(PROJECT_ROOT, "cookies.json")
SEEDS_FILE = os.path.join(PROJECT_ROOT, "seeds", "subreddits.txt")

# ==================== 代理池（可选，规模化轮换出口 IP） ====================
# REDDIT_PROXIES 用逗号/分号/换行分隔多个代理 URL；置空则仅跟随系统代理。
# 例：REDDIT_PROXIES=http://127.0.0.1:7890,http://user:pass@host2:port
PROXIES_RAW = os.environ.get("REDDIT_PROXIES", "").strip()


def load_proxies(raw: str = None) -> list:
    """解析代理列表（逗号/分号/换行分隔），去空去重保序。"""
    text = raw if raw is not None else PROXIES_RAW
    if not text:
        return []
    norm = text.replace(";", ",").replace("\n", ",")
    seen, out = set(), []
    for p in norm.split(","):
        p = p.strip()
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out


PROXIES = load_proxies()

# ==================== 日志 ====================

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


# ==================== 种子 ====================

def load_seeds(path: str = None) -> list:
    """读取种子子版块列表（每行一个，# 注释，可带 r/ 前缀），去重保序。"""
    p = path or SEEDS_FILE
    if not os.path.exists(p):
        return []
    seen, out = set(), []
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name = line[2:].strip() if line.lower().startswith("r/") else line
            if name and name not in seen:
                seen.add(name)
                out.append(name)
    return out
