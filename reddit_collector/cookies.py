# -*- coding: utf-8 -*-
"""Cookie jar 管理：加载 / 保存 / 从浏览器 Cookie 头导入。

纯协议采集依赖会话 cookie（loid/csrf_token/edgebucket/session_tracker）绕过
Reddit 边缘 403。这些 cookie 由真实浏览器通过 JS 挑战 / reCAPTCHA 建立，
curl_cffi 无法自行获取（实测冷启只拿到 edgebucket/csv，缺 loid 即 403）。

获取途径（由 agent 或用户完成，Python 不直连 CDP）：
1. chrome-devtools MCP 在已放行的 reddit 页面执行 document.cookie 导出；
2. 或从浏览器 DevTools / get_network_request 复制一条成功 .json 请求的
   完整 Cookie 头，用 `import_cookie_header()` 落盘。

cookies.json 已在 .gitignore 中排除，属本地运行期产物；loid/session_tracker
会过期，失效后重新导出即可。
"""
import json
import os
import time
from typing import Optional

from . import config


def _path(path: Optional[str] = None) -> str:
    return path or config.COOKIE_FILE


def load_cookie_jar(path: Optional[str] = None) -> dict:
    """读取整个 cookies.json（含元数据）；不存在返回空结构。"""
    p = _path(path)
    if not os.path.exists(p):
        return {"cookies": {}}
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "cookies" in data:
            return data
        # 兼容直接存 {name: value} 的旧格式
        if isinstance(data, dict):
            return {"cookies": data}
    except (json.JSONDecodeError, OSError):
        pass
    return {"cookies": {}}


def load_cookies(path: Optional[str] = None) -> dict:
    """仅返回 {name: value} cookie 字典。"""
    jar = load_cookie_jar(path)
    cookies = jar.get("cookies") or {}
    return {k: v for k, v in cookies.items() if isinstance(v, str)}


def has_cookies(path: Optional[str] = None) -> bool:
    """是否已具备关键 cookie（loid 是绕过 403 的核心）。"""
    cookies = load_cookies(path)
    return "loid" in cookies and "edgebucket" in cookies


def save_cookies(cookies: dict, source: str = "manual",
                 path: Optional[str] = None) -> str:
    """写入 cookies.json（附元数据）。返回文件路径。"""
    p = _path(path)
    jar = {
        "source": source,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "user_agent": config.USER_AGENT,
        "cookies": dict(cookies),
    }
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(jar, f, ensure_ascii=False, indent=2)
    return p


def parse_cookie_header(header: str) -> dict:
    """解析 "a=b; c=d" 或 "Cookie: a=b; c=d" 形式的 Cookie 头为字典。"""
    header = (header or "").strip()
    if header.lower().startswith("cookie:"):
        header = header[len("cookie:"):].strip()
    out = {}
    for part in header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, _, v = part.partition("=")
        out[k.strip()] = v.strip()
    return out


def import_cookie_header(header: str, source: str = "cookie-header",
                         path: Optional[str] = None) -> dict:
    """从 Cookie 头字符串导入并落盘，返回解析出的 cookie 字典。"""
    cookies = parse_cookie_header(header)
    if not cookies:
        raise ValueError("未从提供的字符串解析出任何 cookie")
    save_cookies(cookies, source=source, path=path)
    return cookies


def cookie_age_hours(path: Optional[str] = None) -> Optional[float]:
    """cookie 导出的年龄（小时），无法判定返回 None。"""
    p = _path(path)
    if not os.path.exists(p):
        return None
    try:
        return (time.time() - os.path.getmtime(p)) / 3600.0
    except OSError:
        return None
