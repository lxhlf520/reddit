# -*- coding: utf-8 -*-
"""pytest 公共夹具：路径注入、Windows 事件循环策略、临时库。"""
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 与 main.py 一致：curl_cffi 异步需要 selector 事件循环（触网测试用）
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import pytest  # noqa: E402

from reddit_collector import db as dbm  # noqa: E402


@pytest.fixture()
def conn(tmp_path):
    """临时 SQLite 连接（每测试独立，不污染 reddit.db）。"""
    c = dbm.get_conn(str(tmp_path / "test.db"))
    dbm.init_db(c)
    yield c
    c.close()
