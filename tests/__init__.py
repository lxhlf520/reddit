"""Reddit 采集边界测试套件。

- 离线测试（默认）：FakeClient 回放 + 临时 SQLite，覆盖翻页极限、账号边界、
  morechildren 批量钳制、截断标记等，不触网、不耗配额。
- 在线探针（-m online + RUN_ONLINE=1）：固化实测的 Reddit 边界响应契约。
"""
