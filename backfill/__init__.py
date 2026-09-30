# -*- coding: utf-8 -*-
"""Arctic Shift 历史回补：BT zst dump → 远程 PostgreSQL。

子模块：
- magnets      一次性生成 magnets.json / torrents/ 元数据 / stats.csv（对账基准）
- gen_schema   生成 pg_schema.sql（月度分区母表 + import_log）
- download     aria2c 封装（DHT 自举、断点续传、select-file）
- ingest       zst 流式解压 → PG COPY（整月单事务，分区级幂等）
- pipeline     月度状态机编排（下载完成检测 → 入库 → 对账 → 清理）
"""

__version__ = "0.1.0"
