# -*- coding: utf-8 -*-
"""验证 asyncpg gzip codec 压缩存储方案。

测试：
1. 建表（bytea 列）
2. 写入数据（codec 自动 gzip）
3. 读出数据（codec 自动解压）
4. 验证压缩率
"""
import asyncio
import gzip
import sys

import asyncpg

# 复用 ingest.py 的 codec
from backfill.ingest import init_gzip_codec

# 测试 DSN（本地隔离实例或远程 PG）
DSN = "postgresql://postgres@localhost:5499/reddit_test"

TEST_SCHEMA = """
DROP TABLE IF EXISTS test_bytea_gzip;
CREATE TABLE test_bytea_gzip (
    id text PRIMARY KEY,
    title bytea,
    body bytea,
    raw bytea NOT NULL
);
"""

async def main():
    print("=== asyncpg gzip codec 验证 ===\n")
    
    # 连接 PG（用 create_pool，init 回调注册 codec）
    try:
        pool = await asyncpg.create_pool(DSN, init=init_gzip_codec, min_size=1, max_size=1)
    except Exception as e:
        print(f"连接失败：{e}")
        print("\n请先启动本地隔离实例：")
        print("  initdb -D _tmp_pg -U postgres -A trust -E UTF8 --no-locale")
        print('  pg_ctl -D _tmp_pg -o "-p 5499" -l pg.log start')
        print("  createdb -p 5499 -h localhost -U postgres reddit_test")
        return 1
    
    async with pool.acquire() as conn:
        # 建表
        await conn.execute(TEST_SCHEMA)
        print("[OK] 建表 test_bytea_gzip")
        
        # 测试数据
        test_rows = [
            ("id1", "短标题", "短评论", '{"key": "value"}'),
            ("id2", "这是一个比较长的标题" * 10, "这是一个比较长的正文" * 20, 
             '{"field1": "value1", "field2": "value2"}' * 50),
            ("id3", "中文标题测试 🚀", "中文评论测试 🎉", 
             '{"中文": "字段", "emoji": "🔥"}'),
        ]
        
        # 写入（codec 自动 gzip）
        for id_, title, body, raw in test_rows:
            await conn.execute(
                "INSERT INTO test_bytea_gzip (id, title, body, raw) VALUES ($1, $2, $3, $4)",
                id_, title, body, raw
            )
        print(f"[OK] 写入 {len(test_rows)} 行（codec 自动 gzip）")
        
        # 读出（codec 自动解压）
        rows = await conn.fetch("SELECT id, title, body, raw FROM test_bytea_gzip ORDER BY id")
        print(f"[OK] 读出 {len(rows)} 行（codec 自动解压）")
        
        # 验证
        for row in rows:
            id_ = row['id']
            title = row['title']
            body = row['body']
            raw = row['raw']
            
            # 类型检查
            assert isinstance(title, str), f"title 应该是 str，实际 {type(title)}"
            assert isinstance(body, str), f"body 应该是 str，实际 {type(body)}"
            assert isinstance(raw, str), f"raw 应该是 str，实际 {type(raw)}"
            
            print(f"  {id_}: title={title[:20]!r}..., body={body[:20]!r}..., raw={raw[:30]!r}...")
        
        # 压缩率统计
        print("\n=== 压缩率统计 ===")
        for id_, title, body, raw in test_rows:
            # 原始大小
            orig_size = len(title.encode('utf-8')) + len(body.encode('utf-8')) + len(raw.encode('utf-8'))
            
            # 压缩后大小（从 PG 读 bytea 原始字节）
            compressed = await conn.fetchrow(
                "SELECT length(title) + length(body) + length(raw) AS total_bytes FROM test_bytea_gzip WHERE id = $1",
                id_
            )
            compressed_size = compressed['total_bytes']
            
            ratio = orig_size / compressed_size if compressed_size > 0 else 0
            print(f"  {id_}: 原始 {orig_size}B → 压缩 {compressed_size}B → 压缩率 {ratio:.2f}x")
        
        print("\n[OK] 全部验证通过！")
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
