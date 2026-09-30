# Reddit 纯协议采集系统

用 **Python 纯协议**（`curl_cffi` Chrome TLS 指纹 + 会话 cookie）采集 Reddit 三类公开数据，落地 SQLite：

- **帖子列表**（t3）：按种子子版块翻 `hot/new/top`
- **帖子详情 + 评论树**（t1）：含 `morechildren` 展开与截断标记
- **用户资料**（t2）：对作者去重采集

采集全程走协议，**不碰 HTML**（Reddit HTML 首页对全新指纹弹 reCAPTCHA），浏览器仅用于一次性 cookie 引导兜底。

本仓库两条数据线：

| 数据线 | 入口 | 目标库 | 范围 |
|---|---|---|---|
| **在线采集**（近实时） | `main.py` / `scheduler.py` | SQLite | 列表流 + 评论树 + 用户；受匿名配额与翻页深度限制（见「已知限制」） |
| **历史全量回补** | `backfill/pipeline.py` | PostgreSQL | 2020-01 ~ 2026-08 全字段 dump 落库（BT zst，见「历史回补」章节） |

---

## 核心结论（实测证据）

| 现象 | 结论 |
|---|---|
| 全新 profile 打开 HTML 首页 | 弹 **reCAPTCHA**（"Prove your humanity"）→ 只走 `.json`，不采 HTML |
| 裸客户端（.NET/requests 指纹，无 cookie）请求 `.json` | **HTTP 403**（边缘网络安全拦截） |
| `curl_cffi impersonate="chrome"` **冷启**（仅 edgebucket/csv，缺 loid） | 仍 **403** |
| `curl_cffi impersonate="chrome"` + **浏览器导出的完整 cookie**（含 `loid`） | **200** ✅ |
| 匿名 `.json` 限流头 `x-ratelimit-remaining/reset` | 约 **100 请求 / 600 秒**窗口 |
| `/api/morechildren.json` 响应结构 | `{json:{errors:[],data:{things:[t1]}}}` —— **things 在 `json.data` 下**；垃圾 children → 200 空 `things` |
| 登出态大帖 `morechildren` 展开实测 | 6566 评论帖 5765 个 `more` id 全部展开，评论入库 6077 条 ✅（此前误判“登出态恒空”，实为解析路径 bug） |

**关键**：纯协议可行的前提是「Chrome TLS 指纹 + 会话 cookie（尤其 `loid`）」。cookie 由真实浏览器通过挑战建立，`curl_cffi` 无法自行获取，需从浏览器导出。

---

## 安装

推荐 [uv](https://docs.astral.sh/uv/)：

```bash
cd reddit
uv sync                 # 安装依赖（curl-cffi==0.7.4）
```

或 pip：

```bash
pip install -r requirements.txt
```

> **Windows 版本锁定**：`curl-cffi==0.7.4` 为已验证稳定版；更高版本曾出现 `OPENSSL_internal` TLS 握手兼容问题。

---

## 配置（.env）

所有机器相关配置集中在项目根目录 `.env`（**不入库**，模板见 `.env.example`）：

```bash
cp .env.example .env      # Windows: copy .env.example .env
```

| 用途 | 变量 | 说明 |
|---|---|---|
| 代理·单出口 | `HTTP_PROXY` / `HTTPS_PROXY` | 显式指定代理；**留空**时自动跟随本机系统代理（Windows 注册表），非 Windows 留空 = 直连 |
| 代理·出口池 | `REDDIT_PROXIES` | 多出口逗号分隔，命中 403 自动轮换：`http://127.0.0.1:7890,http://user:pass@host2:port` |
| 数据库·PostgreSQL | `PG_DSN` | **backfill 远程部署必填**：`postgresql://user:pass@host:5432/reddit` |
| 数据库·SQLite | `DB_PATH` | 在线采集落地路径，默认 `<项目根>/reddit.db` |
| BT 下载目录 | `BT_DIR` | zst 暂存（单月峰值 ~70GB，建议独立数据盘）；默认 `backfill/bt_data` |
| zst 保留策略 | `KEEP_ZST` | `0`=入库后删除省盘；`1`=保留作冷备份；默认 0 |
| aria2 路径 | `ARIA2C` | PATH 中找不到时显式指定可执行文件 |
| OAuth（可选） | `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` / `REDDIT_USERNAME` / `REDDIT_PASSWORD` | 提高配额至 ~100 QPM（见「OAuth」章节） |
| 采集调优 | `CONCURRENCY` / `RATE_LIMIT_BUDGET` / `RATE_LIMIT_WINDOW` / `REQUEST_TIMEOUT` / `MAX_RETRIES` / `IMPERSONATE` / `LOG_LEVEL` | 均有代码默认值，一般无需改 |

规则：`.env` 已 gitignore（凭据不入库）；每台机器独立维护自己的 `.env`；`.env` 中已设置的键**优先于**系统环境变量。

---

## 第一步：引导 cookie（关键）

匿名采集依赖会话 cookie。二选一：

### 方式 A：从浏览器复制 Cookie 头（推荐，最简单）

1. 用真实 Chrome 打开 <https://www.reddit.com/> 并通过人机验证，进入正常 feed；
2. F12 → Network → 任选一条成功的 `.json` 请求 → 复制其 **Request Headers** 里的整条 `Cookie:` 值；
3. 导入落盘：

```bash
uv run python main.py --import-cookie-header "loid=...; edgebucket=...; csrf_token=...; session_tracker=..."
```

### 方式 B：chrome-devtools MCP 导出（agent 侧）

在已放行的 reddit 页面执行 `document.cookie` 导出（本项目 cookies.json 即由此产生）。

导入后 cookie 写入 `cookies.json`（已在 `.gitignore` 中排除）。`loid/session_tracker` 会过期，失效后（表现为 403）重新导出即可。

---

## 用法

CLI 入口 `main.py`，按阶段（`--stage`）编排：

```bash
# 0) 建库建表
uv run python main.py --stage initdb

# 1) 帖子列表：默认读 seeds/subreddits.txt；也可指定子版块/排序/条数
uv run python main.py --stage listings --subreddit popular --sort hot --limit 100
uv run python main.py --stage listings --limit 100            # seeds 全部 × hot/new/top

# 2) 帖子详情 + 评论（对已入库帖子，按 num_comments 降序取 limit 个）
uv run python main.py --stage posts --limit 5

# 3) 用户资料（对帖子/评论作者去重后取 limit 个）
uv run python main.py --stage users --limit 20

# 4) 库汇总
uv run python main.py --stage stats

# 一条龙：listings → posts → users
uv run python main.py --stage all --limit 100
```

### 常用参数

| 参数 | 说明 |
|---|---|
| `--stage` | `initdb/listings/posts/users/stats/all` |
| `--subreddit` | 子版块名（可多次；缺省用 seeds） |
| `--sort` | `hot/new/top/rising`（可多次；缺省 `config.LISTING_SORTS`） |
| `--time-filter` | `top` 列表时间窗 `hour/day/week/month/year/all` |
| `--limit` | listings=每 scope 帖数(100)；posts=帖数(10)；users=作者数(50) |
| `--force` | 忽略已完成进度强制重采（用于幂等核对） |
| `--no-expand-more` | posts 阶段不调用 morechildren 展开 |
| `--proxy` | 显式代理 URL（覆盖系统代理） |
| `--oauth / --no-oauth` | 强制启用/禁用 OAuth（缺省按 `.env` 自动判定） |
| `--concurrency` | 并发数（缺省 `config.CONCURRENCY=6`） |
| `--db` | SQLite 路径 |
| `-v/--verbose` | DEBUG 日志 |

---

## 限流与反爬策略

1. **令牌桶限流**：阈值设在真实配额下方（默认 **90/600s**）；每个响应用 `x-ratelimit-remaining` 夹紧本地令牌，服务端为准。
2. **429 退避**：命中 429 → 读 `x-ratelimit-reset` 暂停限流器至窗口重置再重试。
3. **cookie 引导**：主路径 curl_cffi 加载 `cookies.json`；失效（403）→ 重新导出。
4. **403 升级链**：换 cookie → 换出口 IP（代理池轮换）→ 启用 OAuth。
5. **重试退避**：5xx/网络错误按 `RETRY_BACKOFF` 指数退避（`MAX_RETRIES` 次）。
6. **UA**：匿名 `.json` 用真实 Chrome UA；OAuth 模式改用描述性 app UA。

---

## OAuth（可选升级）

匿名模式已可完整展开 morechildren（见边界测试），OAuth 主要用于更高配额（~100 QPM）：

1. 在 <https://www.reddit.com/prefs/apps> 创建 **script** 类型 app，拿到 `client_id/secret`；
2. 复制 `.env.example` 为 `.env`，填入 `REDDIT_CLIENT_ID/SECRET/USERNAME/PASSWORD`；
3. 加 `--oauth` 运行（走 `oauth.reddit.com`，`morechildren` 可完整展开）。

凭据只存 `.env`（已 gitignore），不入库不入代码。

---

## 代理

- **默认跟随系统代理**：`config.apply_system_proxy()` 读取 Windows 注册表回填 `HTTP(S)_PROXY`，curl_cffi 自动使用；无系统代理则直连。
- **代理池轮换**（规模化）：`.env` 配 `REDDIT_PROXIES=url1,url2,...`，命中 403 时自动 `mark_bad` 当前出口并 `rotate` 到下一个重试。

---

## 已知限制

- **评论树 blind more**：`count>0` 但未提供 children id 的 `more` 占位（登出态折叠树深处）无法经 morechildren 展开，此类帖子标记 `truncated`。历史上曾误判“登出态 morechildren 恒返回空”，实为响应解析路径错误（`json.things` ≠ `json.data.things`），已修复并实测大帖展开至 6077 条（详见下方边界测试）。
- **cookie 时效**：`loid/session_tracker` 会过期，失效后需重新导出（表现 403）。
- **HTML 不采**：首页/评论区 HTML 有 reCAPTCHA，本项目只走 `.json`。

---

## 边界测试

`tests/` 共 50 个用例（44 离线 + 6 在线探针），覆盖翻页极限、账号边界、morechildren 批次、client 异常路径、断点/幂等语义：

```bash
uv run pytest                                          # 离线套件（默认排除触网用例）
$env:RUN_ONLINE='1'; uv run pytest -m online -v        # 在线探针（PowerShell，约 6 个请求配额）
RUN_ONLINE=1 uv run pytest -m online -v                # bash 写法
```

实测边界契约（2026-09-25，curl_cffi chrome 指纹 + 会话 cookie）：

| 边界 | 实测行为 |
|---|---|
| 不存在的子版块 | **200 空 Listing**（dist=0, after=None），**不是 404** → 采集器 1 次请求即自然终止，不死循环 |
| 不存在的用户 | 404 JSON `{"message","error"}` → `not_found_ok` 软跳过，进度 failed，不崩溃 |
| 不存在的帖子 | 404 → `RedditNotFound`，进度 failed |
| `limit=105` | 服务端钳到 100（客户端已自行钳制，双保险） |
| `after=None` | 翻页自然终止信号；采集器停发后续页 |
| morechildren 垃圾 children | 200 `{json:{errors:[],data:{things:[]}}}`（**things 在 json.data 下**） |
| morechildren 空批次 | 早停省配额；有真实数据时按 100/批全量展开 |
| 登出态大帖展开 | 6566 评论帖：顶层 500 + morechildren 5765 id 全展开 = 6077 条入库，进度 `done` |

账号边界细节：404 用户在 `run()` 层计入 `authors/skipped` 不计 `failed`；`[deleted]`/空串/NULL 作者不入作者名单；cookie 缺 `loid` 即视为无效。

指定板块翻页深度极限（2026-09-28 实测，r/AskReddit，limit=100/页）：

| 排序 | 实测可翻深度 | 终止方式 |
|---|---|---|
| new | **~998 帖（10 页）** | 服务端伪造自然末页（末页 98 行 + after=None）；并非数据尽头，即匿名 API 只回最近 ~1 天的新帖流 |
| top `t=all` | **~499 帖（5 页）** | 同样伪造末页截断；全历史高分帖只给前 500 |
| hot | 8 页 800 帖仍可继续 | 动态排名池，正常按窗口轮扫前几页即可 |
| search（`q=*&restrict_sr=on&sort=new&t=all`） | **~250 条（3 页）** | 匿名 search 比listing 更浅；且 **`after`/`before` 时间戳参数被服务端直接忽略**（传 2024-01/2023-01 切片返回的仍是最新 100 条，after 游标与不传时相同）→ 历史时间切片挖取不可行 |

结论：JSON 列表翻页的可达范围 = 最近 ~1 天 new 流 + top 前 500 + 动态 hot；**匿名 API 拿不到任意 subreddit 的全量历史帖**（listing 三层截断 + search 无时间切片，Pushshift 已于 2023-05 关停公众访问）。历史全量只能靠第三方归档语料（如 Academic Torrents Reddit dump，非实时）。

### 历史回补：Arctic Shift（实测 2026-09-28）

Pushshift 的免费继任者（单人维护），实测可用性：

| 验证点 | 实测结果 |
|---|---|
| 收费 | **完全免费**（无 API key、无注册）；PullPush 同类已收费化，弃用 |
| 数据起点 | 全站 2005-06-23（Reddit 上线当天，id=87）；各板块回溯到创建日 |
| 时间切片 | `after`/`before`（epoch/ISO 均可）真实生效，与官方 search 被跟忽略形成对照 |
| 限流 | 压测三档全过零 429：单线程 15 连发、并发 10 共 30 发、并发 15 共 60 发（~5 req/s 持续）；响应头有 `x-ratelimit-reset`（60s 窗口）但阈值未探到。官方无固定限额承诺（“随主机承受能力浮动”） |
| 字段 | 107 字段，与官方 API 同构 + 归档专有字段（`removed_by_category` 等） |

| 已知限制 | 说明 |
|---|---|
| 数据时效 | dump 落后 4-6 周（截至 2026-02），只能回溯不能监控——尾部靠现有调度器增量衔接 |
| score 口径 | 归档时刻快照，与当前 Reddit 不一致 → 回补后用官方 `/api/info.json` 批量刷新（实测 2008/2005 历史帖仍可查，100 id/请求） |
| 工程风险 | 单人维护无 SLA；2026 年 Reddit 已发 takedown 要求，要抓趁早 |

接口速查：`GET https://arctic-shift.photon-reddit.com/api/posts/search?subreddit=X&after=...&before=...&limit=100&sort=asc`；限速礼貌值建议 ≤2-3 req/s + 429 退避。

### 全量历史批量获取：dump 渠道实测（2026-09-28）

2020-01 ~ 2026-08 范围合计 **82 亿条**（submissions 20.3 亿 + comments 61.6 亿）。两条 dump 渠道确定性验证：

| 渠道 | 实测结论 |
|---|---|
| ModelScope `open-index/arctic` Parquet | 国内直连秒下（实测 53.5MB 分片）；**但是精简版**——submissions 仅 14 列、comments 仅 12 列（无 upvote_ratio/permalink/edited/gilded/awards 等）→ 不满足全字段需求；且缺月严重（submissions 缺 2022-08~2023-11，comments 停在 2022-07） |
| Academic Torrents BT（zst 全字段） | **国内直连可用，无需代理**（实测 aria2c + magnet：DHT 自举 15 节点、元数据 9 秒、稳定 5 seeds、真实下载 0.4~0.9 MiB/s 家庭宽）；academictorrents.com 网页本身 DNS 污染不可达，但 magnet + DHT + `tracker.opentrackr.org` 不依赖网页 |

- 种子文件结构：`reddit/{submissions,comments}/{RS,RC}_YYYY-MM.zst`（逐行 JSON，107 字段同 API）；单月种子 ≈ 69 GiB（comments 48 + submissions 21）
- 下载策略：2020~2023-12 从总 magnet（2005-06~2025-12，`btih:3d426c47c767d40f82c7ef0f47c3acacedd2bf44`）`--select-file` 挑 2020+ 文件；2024-01~2026-08 用月度种子（哈希见仓库 `download_links.md`）
- 82 亿条 ÷ 0.5~1 MiB/s ≈ 40~80 天单流；远程机器带宽 10 MB/s 时 ≈ 4~6 天（实测完整 zst 总量 **3485 GB / 3.4 TB**，含 stats.csv 缺月部分），可多月种子并行再提速

### 历史回补实现：backfill/ 模块（2026-09-28）

BT zst → 远程 PostgreSQL 的月度状态机管线，代码全部在 `backfill/`：

```
backfill/
├── magnets.py       # 一次性生成 magnets.json + torrents/ 元数据 + stats.csv 基准（已生成并入库）
├── magnets.json     # 总 magnet select-file 索引（144 文件/2.9TB）+ 8 个月度种子 + 88 个月对账基准
├── torrents/        # 9 个 .torrent 元数据（源消亡后的重灌能力资产，commit 进库）
├── gen_schema.py    # 生成 pg_schema.sql（posts/comments 月度 RANGE 分区×160 + import_log 状态机表）
├── pg_schema.sql    # 已生成（2020-01 ~ 2026-08，幂等可重跑补分区；含 ALTER 升级兼容已建库）
├── download.py      # aria2c 封装：渠道分目录 + 断点续传 + 三重完成判据（见下）
├── ingest.py        # zst 流式解压 → psycopg3 COPY（快/慢双路径 + 越界过滤 + TRUNCATE 幂等）
├── fetch_sample.py  # Arctic API 小样本直灌 PG（轻量验收：几千条，免整月 BT 下载）
└── pipeline.py      # 月度状态机 + --loop 无人值守 + docs/backfill_report.md 进度报告
```

**远程部署步骤（国内机器）**：

```bash
# 1) 依赖：aria2（BT）+ Python 3.12（uv sync 自动装 psycopg/zstandard）
apt install aria2

# 2) 配置 .env（复制 .env.example 后填远程 PG）
#    PG_DSN=postgresql://user:pass@host:5432/reddit
#    BT_DIR=/data/bt          # 单月峰值 ~70GB，用大盘
#    KEEP_ZST=0               # 入库后删 zst 省磁盘（1=保留冷备份）

# 3) 建库（幂等，重跑只补缺分区；已建库重跑会自动 ALTER 补新列）
psql "$PG_DSN" -f backfill/pg_schema.sql

# 4) 轻量验收（可选，几分钟）：API 拉几千条灌入当月分区，免整月 BT 下载先验全链路
uv run python -m backfill.fetch_sample --comments 2000 --posts 1000

# 5) 单月验收（先跑通全链路再放开；全月 ~22GB，10MB/s 带宽约 40 分钟）
uv run python -u -m backfill.pipeline --month 2020-01

# 6) 验收通过后放开全量无人值守（80 个月；失败月自动重试，全部到达终态后正常退出）
uv run python -u -m backfill.pipeline --loop --log-file logs/backfill.log

# 预检：列出每个月的数据源与体量（不下载不入库）
uv run python -m backfill.pipeline --from-month 2020-01 --to-month 2026-08 --dry-run
```

要点：
- **轻量验收（fetch_sample）**：BT 最小下载单元是整月（最小月 ~22GB），`backfill/fetch_sample.py` 用 Arctic Shift API（与 dump 同源、字段同构）拉几千条直灌当月分区，几分钟验证「建表 → 字段映射 → COPY 入库 → 分区路由」全链路。可重复运行（id 冲突自动跳过）、不写 import_log（样本 ≠ 月份完成）；`--comments/--posts/--month` 可调，详见 `--help`。API 必须直连（工具内置空代理，禁套代理）
- **无人值守（--loop）**：每轮跑完写 `docs/backfill_report.md`（状态汇总 + 逐月行数对账表 + 待处理清单）；仍有未完成单元时等 5 分钟（`--loop-wait` 可调）再进入下一轮（aria2 控制文件续传 + TRUNCATE 幂等重灌）；`failed` 月达 `--max-retries`（默认 10）后停下等人工；mismatch 不自动重试，人工处理后重置：
  ```sql
  UPDATE import_log SET status='pending', retries=0 WHERE type='...' AND month='...';
  ```
- **完成判据（三重）**：文件存在 + `st_size == 种子声明` + 渠道控制文件已删除。第三条是 sparse 坑的防线（实测：`--file-allocation=none` 下 BT 乱序分片 25 秒把 st_size 膨胀到 16GB 而实际只下了 ~20MB，尾部块早到时 st_size 会撞线声明值而数据不全）；aria2 完成后删 `reddit.aria2` 控制文件，残留即未完成
- **渠道分目录**：`BT_DIR/total/`（总种子 select-file）与 `BT_DIR/monthly/{YYYY-MM}/`（月度种子）分开下载——两类种子 name 都是 `reddit`，同目录下控制文件会互相覆盖/不匹配导致断点损坏
- **入库双路径**：快路径 COPY 直指月分区（全速）；若 zst 内部有重复 id 触发 PK 冲突，自动降级为临时表 + `ON CONFLICT DO NOTHING`（约 2 倍耗时但不会死循环失败）；`created_utc` 越界/缺失行被过滤并计入 `import_log.skipped_rows`（应为 0，非 0 说明该月源有脏行）
- **对账**：每月实际行数 vs `magnets.json` 里的 ModelScope stats.csv 基准（±0.5% 容差；stats.csv 本身缺月 2023 后部分，缺基准时仅验入库成功）；默认 `count(*)` 复核，赶时间可 `--no-recount` 直接信 COPY 提交行数
- **磁盘水位**：下载前检查可用空间 ≥ 单月声明×1.2，不足则本轮跳过（不标 failed），水位恢复后自动继续
- **分区裁剪**：查询带 `created_utc` 范围才走单分区；`subreddit+created_utc`/`author`/`link_id` 分区本地索引已建
- systemd 守护示例（`Restart=always`）见部署机上 `/etc/systemd/system/reddit-backfill.service`，命令同上第 5 步（`--loop` 模式全部终态后正常退出，Restart 不会空转拉起）
- 本地开发机**不要**跑非 `--dry-run` 的 pipeline（下载暂存 70GB/月会撑满盘）；入库/下载链路已在开发机用独立测试库 + 月度种子头部分片端到端验证过（快/慢路径、sparse 回归、幂等重灌）

---

## 持续采集与日容量

`scheduler.py` 按配额窗口（600s × 90 请求）循环跑单板块，用于实测“单账号一天能连续采集多少”：

```bash
$env:HTTP_PROXY=$null; $env:HTTPS_PROXY=$null;   # 必须先清（见下方坑）
uv run python scheduler.py --hours 24 --log-file logs/scheduler_24h.log   # 正式实测
uv run python scheduler.py --hours 0.01 --posts-per-window 2 --users-per-window 2 --no-expand-more   # 冒烟（~3 分钟）
```

> 部署坑（2026-09-28 实录）：harness 后台终端会被注入 `HTTP_PROXY/HTTPS_PROXY`（外部隧道代理），curl_cffi 默认 trust_env 会走它——隧道一失效全部请求 `curl: (35) Connection was reset`。**启动采集器前先清掉这组环境变量**（或显式传 `--proxy`），前台独立 PowerShell 窗口无此问题。

- 每窗口动作：listings 轮扫（hot/top:day/new 各 ≤8 页翻穿，~22 请求）→ posts 串行采评论（**逐帖检查窗口请求预算**：大帖 morechildren 批次弹性大，一个 5k more_ids 帖 ≈ 58 请求）→ users 采作者。
- 只采从未采过的帖（每帖一次评论快照），`truncated`/`failed` 不重烧配额；断点续采/幂等语义与 main.py 一致。
- 结束（到时/Ctrl+C/cookie 失效）自动写 `docs/capacity_report.md`：请求与新帖/评论/用户折算 24h、配额利用率、评论单帖成本、理论上限对照。

理论基线：匿名配额 ~100 请求/600s，令牌桶留边际 90/600s → **日上限 ≈ 12,960 请求**。r/popular 池子实测很小（hot≈501、top:day=795、new=540，三排序合计 1,587 唯一帖，~22 请求即翻穿一轮）→ listings 瓶颈是池子流速而非配额；把预算大头给评论展开才能最大化单账号日数据量。

---

## 数据与幂等

- 存储：SQLite（WAL），自然键（Reddit base36 id / name）幂等 `upsert`；原始 JSON 全量存 `raw_json`。
- 断点续传：`scrape_progress` 按 scope 记录（`list:{sub}:{sort}` / `comments:{t3_name}` / `user:{name}`），状态 `working/done/truncated/failed`；重跑自动跳过 `done`、从游标续采 `working`。
- 字段详见 [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md)。

---

## 合规

遵守 Reddit [API 条款](https://support.reddithelp.com/hc/en-us/articles/16160319875092) 与 `robots.txt`；默认低速、可配 `--limit`；仅采公开内容，不采登录墙后私密数据。请按需自行评估采集频率与用途合规性。

---

## 项目结构

```
reddit/
├── pyproject.toml          # uv 项目（Python 3.12，curl-cffi==0.7.4）
├── requirements.txt        # pip 兜底
├── uv.lock                 # 完整依赖锁定（uv sync 用）
├── README.md               # 本文件
├── DATABASE_SCHEMA.md      # SQLite 表与字段注释
├── .env.example            # 配置模板：代理 / 数据库 / OAuth / 调优（复制为 .env）
├── cookies.json            # 会话 cookie（运行期产物，已 gitignore）
├── seeds/subreddits.txt    # 种子子版块
├── main.py                 # CLI 编排：initdb/listings/posts/users/stats/all
├── scheduler.py            # 持续采集调度（配额窗口循环 + 容量报告）
├── docs/                   # 容量实测报告；backfill 进度报告（运行期生成）
├── tests/                  # 边界测试：44 离线 + 6 在线探针（pytest）
├── backfill/               # 历史全量回补：BT zst → PostgreSQL 月度状态机
│   ├── magnets.py          #   生成 magnets.json + torrents/（源清单）
│   ├── magnets.json        #   总 magnet select-file 索引 + 月度种子 + 对账基准
│   ├── torrents/           #   9 个 .torrent 元数据（源消亡后的重灌资产）
│   ├── gen_schema.py       #   生成 pg_schema.sql（分区表 + 中文注释）
│   ├── pg_schema.sql       #   PostgreSQL DDL（月度分区 ×160 + import_log 状态机）
│   ├── download.py         #   aria2c 封装：渠道分目录 + 断点续传 + 完成判据
│   ├── ingest.py           #   zst 流式解压 → psycopg3 COPY（快/慢双路径）
│   └── pipeline.py         #   月度状态机 + --loop 无人值守 + 进度报告
└── reddit_collector/
    ├── config.py           # 端点/UA/headers/限流/代理/路径/种子加载
    ├── client.py           # curl_cffi AsyncSession 封装：cookie/OAuth/限流/重试/403 升级链
    ├── ratelimit.py        # 异步令牌桶，按 x-ratelimit 校准
    ├── cookies.py          # cookie jar 加载/保存/Cookie 头导入
    ├── db.py               # SQLite schema + 幂等 upsert + 进度
    ├── parsers.py          # Listing/t1/t2/t3/t5 → 行归一化（评论树递归）
    ├── proxy.py            # 系统代理跟随 + 代理池轮换
    └── scrapers/
        ├── listings.py     # 帖子列表（t3）
        ├── posts.py        # 帖子详情 + 评论树（t1）+ morechildren
        └── users.py        # 用户资料（t2）
```
