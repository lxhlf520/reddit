# 数据库结构（SQLite）

存储引擎：SQLite（WAL 模式，`PRAGMA synchronous=NORMAL`）。默认路径 `reddit.db`（可用 `DB_PATH` / `--db` 覆盖）。

设计原则：
- **按 Reddit thing kind 建表**：`t3→posts`、`t1→comments`、`t2→users`、`t5→subreddits`。
- **自然键幂等**：以 Reddit 的 base36 `id`/`name` 为主键，`INSERT ... ON CONFLICT(pk) DO UPDATE`，重复采集不产生脏数据。
- **原始留档**：每表 `raw_json` 完整保存源 JSON，便于后续补字段而无需重采。
- **采集时间**：每表 `collected_at`（Unix 秒，upsert 时刷新）。

Reddit kind 速查：

| kind | 含义 | 落表 |
|---|---|---|
| `Listing` | 容器（`data.children[]`, `after`, `before`, `dist`） | —（拆出 children） |
| `t3` | 帖子/链接 | `posts` |
| `t1` | 评论 | `comments` |
| `t2` | 用户 | `users` |
| `t5` | 子版块 | `subreddits` |
| `more` | 评论占位（待 `morechildren` 展开的 base36 id 列表） | 进度 `truncated` 标记 |

---

## posts（t3 帖子）

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | TEXT PK | t3 base36 id（不含 `t3_` 前缀） |
| `name` | TEXT | 全名 `t3_xxx` |
| `subreddit` | TEXT | 所属子版块（display_name） |
| `author` | TEXT | 作者用户名 |
| `title` | TEXT | 标题 |
| `selftext` | TEXT | 正文（自帖） |
| `score` | INTEGER | 净赞数 |
| `upvote_ratio` | REAL | 赞同比 0~1 |
| `num_comments` | INTEGER | 评论数（服务端计数） |
| `created_utc` | REAL | 创建时间（Unix 秒） |
| `permalink` | TEXT | 站内相对链接 `/r/.../comments/...` |
| `url` | TEXT | 目标 URL（优先 `url_overridden_by_dest`） |
| `domain` | TEXT | 来源域名 |
| `link_flair_text` | TEXT | 帖子 flair 文本 |
| `is_gallery` | INTEGER | 是否图集（0/1） |
| `over_18` | INTEGER | 是否 NSFW（0/1） |
| `stickied` | INTEGER | 是否置顶（0/1） |
| `media_metadata` | TEXT | 图集元数据 JSON 串 |
| `raw_json` | TEXT | 完整 t3 源 JSON |
| `collected_at` | REAL | 入库时间（Unix 秒） |

索引：`idx_posts_subreddit(subreddit)`、`idx_posts_author(author)`、`idx_posts_created_utc(created_utc)`。

---

## comments（t1 评论）

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | TEXT PK | t1 base36 id |
| `name` | TEXT | 全名 `t1_xxx` |
| `link_id` | TEXT | 所属帖 `t3_xxx` |
| `parent_id` | TEXT | 父节点 `t3_xxx`（顶层）或 `t1_xxx`（回复） |
| `author` | TEXT | 作者用户名 |
| `body` | TEXT | 评论正文 |
| `score` | INTEGER | 净赞数 |
| `created_utc` | REAL | 创建时间（Unix 秒） |
| `depth` | INTEGER | 树深度（0=顶层） |
| `subreddit` | TEXT | 所属子版块 |
| `raw_json` | TEXT | 完整 t1 源 JSON |
| `collected_at` | REAL | 入库时间（Unix 秒） |

索引：`idx_comments_link_id(link_id)`、`idx_comments_author(author)`。

> **评论树重建**：用 `parent_id` 自引用即可还原完整树形，`depth` 为冗余的便利字段。
> `morechildren` 展开的评论其 `depth` 取源 `data.depth`（缺省 0），树关系仍以 `parent_id` 为准。

---

## users（t2 用户）

| 字段 | 类型 | 说明 |
|---|---|---|
| `name` | TEXT PK | 用户名（不含 `u/` 前缀） |
| `id` | TEXT | t2 base36 id |
| `link_karma` | INTEGER | 发帖 karma |
| `comment_karma` | INTEGER | 评论 karma |
| `created_utc` | REAL | 注册时间（Unix 秒） |
| `is_employee` | INTEGER | 是否 Reddit 员工（0/1） |
| `has_verified_email` | INTEGER | 是否已验证邮箱（0/1） |
| `raw_json` | TEXT | 完整 t2 源 JSON |
| `collected_at` | REAL | 入库时间（Unix 秒） |

---

## subreddits（t5 子版块，可选）

仅用于种子校验；默认采集链路不写此表。

| 字段 | 类型 | 说明 |
|---|---|---|
| `display_name` | TEXT PK | 子版块名 |
| `id` | TEXT | t5 base36 id |
| `subscribers` | INTEGER | 订阅数 |
| `title` | TEXT | 标题 |
| `public_description` | TEXT | 公开描述 |
| `raw_json` | TEXT | 完整 t5 源 JSON |
| `collected_at` | REAL | 入库时间（Unix 秒） |

---

## scrape_progress（断点续传进度）

| 字段 | 类型 | 说明 |
|---|---|---|
| `scope` | TEXT PK | 采集范围标识（见下） |
| `after` | TEXT | 列表分页游标（`t3_xxx`）；非列表 scope 为 NULL |
| `state` | TEXT | `working`/`done`/`truncated`/`failed` |
| `detail` | TEXT | 附加信息（计数摘要、错误摘要等） |
| `updated_at` | REAL | 更新时间（Unix 秒） |

**scope 命名约定**：

| scope | 阶段 | 说明 |
|---|---|---|
| `list:{sub}:{sort}` | listings | 如 `list:popular:hot`；`after` 存翻页游标 |
| `comments:{t3_name}` | posts | 如 `comments:t3_1wpfoi4`；评论树采集进度 |
| `user:{name}` | users | 如 `user:spez` |

**state 语义**：

- `working`：进行中（listings 会随翻页持续更新 `after`，中断后可续采）。
- `done`：已完成 → 重跑默认**跳过**（`--force` 可重采）。
- `truncated`：帖子评论因登出态 `morechildren` 返回空而**截断**（顶层树已入库，深层未展开）；可切 OAuth 重采补全。
- `failed`：失败（404/网络耗尽/鉴权等），`detail` 存错误摘要。

---

## 幂等与重跑核对

`db._upsert()` 在写入前查询已存在主键，返回 `(inserted, updated)` 拆分：

- **首次采集**：`inserted=N, updated=0`
- **同数据重跑**（`--force`）：`inserted=0, updated=N`（自然键去重，无重复行）

`stats` 阶段输出各表行数与 `scrape_progress` 按 state 的分布，便于快速核对采集完整性。
