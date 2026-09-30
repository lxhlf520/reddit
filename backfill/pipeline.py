# -*- coding: utf-8 -*-
"""月度状态机编排：下载 → 入库 → 对账 → 清理。

流程（单月单类型）:
    pending → downloading → downloaded → ingesting → verifying → done
                                  ↑ 失败/不完整 → retries+1 → 回 downloading

断点语义:
- done 的月跳过；downloading/downloaded/ingesting/verifying 的“半途”状态重启后
  从头跑该月（下载断点由 aria2 session 续传，入库幂等靠 TRUNCATE 重 COPY）
- mismatch（行数对不上）不自动重试，人工处理后 UPDATE import_log 重置状态

CLI:
    uv run python -u -m backfill.pipeline --month 2020-01        # 单月验收
    uv run python -u -m backfill.pipeline --loop                 # 无人值守（失败月自动重试）

无人值守（--loop）:
- 每轮结束写 docs/backfill_report.md 进度报告（从 import_log 快照）
- 仍有未完成单元时等 --loop-wait 秒进入下一轮（aria2 断点续传 / TRUNCATE 重 COPY）
- 达 --max-retries 的 failed 单元不再重试，等人工介入后重置状态
- 全部单元到达终态（done / mismatch / 达上限 failed）后正常退出（systemd 不再拉起）
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

HERE = Path(__file__).parent

log = logging.getLogger("backfill")

# 对账容差：zst 与 ModelScope stats.csv 计数口径差异（补录/删帖）
TOLERANCE = 0.005


def _now():
    return datetime.now(timezone.utc)


def load_env_cfg() -> dict:
    """PG_DSN / BT_DIR / KEEP_ZST（.env 兼容，环境变量优先）。"""
    env_file = HERE.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())
    return {
        "dsn": os.environ.get("PG_DSN", ""),
        "bt_dir": Path(os.environ.get("BT_DIR", HERE / "bt_data")),
        "keep_zst": os.environ.get("KEEP_ZST", "0").strip() in ("1", "true"),
    }


def month_keys(from_month: str, to_month: str) -> list:
    """["comments/2020-01", "submissions/2020-01", ...] 按（类型×月）交叉。"""
    y, m = int(from_month[:4]), int(from_month[5:7])
    ey, em = int(to_month[:4]), int(to_month[5:7])
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    keys = []
    for month in months:
        keys += [f"comments/{month}", f"submissions/{month}"]
    return keys


class Pipeline:
    def __init__(self, dsn: str, bt_dir: Path, magnets: dict,
                 max_retries: int = 10, recount: bool = True):
        self.dsn = dsn
        self.bt_dir = bt_dir
        self.magnets = magnets
        self.max_retries = max_retries
        self.recount = recount       # True: verify 用 count(*) 复核（慢但准）
        from backfill.download import BtDownloader
        self.dl = BtDownloader(bt_dir)

    # ── import_log 状态机 ─────────────────────────────────────

    def _upsert(self, key: str, **fields) -> None:
        kind, month = key.split("/")
        # started_at/finished_at 传 "now()" 语义 → 实际时间戳（参数绑定不做 SQL 函数求值）
        for ts_col in ("started_at", "finished_at"):
            if fields.get(ts_col) == "now()":
                fields[ts_col] = _now()
        cols = ["type", "month"] + list(fields)
        vals = [kind, month] + [fields[c] for c in fields]
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in fields)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO import_log ({', '.join(cols)}) "
                    f"VALUES ({placeholders}) "
                    f"ON CONFLICT (type, month) DO UPDATE SET {updates}",
                    vals)
            conn.commit()

    def _status(self, key: str) -> str | None:
        kind, month = key.split("/")
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM import_log WHERE type=%s AND month=%s",
                    (kind, month))
                row = cur.fetchone()
        return row[0] if row else None

    def _retries(self, key: str) -> int:
        kind, month = key.split("/")
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT retries FROM import_log WHERE type=%s AND month=%s",
                    (kind, month))
                row = cur.fetchone()
        return row[0] if row else 0

    def _count_rows(self, key: str) -> int:
        kind, month = key.split("/")
        table = "comments" if kind == "comments" else "posts"
        y, m = month.split("-")
        partition = f"{table}_{y}_{m}"
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {partition}")
                return cur.fetchone()[0]

    # ── 单元处理 ─────────────────────────────────────────────

    def source_of(self, key: str) -> dict:
        """月数据源：月度种子（2026-01+）或总种子 select-file。"""
        sf = self.magnets["total_magnet"]["select_file"]
        monthly = self.magnets.get("monthly_torrents", {})
        if key in monthly:
            m = monthly[key]
            return {"mode": "monthly", "infohash": m["infohash"],
                    "size": m["size"],
                    "torrent": HERE / "torrents" / m["torrent"],
                    "inner_path": m["path"]}
        return {"mode": "total", "size": sf[key]["size"],
                "inner_path": sf[key]["path"]}

    def ensure_downloaded(self, key: str) -> Path | None:
        """确保 zst 完整；返回文件路径或 None（本轮未完成）。"""
        src = self.source_of(key)
        declared = src["size"]
        mode = src["mode"]
        if self.dl.is_complete(key, declared, src.get("inner_path"), mode):
            return self.dl.zst_path(key, src.get("inner_path"), mode)
        # 磁盘水位：暂存单月 ~70GB + aria2 控制文件，留 20% 余量；
        # 水位不足时本轮跳过（不标 failed），下轮水位恢复自动续
        root = self.dl.channel_dir(mode, key.split("/")[1])
        probe = root
        while not probe.exists():
            probe = probe.parent
        free = shutil.disk_usage(probe).free
        if free < declared * 1.2:
            log.warning(f"{key}: 磁盘剩余 {free / 1e9:.0f} GB，"
                        f"低于该月需求 {declared * 1.2 / 1e9:.0f} GB，本轮跳过")
            return None
        self._upsert(key, status="downloading", infohash=src.get("infohash"),
                     declared_bytes=declared,
                     zst_path=str(self.dl.zst_path(key, src.get("inner_path"), mode)),
                     started_at="now()")
        log_path = self.bt_dir / "aria2.log"
        if mode == "monthly":
            ok = self.dl.ensure_from_monthly(key, src["torrent"], declared,
                                             log_path)
        else:
            ok = key in self.dl.ensure_from_total(
                self.magnets, [key], log_path)
        if not ok:
            return None
        p = self.dl.zst_path(key, src.get("inner_path"), mode)
        self._upsert(key, status="downloaded", zst_bytes=p.stat().st_size)
        return p

    def ingest(self, key: str, zst: Path) -> dict:
        """整月入库；返回 ingest_month 的统计 dict。

        异常直接抛出（由调用方统一标 failed + retries+1，避免重复计数）。
        """
        from backfill.ingest import ingest_month
        kind, month = key.split("/")
        self._upsert(key, status="ingesting")
        stats = ingest_month(self.dsn, zst, kind, month, log=log.info)
        self._upsert(key, status="verifying", rows=stats["rows"],
                     skipped_rows=stats["skipped_range"],
                     deduped_rows=stats["deduped"])
        return stats

    def verify(self, key: str, zst: Path, keep_zst: bool,
               copied_rows: int | None = None) -> str:
        """对账：实际行数 vs stats.csv 基准（±容差）。返回终态。

        recount=True 时用 count(*) 复核（索引扫描，单月几十秒，
        但独立验证 COPY 提交后的真实行数）；--no-recount 时直接信
        COPY 提交行数（单事务成功即准确），换取验证速度。
        """
        kind, month = key.split("/")
        expected = self.magnets.get("expected_rows", {}).get(key)
        if self.recount or copied_rows is None:
            actual = self._count_rows(key)
        else:
            actual = copied_rows
        if expected and abs(actual - expected) / expected > TOLERANCE:
            self._upsert(key, status="mismatch", rows=actual,
                         expected_rows=expected, finished_at="now()")
            return "mismatch"
        self._upsert(key, status="done", rows=actual,
                     expected_rows=expected, finished_at="now()")
        if not keep_zst:
            try:
                zst.unlink()
                log.info(f"{key}: zst 已清理（KEEP_ZST=0）")
            except OSError as e:
                log.warning(f"{key}: zst 清理失败 {e}")
        return "done"

    def process(self, key: str, keep_zst: bool) -> str:
        st = self._status(key)
        if st == "done":
            log.info(f"{key}: 已 done，跳过")
            return "done"
        if st == "mismatch":
            log.warning(f"{key}: mismatch 待人工处理，跳过")
            return "mismatch"
        if st == "failed" and self._retries(key) >= self.max_retries:
            log.warning(f"{key}: 失败 {self._retries(key)} 次达上限，"
                        f"跳过待人工（重置：UPDATE import_log SET status='pending'")
            return "failed"
        zst = self.ensure_downloaded(key)
        if not zst:
            self._upsert(key, retries=self._retries(key) + 1)
            log.warning(f"{key}: 本轮下载未完成（retries+1，下轮续传）")
            return "downloading"
        stats = self.ingest(key, zst)
        log.info(f"{key}: 入库 {stats['rows']:,} 行 / {stats['seconds']:.0f}s"
                 f"（越界 {stats['skipped_range']:,}，去重 {stats['deduped']:,}"
                 f"{'，降级路径' if stats['fallback'] else ''}）")
        return self.verify(key, zst, keep_zst, copied_rows=stats["rows"])


def write_report(dsn: str, out: Path) -> dict:
    """把 import_log 全量快照写成 docs/backfill_report.md。

    返回 {key: (status, retries)}，供 --loop 判断是否还有未完成单元。
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT type, month, status, rows, expected_rows, retries, "
            "skipped_rows, deduped_rows, error "
            "FROM import_log ORDER BY month, type")
        data = cur.fetchall()
    # 列序：type, month, status, rows, expected_rows, retries, ...
    statuses = {f"{t}/{m}": (s, retr)
                for t, m, s, _rows, _exp, retr, *_ in data}

    by_status: dict = {}
    for r in data:
        by_status[r[2]] = by_status.get(r[2], 0) + 1
    done = [r for r in data if r[2] == "done"]
    total_rows = sum(r[3] or 0 for r in done)
    total_skip = sum(r[6] or 0 for r in data)
    total_dedup = sum(r[7] or 0 for r in data)

    lines = [
        "# Reddit 历史回补进度", "",
        f"生成时间：{_now():%Y-%m-%d %H:%M:%S} UTC", "",
        "## 汇总", "",
        "| 状态 | 单元数 |", "|---|---|",
    ]
    for s in sorted(by_status):
        lines.append(f"| {s} | {by_status[s]} |")
    lines += [
        "", f"- done 入库总行数：**{total_rows:,}**",
        f"- 越界跳过行合计：{total_skip:,}"
        "（应为 0；非 0 说明该月 zst 有跨月/无 created_utc 脏行）",
        f"- 冲突去重行合计：{total_dedup:,}"
        "（非 0 说明走了 ON CONFLICT 降级路径，源文件含重复 id）", "",
        "## 明细", "",
        "| type | month | status | rows | expected | Δ% | retries |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for t, m, s, rows, exp, retr, skip, dedup, err in data:
        delta = (f"{(rows - exp) / exp * 100:+.2f}"
                 if rows and exp else "")
        lines.append(f"| {t} | {m} | {s} | {rows or 0:,} | "
                     f"{exp or 0:,} | {delta} | {retr or 0} |")
    todo = [r for r in data if r[2] != "done"]
    if todo:
        lines += ["", "## 待处理（非 done）", "",
                  "| type | month | status | retries | error |",
                  "|---|---|---|---:|---|"]
        for t, m, s, rows, exp, retr, skip, dedup, err in todo:
            lines.append(f"| {t} | {m} | {s} | {retr or 0} | "
                         f"{(err or '')[:80]} |")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return statuses


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Arctic Shift zst dump → PG 月度状态机回补")
    ap.add_argument("--from-month", default="2020-01")
    ap.add_argument("--to-month", default="2026-08")
    ap.add_argument("--month", help="只跑单月（YYYY-MM，覆盖区间）")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--loop", action="store_true",
                    help="无人值守：跑完一轮若有未完成单元，等待后自动再来一轮")
    ap.add_argument("--loop-wait", type=int, default=300,
                    help="--loop 轮间隔秒（默认 300）")
    ap.add_argument("--max-retries", type=int, default=10,
                    help="failed 单元重试上限，达到后等人工（默认 10）")
    ap.add_argument("--no-recount", action="store_true",
                    help="对账直接信 COPY 提交行数，跳过 count(*) 复核")
    ap.add_argument("--log-file", default=None)
    args = ap.parse_args()

    if args.log_file:
        os.makedirs(os.path.dirname(os.path.abspath(args.log_file)),
                    exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        filename=args.log_file, filemode="a",
        force=True)
    if not args.log_file:
        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    cfg = load_env_cfg()

    magnets = json.loads((HERE / "magnets.json").read_text(encoding="utf-8"))
    keys = (month_keys(args.month, args.month) if args.month
            else month_keys(args.from_month, args.to_month))
    # 每月先 comments 后 submissions（comments 体量大，优先灌）；
    # 超出 magnets.json 覆盖范围的月份安全跳过（避免 KeyError）
    lo, hi = magnets["from_month"], magnets["to_month"]
    dropped = [k for k in keys if not lo <= k.split("/")[1] <= hi]
    keys = [k for k in keys if lo <= k.split("/")[1] <= hi]
    if dropped:
        log.warning(f"{len(dropped)} 个月超出 magnets.json 覆盖（{lo}~{hi}）已跳过，"
                    f"如需扩展先重跑 backfill.magnets")

    if args.dry_run:
        p = Pipeline(cfg["dsn"], cfg["bt_dir"], magnets)
        total = 0
        for k in keys:
            src = p.source_of(k)
            total += src["size"]
            print(f"{k}: {src['mode']:8s} "
                  f"{src['size'] / 1e9:7.1f} GB  {src.get('inner_path')}")
        print(f"共 {len(keys)} 个（类型×月），总 {total / 1e9:.0f} GB")
        return

    if not cfg["dsn"]:
        sys.exit("PG_DSN 未配置（.env 或环境变量）")

    p = Pipeline(cfg["dsn"], cfg["bt_dir"], magnets,
                 max_retries=args.max_retries, recount=not args.no_recount)
    report_path = HERE.parent / "docs" / "backfill_report.md"
    round_no = 0
    while True:
        round_no += 1
        log.info(f"=== 第 {round_no} 轮开始（{len(keys)} 个单元）===")
        summary = {}
        for k in keys:
            try:
                st = p.process(k, cfg["keep_zst"])
            except Exception as e:
                # 统一失败计数：ingest/verify 抛出的异常都在这里标 failed + retries+1；
                # PG 本身不可达时 upsert 也会炸 —— 再包一层，保住循环等下轮
                try:
                    p._upsert(k, status="failed", error=str(e)[:2000],
                              retries=p._retries(k) + 1, finished_at="now()")
                except Exception:
                    log.error(f"{k}: 状态标记失败（PG 不可达？），"
                              f"import_log 保持原状态，下轮重试")
                log.error(f"{k}: 失败（retries+1）{e}")
                st = "failed"
            summary[st] = summary.get(st, 0) + 1
        log.info(f"第 {round_no} 轮汇总: {summary}")

        try:
            statuses = write_report(cfg["dsn"], report_path)
            log.info(f"进度报告已刷新 {report_path}")
        except Exception as e:
            statuses = {}
            log.warning(f"报告写入失败: {e}")

        if not args.loop:
            break
        # 未完成 = 非终态，且未达重试上限的 failed（达上限的等人工，不再空转）
        pending = []
        for k in keys:
            st, retr = statuses.get(k, ("pending", 0))
            if st in ("done", "mismatch"):
                continue
            if st == "failed" and retr >= args.max_retries:
                continue
            pending.append(k)
        if not pending:
            log.info("--loop：全部单元到达终态（done/mismatch/达上限 failed），退出")
            break
        log.info(f"--loop：{len(pending)} 个未完成，"
                 f"{args.loop_wait}s 后进入下一轮（断点续传/幂等重灌）")
        time.sleep(args.loop_wait)


if __name__ == "__main__":
    main()
