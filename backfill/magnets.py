# -*- coding: utf-8 -*-
"""一次性生成 backfill 的静态元数据（联网，约 1-3 分钟）。

用法（需 aria2c 在 PATH，国内直连无需代理）:
    uv run python -m backfill.magnets

产物（全部 commit 进库，部署端无需重跑）:
    magnets.json   总 magnet + 月度种子哈希 + select-file 索引 + 每文件声明大小
    torrents/      总种子 + 月度种子 .torrent 元数据（源消亡后的重灌能力）
    stats.csv      ModelScope open-index/arctic 月度行数（入库对账基准）
"""

import base64
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests

HERE = Path(__file__).parent

REPO_MD_URL = ("https://api.github.com/repos/ArthurHeitmann/arctic_shift/"
               "contents/download_links.md")
STATS_CSV_URL = "https://www.modelscope.cn/api/v1/datasets/open-index/arctic/repo"

# BT 公共 tracker（国内直连实测可用；opentrackr 响应最快放首位）
TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
]

# 回补时间范围（与计划一致）
FROM_MONTH = (2020, 1)
TO_MONTH = (2026, 8)

# 总 magnet 覆盖 2005-06 ~ 2025-12；超出部分必须用月度种子
TOTAL_MAGNET_COVERS_TO = (2025, 12)


def month_range(begin: tuple, end: tuple) -> list:
    """[(year, month), ...] 闭区间迭代。"""
    out, (y, m) = [], begin
    while (y, m) <= end:
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def build_magnet(infohash: str) -> str:
    """magnet 链接 + URL 编码 tracker。"""
    tr = "".join(
        "&tr=" + t.replace(":", "%3A").replace("/", "%2F") for t in TRACKERS)
    return f"magnet:?xt=urn:btih:{infohash}{tr}"


# ── bencode（torrent 元文件解析，标准库手写）────────────────────

def bdecode(data: bytes):
    """解析 bencoded 字节流（torrent 元文件用，仅此场景不求完备）。"""
    pos = 0

    def parse():
        nonlocal pos
        c = data[pos:pos + 1]
        if c == b"i":
            end = data.index(b"e", pos)
            v = int(data[pos + 1:end])
            pos = end + 1
            return v
        if c == b"l":
            pos += 1
            out = []
            while data[pos:pos + 1] != b"e":
                out.append(parse())
            pos += 1
            return out
        if c == b"d":
            pos += 1
            out = {}
            while data[pos:pos + 1] != b"e":
                k = parse()
                out[k] = parse()
            pos += 1
            return out
        colon = data.index(b":", pos)
        n = int(data[pos:colon])
        s = data[colon + 1:colon + 1 + n]
        pos = colon + 1 + n
        return s

    return parse()


def torrent_files(torrent_path: Path) -> list:
    """文件表 [{path, size}]，多文件种子保持种子内相对路径。"""
    info = bdecode(torrent_path.read_bytes())[b"info"]
    if b"files" in info:
        return [{"path": "/".join(p.decode() for p in f[b"path"]),
                 "size": f[b"length"]} for f in info[b"files"]]
    return [{"path": info[b"name"].decode(), "size": info[b"length"]}]


def find_aria2c() -> str:
    """定位 aria2c：ARIA2C 环境变量 > PATH > winget 用户目录。"""
    env = os.environ.get("ARIA2C", "").strip()
    if env:
        return env
    found = shutil.which("aria2c")
    if found:
        return found
    candidates = [
        *Path.home().glob("AppData/Local/Microsoft/WinGet/Packages/*/aria2*/aria2c.exe"),
        Path("/usr/bin/aria2c"), Path("/usr/local/bin/aria2c"),
    ]
    for h in candidates:
        if h.exists():
            return str(h)
    sys.exit("找不到 aria2c：请安装（winget install aria2.aria2 / apt install aria2）"
             "或设 ARIA2C 环境变量指向可执行文件")


def fetch_metadata(magnet: str, infohash: str, out_dir: Path,
                   timeout: int = 300) -> Path:
    """aria2c 仅取元数据落 .torrent（DHT+tracker，实测 ~9s）。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{infohash}.torrent"
    if out.exists():
        return out
    subprocess.run(
        [find_aria2c(), "--bt-metadata-only", "--bt-save-metadata=true",
         "--enable-dht", "--dht-entry-point=router.bittorrent.com:6881",
         "--quiet=true", "--dir", str(out_dir),
         "--bt-stop-timeout", str(timeout), "--seed-time=0", magnet],
        check=True, timeout=timeout + 60,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not out.exists():
        raise RuntimeError(f"元数据获取失败（{infohash}，{timeout}s 无 peer）")
    return out


def fetch_download_links() -> str:
    r = requests.get(REPO_MD_URL, params={"ref": "master"}, timeout=30,
                     impersonate="chrome",
                     headers={"Accept": "application/vnd.github+json"})
    r.raise_for_status()
    return base64.b64decode(r.json()["content"]).decode("utf-8", "replace")


def parse_monthly_hashes(md_text: str) -> dict:
    """download_links.md 两段月度表（submissions 在前、comments 在后）。

    行形如 `| 2024-12 | [Academic Torrents](.../details/<hash>) | ... |`；
    月份 → infohash，两段都有则以后者（comments）覆盖——同月两表只存一个，
    实际下载用月份种子时两个文件都在同一个种子里（见验证输出）。
    """
    out = {}
    for month, h in re.findall(
            r"\|\s*(\d{4}-\d{2})\s*\|\s*\|?\s*\[Academic Torrents\]"
            r"\(https?://academictorrents\.com/details/([0-9a-f]{40})\)",
            md_text):
        out[month] = h
    return out


def fetch_stats_csv(out_path: Path) -> dict:
    """ModelScope stats.csv → 落盘 + 返回 {(type, year, month): count}。"""
    import csv
    import io
    r = requests.get(STATS_CSV_URL,
                     params={"Revision": "master", "FilePath": "stats.csv"},
                     timeout=60, impersonate="chrome")
    r.raise_for_status()
    out_path.write_text(r.text, encoding="utf-8")
    counts = {}
    for row in csv.DictReader(io.StringIO(r.text)):
        try:
            key = (row["type"], int(row["year"]), int(row["month"]))
        except (KeyError, ValueError):
            continue
        counts[key] = int(row["count"])
    return counts


def main() -> None:
    print("1/4 抓取 download_links.md 与 stats.csv …")
    md = fetch_download_links()
    monthly = parse_monthly_hashes(md)
    stats = fetch_stats_csv(HERE / "stats.csv")
    total_m = re.search(
        r"magnet:\?xt=urn:btih:([0-9a-f]{40})", md)
    if not total_m:
        sys.exit("总 magnet 未解析到")
    total_hash = total_m.group(1)

    months = month_range(FROM_MONTH, TO_MONTH)
    need_monthly = [m for m in months if m > TOTAL_MAGNET_COVERS_TO]
    missing = [f"{y}-{m:02d}" for y, m in need_monthly
               if f"{y}-{m:02d}" not in monthly]
    if missing:
        sys.exit(f"月度种子缺失（download_links.md 未覆盖）: {missing}")

    print(f"2/4 获取总种子元数据（{total_hash[:12]}…）…")
    tdir = HERE / "torrents"
    tp = fetch_metadata(build_magnet(total_hash), total_hash, tdir)
    files = torrent_files(tp)
    print(f"   总种子含 {len(files)} 个文件")

    # select-file 索引（1-based，aria2c 口径）：2020+ 的 RS/RC 月度文件
    wanted = {}
    for i, f in enumerate(files, start=1):
        mobj = re.search(r"/(RS|RC)_(\d{4})-(\d{2})\.zst$", f["path"])
        if not mobj:
            continue
        kind = "submissions" if mobj.group(1) == "RS" else "comments"
        ym = (int(mobj.group(2)), int(mobj.group(3)))
        if FROM_MONTH <= ym <= TOTAL_MAGNET_COVERS_TO:
            key = f"{kind}/{ym[0]:04d}-{ym[1]:02d}"
            wanted[key] = {"index": i, "size": f["size"],
                           "path": f["path"]}

    print(f"   范围内文件 {len(wanted)} 个 "
          f"({sum(v['size'] for v in wanted.values()) / 1e9:.0f} GB)")

    print("3/4 获取月度种子元数据 …")
    monthly_meta = {}
    for y, m in need_monthly:
        key = f"{y}-{m:02d}"
        h = monthly[key]
        print(f"   {key} → {h[:12]}…")
        mp = fetch_metadata(build_magnet(h), h, tdir)
        for f in torrent_files(mp):
            mobj = re.search(r"(RS|RC)_(\d{4})-(\d{2})\.zst$", f["path"])
            if mobj:
                kind = "submissions" if mobj.group(1) == "RS" else "comments"
                monthly_meta[f"{kind}/{key}"] = {
                    "infohash": h, "size": f["size"],
                    "path": f["path"], "torrent": mp.name}

    print("4/4 汇总 stats 对账基准 …")
    expected = {}
    for (t, y, m), cnt in stats.items():
        if (y, m) >= FROM_MONTH and t in ("submissions", "comments"):
            expected[f"{t}/{y:04d}-{m:02d}"] = cnt

    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "from_month": "%04d-%02d" % FROM_MONTH,
        "to_month": "%04d-%02d" % TO_MONTH,
        "trackers": TRACKERS,
        "total_magnet": {
            "infohash": total_hash,
            "magnet": build_magnet(total_hash),
            "torrent": tp.name,
            "select_file": wanted,
        },
        "monthly_torrents": monthly_meta,
        "expected_rows": expected,
    }
    # 重新生成时保留已有 wanted 的 path 字段兼容（老文件无 path）
    (HERE / "magnets.json").write_text(
        json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"完成：magnets.json / torrents/({len(list(tdir.glob('*.torrent')))} 个) / "
          f"stats.csv（{len(expected)} 个月度基准）")


if __name__ == "__main__":
    main()
