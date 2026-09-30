# -*- coding: utf-8 -*-
"""aria2c 下载封装：BT zst 月度文件 → 本地 BT_DIR。

设计（对应实测结论）:
- 国内直连：DHT 引导 router.bittorrent.com + 公共 UDP tracker，无需代理
- 断点续传：aria2 种子级控制文件（reddit.aria2）保存进度，重启自动续
- 总 magnet 用 --select-file 只下 2020+ 文件（跳过 2005-2019 的 ~1.9TB）
- 渠道隔离：total / monthly/{month} 分目录 —— 两类种子 name 都是 reddit，
  同目录下控制文件会互相覆盖/不匹配 → 断点损坏（实测确认月度种子也是
  reddit/comments + reddit/submissions 多文件结构）
- 完成检测（三重判据）：文件存在 + st_size == 声明 + 无控制文件。
  sparse 坑：--file-allocation=none 下 BT 乱序分片把文件 extend 到已写
  最大 offset，st_size 可提前达到 declared 而数据不全（实测 25s 内
  st_size 膨胀到 16GB 而实际只下了 ~20MB）；aria2 完成后删控制文件，
  残留即有未完成进度。

调用方式（pipeline 驱动）:
    from backfill.download import BtDownloader
    d = BtDownloader(bt_dir=Path("/data/bt"))
    d.ensure_from_total(magnets, ["comments/2020-01"], log)   # 总种子
    d.ensure_from_monthly("comments/2026-01", torrent, size, log)  # 月度种子
"""

import os
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).parent


def zst_filename(month_key: str) -> str:
    """月度键（"comments/2020-01"）→ 种子内文件名。"""
    kind, ym = month_key.split("/")
    prefix = "RS" if kind == "submissions" else "RC"
    return f"{prefix}_{ym}.zst"


def find_aria2c() -> str:
    """复用 magnets.find_aria2c 的探测逻辑（避免模块级循环导入）。"""
    from backfill.magnets import find_aria2c as _find
    return _find()


class BtDownloader:
    """单实例管理 aria2c 会话；每次 ensure_file 一轮（下载完自动退出）。"""

    def __init__(self, bt_dir: Path, aria2c: str = None,
                 max_concurrent: int = 4, timeout_s: int = 0):
        self.bt_dir = Path(bt_dir)
        # 目录延迟到实际下载时创建（dry-run 等无副作用）
        self.aria2c = aria2c or find_aria2c()
        self.max_concurrent = max_concurrent
        # 0 = 不限时（月文件大，交给外部调度/进程管理器控制生命周期）
        self.timeout_s = timeout_s
        self.session = self.bt_dir / "aria2.session"

    def channel_dir(self, mode: str, month: str = None) -> Path:
        """渠道工作目录：total / monthly/{month}（控制文件隔离的关键）。"""
        if mode == "monthly":
            return self.bt_dir / "monthly" / month
        return self.bt_dir / "total"

    def _ctrl_file(self, mode: str, month: str) -> Path:
        """种子级控制文件（两类种子的 name 都是 reddit）。"""
        return self.channel_dir(mode, month) / "reddit.aria2"

    def _base_cmd(self, workdir: Path) -> list:
        cmd = [
            self.aria2c,
            "--enable-dht",
            "--dht-entry-point=router.bittorrent.com:6881",
            "--dir", str(workdir),
            "--max-concurrent-downloads", str(self.max_concurrent),
            "--save-session", str(self.session),
            "--seed-time=0",                    # 下载完即退出，不做种（可改）
            "--file-allocation=none",           # 69GiB 预分配慢且无必要
            "--summary-interval=30",
            "--console-log-level=notice",
            "--bt-stop-timeout=300",            # 5 分钟无 peers 放弃本轮
        ]
        # 隔离宿主注入的失效代理（curl/aria2 都会读 HTTP(S)_PROXY）
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ.pop(k, None)
        return cmd

    def _run(self, cmd: list, log_path: Path) -> int:
        """同步跑 aria2c，输出直写日志文件（避免 PowerShell 管道截断坑）。

        返回退出码（0=aria2 自报完成；记入日志供排障，完成判定
        以 is_complete 三重判据为准，不单靠退出码）。
        """
        self.bt_dir.mkdir(parents=True, exist_ok=True)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "ab") as lf:
            lf.write(f"\n===== {time.strftime('%F %T')} {' '.join(cmd[1:])}\n"
                     .encode())
            lf.flush()
            try:
                r = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT,
                                   timeout=self.timeout_s or None)
                lf.write(f"===== exit {r.returncode}\n".encode())
                return r.returncode
            except subprocess.TimeoutExpired:
                # 超时仅终止本轮，断点由控制文件保留
                lf.write(b"===== timeout killed\n")
                return 124

    def ensure_from_total(self, magnets: dict, month_keys: list,
                          log_path: Path) -> list:
        """从总 magnet 用 select-file 批量补齐若干月（一次 aria2c 进程）。

        返回本轮完成的 month_keys（is_complete 三重判据）。已完整的月自动跳过。
        """
        sf = magnets["total_magnet"]["select_file"]
        pending, idxs = [], []
        for key in month_keys:
            if self.is_complete(key, sf[key]["size"], mode="total"):
                continue
            pending.append(key)
            idxs.append(str(sf[key]["index"]))
        if not pending:
            return []
        # 用已存 .torrent 启动（免每轮 DHT 重取元数据）
        torrent = HERE / "torrents" / magnets["total_magnet"]["torrent"]
        workdir = self.channel_dir("total")
        cmd = self._base_cmd(workdir) + [
            "--select-file", ",".join(idxs),
            str(torrent),
        ]
        self._run(cmd, log_path)
        return [k for k in pending
                if self.is_complete(k, sf[k]["size"], mode="total")]

    def ensure_from_monthly(self, month_key: str, torrent: Path,
                            declared: int, log_path: Path) -> bool:
        """月度种子（同月 RC+RS 两文件共用一个种子/目录）。

        不做 select-file：单键跑完整种子（两文件都要下，总时长不变），
        同月另一键届时 is_complete 直接命中，不再启动 aria2。
        """
        month = month_key.split("/")[1]
        if self.is_complete(month_key, declared, mode="monthly"):
            return True
        workdir = self.channel_dir("monthly", month)
        cmd = self._base_cmd(workdir) + [
            "--max-concurrent-downloads", "1",
            str(Path(torrent).resolve()),
        ]
        self._run(cmd, log_path)
        return self.is_complete(month_key, declared, mode="monthly")

    def zst_path(self, month_key: str, inner_path: str = None,
                 mode: str = None) -> Path:
        """月度文件在磁盘的落点（渠道感知 + 兼容旧直落点）。

        aria2c 多文件种子按种子内相对路径落盘（种子 name=reddit）：
            {bt_dir}/total/reddit/{kind}/RC_2020-01.zst      （总种子渠道）
            {bt_dir}/monthly/{month}/reddit/{kind}/RC_2026-01.zst（月度渠道）
        兼容旧落点：{bt_dir}/reddit/{kind}/... 、{bt_dir}/{inner_path}
        """
        kind = month_key.split("/")[0]
        month = month_key.split("/")[1]
        name = zst_filename(month_key)
        candidates = []
        if mode == "monthly":
            candidates.append(
                self.channel_dir("monthly", month) / "reddit" / kind / name)
        else:
            candidates.append(self.channel_dir("total") / "reddit" / kind / name)
        if inner_path:                     # 旧直落点（magnets.json 里的 path）
            candidates.append(self.bt_dir / inner_path)
        candidates.append(self.bt_dir / "reddit" / kind / name)
        candidates.append(self.bt_dir / name)
        for c in candidates:
            if c.exists():
                return c
        return candidates[0]               # 不存在时返回首选（日志/预填用）

    def is_complete(self, month_key: str, declared: int,
                    inner_path: str = None, mode: str = None) -> bool:
        """完成检测（三重判据）：

        1. 文件存在且 st_size == 种子声明
        2. 渠道控制文件（reddit.aria2）不存在 —— aria2 完成后删除它，
           残留即有未完成进度。

        第 2 条是 sparse 坑的关键防线：--file-allocation=none 下 BT 乱序
        分片会把文件 extend 到已写最大 offset（st_size 可提前达到 declared
        而数据不全，实测 25 秒膨胀到 16GB 而实际只下了 ~20MB），
        大小相等不等于完成。
        """
        month = month_key.split("/")[1]
        p = self.zst_path(month_key, inner_path, mode)
        if not (p.exists() and p.stat().st_size == declared):
            return False
        ctrl = self._ctrl_file(mode or "total", month)
        return not ctrl.exists()
