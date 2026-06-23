"""
息壤（Xi-Rang）每日数据拉取脚本

在本地电脑运行，拉取最新行情，保存为 CSV，然后 git push 到 GitHub。
服务器通过 git pull 获取最新数据。

现在使用统一的 DataManager，自动增量更新 + 限流保护。

用法（本地电脑）：
    python data/daily_fetch.py              # 拉取并推送
    python data/daily_fetch.py --no-push    # 只拉取不推送
    python data/daily_fetch.py --force      # 手动强制拉取

数据流：
    本地电脑 (DataManager) → CSV → git push → GitHub → 服务器 git pull → daily_runner
"""

import sys
import subprocess
import logging
import argparse
from datetime import datetime
from pathlib import Path

# 添加项目根目录到 sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.data_manager import DataManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def fetch_and_save(force: bool = False, allow_yfinance: bool = True):
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"息壤数据拉取 - {today}")
    print("=" * 50)

    dm = DataManager(min_interval=10.0, max_hourly=12, allow_yfinance=allow_yfinance)
    try:
        summary = dm.update_live(force=force)
        if summary.get("updated_files"):
            files = ", ".join(summary["updated_files"])
            print(f"\n✓ 数据更新完成: {files}")
        else:
            print("\n✓ 数据已是当前时段可用的最新版本，无需推送")
        if summary.get("cooldown_active"):
            print("  数据源仍在冷却期，本次未发起外部请求")
        return True, summary
    except Exception as e:
        print(f"\n✗ 数据拉取失败: {e}")
        return False, {}


def git_push():
    """提交并推送到 GitHub"""
    print("\n推送到 GitHub...")
    try:
        subprocess.run(
            [
                "git",
                "add",
                "data/live_us.csv",
                "data/live_cn.csv",
                "data/last_update.txt",
            ],
            check=True,
            capture_output=True,
        )
        diff = subprocess.run(
            ["git", "diff", "--cached", "--quiet"], capture_output=True
        )
        if diff.returncode == 0:
            print("  行情文件无 staged 变更，跳过推送")
            return
        if diff.returncode not in (0, 1):
            stderr = diff.stderr.decode("utf-8", errors="replace")
            raise subprocess.CalledProcessError(diff.returncode, diff.args, stderr=stderr)
        today = datetime.now().strftime("%Y-%m-%d")
        subprocess.run(
            ["git", "commit", "-m", f"data: 每日行情更新 {today}"],
            check=True,
            capture_output=True,
        )
        subprocess.run(["git", "push"], check=True, capture_output=True)
        print("  ✓ 推送成功")
    except subprocess.CalledProcessError as e:
        print(f"  ✗ 推送失败: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="息壤每日行情拉取与推送")
    parser.add_argument("--no-push", action="store_true", help="只更新本地 CSV，不推送 GitHub")
    parser.add_argument("--force", action="store_true", help="忽略交易时段判断和冷却状态")
    parser.add_argument(
        "--allow-yfinance",
        action="store_true",
        help="兼容旧参数；Yahoo/yfinance 现在默认开启",
    )
    parser.add_argument(
        "--no-yfinance",
        action="store_true",
        help="禁用 Yahoo/yfinance；美股 ETF 将只使用 akshare 非复权兜底",
    )
    args = parser.parse_args()

    ok, summary = fetch_and_save(
        force=args.force,
        allow_yfinance=not args.no_yfinance,
    )
    if ok and not args.no_push and summary.get("updated_files"):
        git_push()
