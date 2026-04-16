"""
息壤（Xi-Rang）每日数据拉取脚本

在本地电脑运行，拉取最新行情，保存为 CSV，然后 git push 到 GitHub。
服务器通过 git pull 获取最新数据。

现在使用统一的 DataManager，自动增量更新 + 限流保护。

用法（本地电脑）：
    python data/daily_fetch.py              # 拉取并推送
    python data/daily_fetch.py --no-push    # 只拉取不推送

数据流：
    本地电脑 (DataManager) → CSV → git push → GitHub → 服务器 git pull → daily_runner
"""

import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path

# 添加项目根目录到 sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.data_manager import DataManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def fetch_and_save():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"息壤数据拉取 - {today}")
    print("=" * 50)

    dm = DataManager(min_interval=5.0, max_hourly=30)
    try:
        dm.update_live()
        print(f"\n✓ 数据拉取完成")
        return True
    except Exception as e:
        print(f"\n✗ 数据拉取失败: {e}")
        return False


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
        result = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True
        )
        if not result.stdout.strip():
            print("  无变更，跳过推送")
            return
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
    ok = fetch_and_save()
    if ok and "--no-push" not in sys.argv:
        git_push()
