"""
息壤（Xi-Rang）每日数据拉取脚本

在本地电脑运行，拉取最新行情，保存为 CSV，然后 git push 到 GitHub。
服务器通过 git pull 获取最新数据。

用法（本地电脑）：
    python data/daily_fetch.py              # 拉取并推送
    python data/daily_fetch.py --no-push    # 只拉取不推送

数据流：
    本地电脑 (yfinance) → CSV → git push → GitHub → 服务器 git pull → daily_runner
"""

import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

DATA_DIR = Path(__file__).parent

# 所有需要拉取的标的（美股 + 中国）
TICKERS = {
    # 美股组合
    "SPY": "美股大盘",
    "TLT": "长期国债",
    "GLD": "黄金",
    "SHV": "现金",
    # 中国组合（通过 yfinance 拉取）
    "510300.SS": "沪深300",
    "511010.SS": "国债ETF",
    "518880.SS": "黄金ETF",
}

LOOKBACK_DAYS = 90  # 拉取最近 90 天，足够计算 30 日滚动指标


def fetch_and_save():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"息壤数据拉取 - {today}")
    print("=" * 50)

    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS + 10)

    all_data = {}

    for ticker, name in TICKERS.items():
        print(f"  {ticker} ({name})...", end=" ")
        try:
            df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
            if df.empty:
                print("⚠ 无数据")
                continue

            if isinstance(df.columns, pd.MultiIndex):
                adj = df["Adj Close"]
                if isinstance(adj, pd.DataFrame):
                    adj = adj.iloc[:, 0]
            else:
                adj = df["Adj Close"]

            all_data[ticker] = adj
            print(f"✓ {len(adj)} 条")
        except Exception as e:
            print(f"✗ {e}")

    if not all_data:
        print("✗ 未获取到任何数据")
        return False

    # 保存为 CSV
    prices = pd.DataFrame(all_data)
    prices.index.name = "date"

    # 美股组合
    us_cols = [c for c in ["SPY", "TLT", "GLD", "SHV"] if c in prices.columns]
    if us_cols:
        us_prices = prices[us_cols].dropna()
        us_path = DATA_DIR / "live_us.csv"
        us_prices.to_csv(us_path)
        print(f"\n  美股数据: {us_path} ({len(us_prices)} 行)")

    # 中国组合
    cn_cols = [c for c in ["510300.SS", "511010.SS", "518880.SS"] if c in prices.columns]
    if cn_cols:
        cn_prices = prices[cn_cols].dropna()
        cn_path = DATA_DIR / "live_cn.csv"
        cn_prices.to_csv(cn_path)
        print(f"  中国数据: {cn_path} ({len(cn_prices)} 行)")

    # 写入更新时间戳
    ts_path = DATA_DIR / "last_update.txt"
    ts_path.write_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print(f"\n✓ 数据拉取完成")
    return True


def git_push():
    """提交并推送到 GitHub"""
    print("\n推送到 GitHub...")
    try:
        subprocess.run(["git", "add", "data/live_us.csv", "data/live_cn.csv", "data/last_update.txt"],
                       check=True, capture_output=True)
        result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not result.stdout.strip():
            print("  无变更，跳过推送")
            return
        today = datetime.now().strftime("%Y-%m-%d")
        subprocess.run(["git", "commit", "-m", f"data: 每日行情更新 {today}"],
                       check=True, capture_output=True)
        subprocess.run(["git", "push"], check=True, capture_output=True)
        print("  ✓ 推送成功")
    except subprocess.CalledProcessError as e:
        print(f"  ✗ 推送失败: {e}")


if __name__ == "__main__":
    ok = fetch_and_save()
    if ok and "--no-push" not in sys.argv:
        git_push()
