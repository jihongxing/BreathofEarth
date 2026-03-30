"""
息壤（Xi-Rang）数据加载器

职责：
1. 从 Yahoo Finance 拉取 ETF 日线数据（Adjusted Close）
2. 从 FRED 拉取 CPI 月度数据
3. 对齐交易日、处理缺失值
4. 输出干净的 CSV 供回测使用

ETF 标的：
- SPY: 美股大盘（S&P 500）
- TLT: 长期国债（20+ Year Treasury）
- GLD: 黄金
- SHV: 短期国债 / 现金等价物（2007年上市）

注意事项：
- 必须使用 Adjusted Close（复权价），不能用 Close
- SHV 2007年才上市，2005-2007期间用 ffill 或 BIL 替代
- CPI 是月频，ETF 是日频，MVP 阶段按月对齐核算
"""

import os
import sys
from pathlib import Path

import pandas as pd
import yfinance as yf
import pandas_datareader.data as web

# ── 配置 ──────────────────────────────────────────────

DATA_DIR = Path(__file__).parent
START_DATE = "2005-01-01"
END_DATE = "2025-12-31"

ETF_TICKERS = {
    "SPY": "美股大盘 S&P500",
    "TLT": "长期国债 20Y+",
    "GLD": "黄金",
    "SHV": "短期国债/现金",
}

# ── ETF 数据 ──────────────────────────────────────────


def fetch_etf_data() -> pd.DataFrame:
    """
    拉取所有 ETF 的 Adjusted Close 日线数据，对齐交易日。
    """
    print("正在从 Yahoo Finance 拉取 ETF 数据...")

    frames = {}
    for ticker, desc in ETF_TICKERS.items():
        print(f"  {ticker} ({desc})...", end=" ")
        df = yf.download(ticker, start=START_DATE, end=END_DATE, auto_adjust=False, progress=False)
        if df.empty:
            print("⚠ 无数据!")
            continue

        # yfinance 返回的列可能是 MultiIndex，统一处理
        if isinstance(df.columns, pd.MultiIndex):
            adj_close = df["Adj Close"]
            if isinstance(adj_close, pd.DataFrame):
                adj_close = adj_close.iloc[:, 0]
        else:
            adj_close = df["Adj Close"]

        frames[ticker] = adj_close
        print(f"✓ {len(adj_close)} 条记录 ({adj_close.index[0].date()} ~ {adj_close.index[-1].date()})")

    if not frames:
        print("✗ 未获取到任何 ETF 数据，请检查网络连接。")
        sys.exit(1)

    # 合并，以所有交易日的并集为索引
    combined = pd.DataFrame(frames)
    combined.index.name = "date"

    # SHV 2007年才上市，之前的缺失用前值填充
    # 如果最早期也没有值（SHV 完全没数据的区间），用 0 收益率处理
    # 先 ffill，再 bfill 处理最前面几行
    combined = combined.ffill().bfill()

    # 确认无 NaN
    nan_count = combined.isna().sum()
    if nan_count.any():
        print(f"⚠ 仍有缺失值:\n{nan_count[nan_count > 0]}")
        combined = combined.dropna()

    return combined


# ── CPI 数据 ──────────────────────────────────────────


def fetch_cpi_data() -> pd.DataFrame:
    """
    从 FRED 拉取 CPI-U 月度数据（CPIAUCSL）。
    返回月度 DataFrame，包含 CPI 值和月度通胀率。
    """
    print("正在从 FRED 拉取 CPI 数据...")

    try:
        cpi = web.DataReader("CPIAUCSL", "fred", START_DATE, END_DATE)
    except Exception as e:
        print(f"⚠ FRED 拉取失败: {e}")
        print("  尝试备用方案：从本地文件加载...")
        cpi_path = DATA_DIR / "cpi_manual.csv"
        if cpi_path.exists():
            cpi = pd.read_csv(cpi_path, index_col=0, parse_dates=True)
        else:
            print(f"  ✗ 备用文件 {cpi_path} 不存在。请手动从 FRED 下载 CPIAUCSL 数据。")
            print("    https://fred.stlouisfed.org/series/CPIAUCSL")
            sys.exit(1)

    cpi.columns = ["cpi"]
    cpi.index.name = "date"

    # 计算月度通胀率和年化通胀率
    cpi["cpi_mom"] = cpi["cpi"].pct_change(fill_method=None)  # 月环比
    cpi["cpi_yoy"] = cpi["cpi"].pct_change(12, fill_method=None)  # 年同比

    print(f"  ✓ {len(cpi)} 条记录 ({cpi.index[0].date()} ~ {cpi.index[-1].date()})")
    return cpi


# ── 月度对齐 ──────────────────────────────────────────


def build_monthly_dataset(etf_daily: pd.DataFrame, cpi_monthly: pd.DataFrame) -> pd.DataFrame:
    """
    将 ETF 日线数据降频到月末，与 CPI 月度数据对齐。
    MVP 阶段按月核算，足够简单也足够准确。
    """
    print("正在构建月度对齐数据集...")

    # ETF 取每月最后一个交易日的价格
    etf_monthly = etf_daily.resample("ME").last()

    # 计算月度收益率
    etf_returns = etf_monthly.pct_change()
    etf_returns.columns = [f"{col}_return" for col in etf_returns.columns]

    # 合并 ETF 价格 + 收益率 + CPI
    monthly = etf_monthly.join(etf_returns).join(cpi_monthly, how="left")

    # CPI ffill（某些月份可能还没发布）
    monthly["cpi"] = monthly["cpi"].ffill()
    monthly["cpi_yoy"] = monthly["cpi_yoy"].ffill()

    # 去掉最开头的 NaN 行（第一个月没有收益率）
    monthly = monthly.dropna(subset=[f"{list(ETF_TICKERS.keys())[0]}_return"])

    print(f"  ✓ {len(monthly)} 个月 ({monthly.index[0].date()} ~ {monthly.index[-1].date()})")
    return monthly


# ── 保存 ──────────────────────────────────────────────


def save_data(etf_daily: pd.DataFrame, cpi_monthly: pd.DataFrame, monthly: pd.DataFrame):
    """保存所有数据到 CSV。"""
    etf_path = DATA_DIR / "etf_daily.csv"
    cpi_path = DATA_DIR / "cpi_monthly.csv"
    monthly_path = DATA_DIR / "monthly_aligned.csv"

    etf_daily.to_csv(etf_path)
    cpi_monthly.to_csv(cpi_path)
    monthly.to_csv(monthly_path)

    print(f"\n数据已保存:")
    print(f"  {etf_path} ({os.path.getsize(etf_path) / 1024:.0f} KB)")
    print(f"  {cpi_path} ({os.path.getsize(cpi_path) / 1024:.0f} KB)")
    print(f"  {monthly_path} ({os.path.getsize(monthly_path) / 1024:.0f} KB)")


# ── 数据质量检查 ──────────────────────────────────────


def quality_check(etf_daily: pd.DataFrame, monthly: pd.DataFrame):
    """基础数据质量检查。"""
    print("\n── 数据质量检查 ──")

    # 1. 日期范围
    print(f"ETF 日线范围: {etf_daily.index[0].date()} ~ {etf_daily.index[-1].date()}")
    print(f"月度数据范围: {monthly.index[0].date()} ~ {monthly.index[-1].date()}")

    # 2. 缺失值
    daily_nan = etf_daily.isna().sum()
    if daily_nan.any():
        print(f"⚠ 日线缺失值: {daily_nan[daily_nan > 0].to_dict()}")
    else:
        print("✓ 日线数据无缺失值")

    # 3. 各 ETF 数据量
    for col in ETF_TICKERS:
        if col in etf_daily.columns:
            n = etf_daily[col].notna().sum()
            print(f"  {col}: {n} 个交易日")

    # 4. 简单合理性检查：价格应该都是正数
    for col in ETF_TICKERS:
        if col in etf_daily.columns:
            if (etf_daily[col] <= 0).any():
                print(f"⚠ {col} 存在非正价格!")
            else:
                first = etf_daily[col].iloc[0]
                last = etf_daily[col].iloc[-1]
                print(f"  {col}: {first:.2f} → {last:.2f} (总变化 {(last/first - 1)*100:+.1f}%)")

    print("── 检查完成 ──\n")


# ── 主入口 ────────────────────────────────────────────


def main():
    print("=" * 50)
    print("息壤（Xi-Rang）数据准备")
    print("=" * 50)
    print()

    etf_daily = fetch_etf_data()
    cpi_monthly = fetch_cpi_data()
    monthly = build_monthly_dataset(etf_daily, cpi_monthly)

    quality_check(etf_daily, monthly)
    save_data(etf_daily, cpi_monthly, monthly)

    print("\n✓ Phase 0 数据准备完成。可以开始 Phase 1 回测。")


if __name__ == "__main__":
    main()
