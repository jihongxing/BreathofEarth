"""
息壤（Xi-Rang）市场数据服务

双数据源 + 多市场支持：
- yfinance: 美股 ETF（优先）
- akshare: 美股 ETF 后备 + 中国 A 股 ETF
- 货币基金(MONEY): 年化 2% 模拟
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

from engine.config import ASSETS, CORR_WINDOW

logger = logging.getLogger("xirang.market_data")

AKSHARE_US_SYMBOLS = {"SPY": "SPY", "TLT": "TLT", "GLD": "GLD", "SHV": "SHV"}


class MarketDataService:

    def __init__(self, db=None, assets=None, data_source=None):
        self.db = db
        self.assets = assets or ASSETS
        self.data_source = data_source
        self._cache: Optional[pd.DataFrame] = None

    def fetch_latest(self, lookback_days: int = 60) -> pd.DataFrame:
        source = self.data_source or os.environ.get("XIRANG_DATA_SOURCE", "").lower()

        # 优先从本地 CSV 读取（服务器模式）
        if source == "local" or source == "":
            try:
                return self._fetch_local_csv()
            except Exception as e:
                logger.warning(f"本地 CSV 读取失败: {e}，尝试在线数据源...")

        if source == "akshare":
            return self._fetch_akshare_us(lookback_days)
        elif source == "akshare_cn":
            return self._fetch_akshare_cn(lookback_days)

        # 中国标的自动用 akshare_cn
        if any(t.endswith(".SS") or t.endswith(".SZ") or t == "MONEY" for t in self.assets):
            try:
                return self._fetch_local_csv()
            except Exception:
                return self._fetch_akshare_cn(lookback_days)

        try:
            return self._fetch_yfinance(lookback_days)
        except Exception as e:
            logger.warning(f"yfinance 失败: {e}，切换 akshare...")
            return self._fetch_akshare_us(lookback_days)

    def _fetch_local_csv(self) -> pd.DataFrame:
        """
        从本地 CSV 文件读取数据。

        服务器通过 git pull 获取由本地电脑推送的最新 CSV。
        文件路径：data/live_us.csv 或 data/live_cn.csv
        """
        data_dir = Path(__file__).parent.parent / "data"

        # 判断是美股还是中国标的
        is_cn = any(t.endswith(".SS") or t.endswith(".SZ") or t == "MONEY" for t in self.assets)
        csv_path = data_dir / ("live_cn.csv" if is_cn else "live_us.csv")

        if not csv_path.exists():
            raise FileNotFoundError(f"数据文件不存在: {csv_path}，请先在本地运行 python data/daily_fetch.py")

        # 检查数据新鲜度
        ts_path = data_dir / "last_update.txt"
        if ts_path.exists():
            last_update = ts_path.read_text().strip()
            logger.info(f"  数据最后更新: {last_update}")

        prices = pd.read_csv(csv_path, index_col="date", parse_dates=True)

        # 中国组合需要模拟货币基金
        if is_cn and "MONEY" in self.assets and "MONEY" not in prices.columns:
            daily_rate = (1.02 ** (1 / 252)) - 1
            money = pd.Series(index=prices.index, dtype=float)
            money.iloc[0] = 100.0
            for j in range(1, len(money)):
                money.iloc[j] = money.iloc[j - 1] * (1 + daily_rate)
            prices["MONEY"] = money

        # 确保所有需要的列都在
        missing = [a for a in self.assets if a not in prices.columns]
        if missing:
            raise RuntimeError(f"CSV 中缺少列: {missing}")

        prices = prices[self.assets].ffill().bfill().dropna()
        prices.index.name = "date"
        self._cache = prices
        logger.info(f"数据源: 本地 CSV ({csv_path.name}, {len(prices)} 行)")
        return prices

    def _fetch_yfinance(self, lookback_days):
        import yfinance as yf
        end = datetime.now()
        start = end - timedelta(days=lookback_days + 10)
        frames = {}
        for ticker in self.assets:
            df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
            if df.empty:
                raise RuntimeError(f"无法获取 {ticker}")
            if isinstance(df.columns, pd.MultiIndex):
                adj = df["Adj Close"]
                if isinstance(adj, pd.DataFrame): adj = adj.iloc[:, 0]
            else:
                adj = df["Adj Close"]
            frames[ticker] = adj
        prices = pd.DataFrame(frames).ffill().bfill()
        prices.index.name = "date"
        self._cache = prices
        logger.info("数据源: yfinance")
        return prices

    def _fetch_akshare_us(self, lookback_days):
        try:
            import akshare as ak
        except ImportError:
            raise RuntimeError("akshare 未安装: pip3 install akshare")
        end = datetime.now()
        start = end - timedelta(days=lookback_days + 10)
        s, e = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
        frames = {}
        for ticker in self.assets:
            sym = AKSHARE_US_SYMBOLS.get(ticker, ticker)
            df = ak.stock_us_daily(symbol=sym, adjust="qfq")
            if df.empty: raise RuntimeError(f"akshare 无 {ticker}")
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()[s:e]
            frames[ticker] = df["close"]
            logger.info(f"  akshare_us {ticker}: {len(df)} 条")
        prices = pd.DataFrame(frames).ffill().bfill()
        prices.index.name = "date"
        self._cache = prices
        logger.info("数据源: akshare (美股)")
        return prices

    def _fetch_akshare_cn(self, lookback_days):
        try:
            import akshare as ak
        except ImportError:
            raise RuntimeError("akshare 未安装: pip3 install akshare")
        end = datetime.now()
        start = end - timedelta(days=lookback_days + 10)
        s, e = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
        frames = {}
        for ticker in self.assets:
            if ticker == "MONEY": continue
            code = ticker.replace(".SS", "").replace(".SZ", "")
            try:
                df = ak.fund_etf_hist_em(symbol=code, period="daily",
                                         start_date=s, end_date=e, adjust="qfq")
                if df.empty: raise RuntimeError(f"无数据")
                df["日期"] = pd.to_datetime(df["日期"])
                df = df.set_index("日期").sort_index()
                frames[ticker] = df["收盘"].astype(float)
                logger.info(f"  akshare_cn {ticker}: {len(df)} 条")
            except Exception as ex:
                logger.warning(f"  akshare_cn {ticker} 失败，尝试 yfinance...")
                import yfinance as yf
                ydf = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
                if ydf.empty: raise RuntimeError(f"{ticker} 所有数据源失败")
                if isinstance(ydf.columns, pd.MultiIndex):
                    adj = ydf["Adj Close"]
                    if isinstance(adj, pd.DataFrame): adj = adj.iloc[:, 0]
                else:
                    adj = ydf["Adj Close"]
                frames[ticker] = adj
                logger.info(f"  yfinance {ticker}: {len(adj)} 条 (备用)")
        prices = pd.DataFrame(frames).ffill().bfill()
        if "MONEY" in self.assets:
            daily_rate = (1.02 ** (1/252)) - 1
            money = pd.Series(index=prices.index, dtype=float)
            money.iloc[0] = 100.0
            for j in range(1, len(money)):
                money.iloc[j] = money.iloc[j-1] * (1 + daily_rate)
            prices["MONEY"] = money
        prices = prices.dropna()
        prices.index.name = "date"
        self._cache = prices
        logger.info("数据源: akshare (A股)")
        return prices

    def get_daily_returns(self, prices=None):
        if prices is None: prices = self._cache
        return prices.pct_change().fillna(0)

    def get_risk_indicators(self, prices=None):
        if prices is None: prices = self._cache
        cols = list(prices.columns)
        stock, bond = cols[0], cols[1]
        ret = prices.pct_change().fillna(0)
        corr = ret[stock].rolling(CORR_WINDOW).corr(ret[bond])
        s30 = prices[stock].pct_change(CORR_WINDOW)
        b30 = prices[bond].pct_change(CORR_WINDOW)
        return {
            "spy_tlt_corr": float(corr.iloc[-1]) if not pd.isna(corr.iloc[-1]) else 0.0,
            "spy_30d_ret": float(s30.iloc[-1]) if not pd.isna(s30.iloc[-1]) else 0.0,
            "tlt_30d_ret": float(b30.iloc[-1]) if not pd.isna(b30.iloc[-1]) else 0.0,
        }

    def get_today_returns(self, prices=None):
        return self.get_daily_returns(prices).iloc[-1].values
