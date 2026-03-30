"""
息壤（Xi-Rang）市场数据服务

双数据源策略：
1. 优先使用 yfinance（Yahoo Finance）
2. 如果被限流或网络不通，自动切换到 akshare（国内数据源）

akshare 通过新浪/东方财富等国内接口拉取美股 ETF 数据，
对中国大陆服务器完全友好，无需代理。
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import numpy as np

from engine.config import ASSETS, CORR_WINDOW

logger = logging.getLogger("xirang.market_data")

# akshare 对应的美股代码（与 yfinance 相同）
AKSHARE_SYMBOLS = {
    "SPY": "SPY",
    "TLT": "TLT",
    "GLD": "GLD",
    "SHV": "SHV",
}


class MarketDataService:
    """市场数据服务（双数据源）"""

    def __init__(self, db=None):
        self.db = db
        self._cache: Optional[pd.DataFrame] = None

    def fetch_latest(self, lookback_days: int = 60) -> pd.DataFrame:
        """
        拉取最近 N 天的 ETF 数据。

        优先 yfinance，失败后自动切换 akshare。
        可通过环境变量 XIRANG_DATA_SOURCE=akshare 强制使用 akshare。
        """
        force_source = os.environ.get("XIRANG_DATA_SOURCE", "").lower()

        if force_source == "akshare":
            return self._fetch_akshare(lookback_days)

        # 优先 yfinance
        try:
            return self._fetch_yfinance(lookback_days)
        except Exception as e:
            logger.warning(f"yfinance 失败: {e}，切换到 akshare...")
            return self._fetch_akshare(lookback_days)

    def _fetch_yfinance(self, lookback_days: int) -> pd.DataFrame:
        """从 Yahoo Finance 拉取"""
        import yfinance as yf

        end = datetime.now()
        start = end - timedelta(days=lookback_days + 10)

        frames = {}
        for ticker in ASSETS:
            df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
            if df.empty:
                raise RuntimeError(f"无法获取 {ticker} 数据，请检查网络连接")

            if isinstance(df.columns, pd.MultiIndex):
                adj_close = df["Adj Close"]
                if isinstance(adj_close, pd.DataFrame):
                    adj_close = adj_close.iloc[:, 0]
            else:
                adj_close = df["Adj Close"]

            frames[ticker] = adj_close

        prices = pd.DataFrame(frames).ffill().bfill()
        prices.index.name = "date"
        self._cache = prices
        logger.info("数据源: yfinance")
        return prices

    def _fetch_akshare(self, lookback_days: int) -> pd.DataFrame:
        """
        从 akshare 拉取美股 ETF 数据。

        akshare 通过国内接口获取美股数据，无需代理。
        """
        try:
            import akshare as ak
        except ImportError:
            raise RuntimeError(
                "akshare 未安装。请运行: pip3 install akshare\n"
                "akshare 是国内金融数据源，对中国大陆服务器友好，无需代理。"
            )

        end = datetime.now()
        start = end - timedelta(days=lookback_days + 10)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        frames = {}
        for ticker in ASSETS:
            ak_symbol = AKSHARE_SYMBOLS.get(ticker, ticker)
            try:
                # akshare 的美股日线接口
                df = ak.stock_us_daily(symbol=ak_symbol, adjust="qfq")
                if df.empty:
                    raise RuntimeError(f"akshare 无 {ticker} 数据")

                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date").sort_index()
                df = df[start_str:end_str]

                frames[ticker] = df["close"]
                logger.info(f"  akshare {ticker}: {len(df)} 条")
            except Exception as e:
                raise RuntimeError(f"akshare 获取 {ticker} 失败: {e}")

        prices = pd.DataFrame(frames).ffill().bfill()
        prices.index.name = "date"
        self._cache = prices
        logger.info("数据源: akshare")
        return prices

    def get_daily_returns(self, prices: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """计算日收益率"""
        if prices is None:
            prices = self._cache
        return prices.pct_change().fillna(0)

    def get_risk_indicators(self, prices: Optional[pd.DataFrame] = None) -> dict:
        """
        计算风控所需的指标（最新一天的值）。

        Returns:
            {
                "spy_tlt_corr": float,
                "spy_30d_ret": float,
                "tlt_30d_ret": float,
            }
        """
        if prices is None:
            prices = self._cache

        returns = prices.pct_change().fillna(0)

        spy_tlt_corr = returns["SPY"].rolling(window=CORR_WINDOW).corr(returns["TLT"])
        spy_30d_ret = prices["SPY"].pct_change(CORR_WINDOW)
        tlt_30d_ret = prices["TLT"].pct_change(CORR_WINDOW)

        return {
            "spy_tlt_corr": float(spy_tlt_corr.iloc[-1]) if not pd.isna(spy_tlt_corr.iloc[-1]) else 0.0,
            "spy_30d_ret": float(spy_30d_ret.iloc[-1]) if not pd.isna(spy_30d_ret.iloc[-1]) else 0.0,
            "tlt_30d_ret": float(tlt_30d_ret.iloc[-1]) if not pd.isna(tlt_30d_ret.iloc[-1]) else 0.0,
        }

    def get_today_returns(self, prices: Optional[pd.DataFrame] = None) -> np.ndarray:
        """获取最新一天的各资产收益率"""
        returns = self.get_daily_returns(prices)
        return returns.iloc[-1].values
