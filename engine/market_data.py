"""
息壤（Xi-Rang）市场数据服务

职责：
1. 从 Yahoo Finance 拉取最新 ETF 价格
2. 计算日收益率
3. 计算 SPY-TLT 滚动相关性和 30 日收益
4. 提供历史数据查询
"""

from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import numpy as np
import yfinance as yf

from engine.config import ASSETS, CORR_WINDOW


class MarketDataService:
    """市场数据服务"""

    def __init__(self, db=None):
        self.db = db
        self._cache: Optional[pd.DataFrame] = None

    def fetch_latest(self, lookback_days: int = 60) -> pd.DataFrame:
        """
        拉取最近 N 天的 ETF 数据。
        lookback_days 需要 >= 30 以计算滚动相关性。
        """
        end = datetime.now()
        start = end - timedelta(days=lookback_days + 10)  # 多拉几天防止节假日

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
                "spy_tlt_corr": float,  # 30日滚动相关性
                "spy_30d_ret": float,   # SPY 30日收益
                "tlt_30d_ret": float,   # TLT 30日收益
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
