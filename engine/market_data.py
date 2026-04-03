"""
息壤（Xi-Rang）市场数据服务

数据加载策略（v2 - 本地优先，零限流）：
1. 优先读取本地 CSV（live_us.csv / live_cn.csv）
2. 本地没数据时，通过 DataManager 增量更新（带统一限流保护）
3. 永不直接裸调 API，所有网络请求都经过 DataManager

回测数据：直接使用 DataManager.load_local()，100% 不联网。
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

from engine.config import ASSETS, CORR_WINDOW

logger = logging.getLogger("xirang.market_data")


class DataFetchError(Exception):
    """数据拉取失败"""
    pass


class MarketDataService:

    def __init__(self, db=None, assets=None, data_source=None):
        self.db = db
        self.assets = assets or ASSETS
        self.data_source = data_source
        self._cache: Optional[pd.DataFrame] = None

    def fetch_latest(self, lookback_days: int = 60) -> pd.DataFrame:
        """
        获取最新市场数据。

        策略（v2）：
        1. 总是先尝试本地 CSV（零 API 调用）
        2. 只有本地没数据时，才���过 DataManager 增量更新
        3. DataManager 内部有统一限流，确保不被封
        """
        # 第一步：尝试本地 CSV
        try:
            return self._fetch_local_csv()
        except Exception as e:
            logger.warning(f"本地 CSV 读取失败: {e}")

        # 第二步：通过 DataManager 增量更新（带限流保护）
        logger.info("通过 DataManager 增量更新数据...")
        try:
            from data.data_manager import DataManager
            dm = DataManager(min_interval=2.0)

            is_cn = any(t.endswith(".SS") or t.endswith(".SZ") or t == "MONEY" for t in self.assets)
            if is_cn:
                dm.update_live()
                return self._fetch_local_csv()
            else:
                dm.update_live()
                return self._fetch_local_csv()
        except Exception as e2:
            raise DataFetchError(
                f"数据获取失败（本地和在线均失败）: {e2}\n"
                f"请先在本地运行: python -m data.data_manager --update-live"
            )

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
