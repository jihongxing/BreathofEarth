"""
息壤（Xi-Rang）统一数据管理器

核心目标：100% 避免限流，最小化 API 调用，所有回测使用本地数据。

设计原则：
1. 本地 CSV 是唯一真相来源（Single Source of Truth）
2. 增量更新：只拉缺失的日期，不重复拉已有数据
3. 统一限流：所有 API 调用经过同一个速率控制器
4. akshare 优先：中国用 sina/em，美股用 stock_us_daily，yfinance 仅作最终 fallback
5. 回测零 API：回测引擎直接读本地 CSV，永远不触网
6. 单 ticker 失败不崩溃：跳过继续，用本地缓存兜底

用法：
    from data.data_manager import DataManager

    dm = DataManager()

    # 增量更新（只拉缺失的天数）
    dm.update_all()

    # 回测专用（100% 本地，缺数据直接报错不联网）
    prices = dm.load_local("us")

    # 查看数据状态
    dm.status()
"""

import time
import logging
import hashlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger("xirang.data_manager")

DATA_DIR = Path(__file__).parent
RAW_DIR = DATA_DIR / "raw"
STATUS_FILE = DATA_DIR / "data_status.json"

# ── 市场配置 ──────────────────────────────────────────

MARKET_CONFIGS = {
    "us": {
        "file": "market_us.csv",
        "tickers": ["SPY", "TLT", "GLD", "SHV"],
        "start": "2005-01-01",
        "money": False,
        "description": "美股四资产永久组合",
    },
    "cn": {
        "file": "market_cn.csv",
        "tickers": ["510300.SS", "511010.SS", "518880.SS"],
        "start": "2012-01-01",
        "money": True,
        "description": "中国A股四资产组合",
    },
    "chimerica": {
        "file": "market_chimerica.csv",
        "tickers": ["513500.SS", "511010.SS", "518880.SS"],
        "start": "2012-01-01",
        "money": True,
        "description": "中美混血配置",
    },
    "eu": {
        "file": "market_eu.csv",
        "tickers": ["EZU", "BWX", "GLD", "SHV"],
        "start": "2005-01-01",
        "money": False,
        "description": "欧洲市场",
    },
    "india": {
        "file": "market_india.csv",
        "tickers": ["EPI", "EMB", "GLD", "SHV"],
        "start": "2007-01-01",
        "money": False,
        "description": "印度市场",
    },
    "global": {
        "file": "market_global.csv",
        "tickers": ["VT", "BWX", "GLD", "SHV"],
        "start": "2007-01-01",
        "money": False,
        "description": "全球配置",
    },
}

# 所有 live 数据配置（每日更新用）
LIVE_CONFIGS = {
    "live_us": {
        "file": "live_us.csv",
        "tickers": ["SPY", "TLT", "GLD", "SHV"],
        "lookback_days": 90,
    },
    "live_cn": {
        "file": "live_cn.csv",
        "tickers": ["510300.SS", "511010.SS", "513500.SS", "518880.SS"],
        "lookback_days": 90,
    },
}


# ── 限流控制器 ────────────────────────────────────────


class RateLimiter:
    """
    统一限流控制器。

    - 基础请求间隔 ≥ min_interval 秒
    - 被限流后指数退避：30s → 60s → 120s → ... → max_backoff
    - 每小时总请求数限制
    - 成功后立即恢复正常间隔
    """

    def __init__(
        self,
        min_interval: float = 3.0,
        max_hourly_requests: int = 60,
        max_backoff: float = 600.0,
    ):
        self.min_interval = min_interval
        self.max_hourly_requests = max_hourly_requests
        self.max_backoff = max_backoff
        self._last_request_time = 0.0
        self._hourly_count = 0
        self._hourly_reset_time = time.time()
        self._consecutive_failures = 0

    def wait(self):
        """在发起 API 请求前调用，自动等待到合适时机。"""
        now = time.time()

        # 重置小时计数器
        if now - self._hourly_reset_time >= 3600:
            self._hourly_count = 0
            self._hourly_reset_time = now

        # 每小时请求数检查
        if self._hourly_count >= self.max_hourly_requests:
            wait_time = 3600 - (now - self._hourly_reset_time)
            if wait_time > 0:
                logger.warning(
                    f"每小时请求数已达上限 ({self.max_hourly_requests})，等待 {wait_time:.0f}s"
                )
                time.sleep(wait_time)
                self._hourly_count = 0
                self._hourly_reset_time = time.time()

        # 计算需要等待的间隔
        elapsed = now - self._last_request_time
        if self._consecutive_failures > 0:
            # 限流退避：30s 起步，指数增长
            backoff = min(
                self.max_backoff, 30.0 * (2 ** (self._consecutive_failures - 1))
            )
            logger.info(
                f"限流退避: 等待 {backoff:.0f}s (连续失败 {self._consecutive_failures} 次)"
            )
            time.sleep(backoff)
        elif elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

        self._last_request_time = time.time()
        self._hourly_count += 1

    def report_success(self):
        """API 调用成功后调用，立即恢复正常间隔。"""
        self._consecutive_failures = 0

    def report_failure(self, is_rate_limit: bool = False):
        """API 调用失败后调用。"""
        self._consecutive_failures += 1
        if is_rate_limit:
            # 限流错误额外惩罚
            self._consecutive_failures = max(self._consecutive_failures, 2)

    @property
    def stats(self) -> dict:
        return {
            "hourly_count": self._hourly_count,
            "hourly_limit": self.max_hourly_requests,
            "consecutive_failures": self._consecutive_failures,
        }


# ── 辅助函数 ─────────────────────────────────────────


def _is_rate_limit_error(e: Exception) -> bool:
    """判断异常是否是限流错误。"""
    msg = str(e).lower()
    return any(
        kw in msg for kw in ["rate limit", "too many requests", "429", "ratelimit"]
    )


def _clear_proxy():
    """临时清除代理环境变量，返回备份。"""
    keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]
    saved = {}
    for k in keys:
        v = os.environ.pop(k, None)
        if v is not None:
            saved[k] = v
    return saved


def _restore_proxy(saved: dict):
    """恢复代理环境变量。"""
    os.environ.update(saved)


# ── 数据源适配器 ──────────────────────────────────────


class DataSource:
    """
    统一数据源接口。

    数据源优先级（akshare 优先，完全避免 yfinance 限流）：
    - 中国标的：akshare_sina → akshare_em
    - 美股标的：akshare_us_daily → akshare_us_hist → yfinance（最终 fallback）

    yfinance 仅作为最后的兜底手段，正常流程不会触发。
    """

    def __init__(self, rate_limiter: RateLimiter):
        self.rl = rate_limiter

    def fetch_ticker(self, ticker: str, start: str, end: str) -> pd.Series:
        """
        拉取单个 ticker 的日线数据。

        中国标的：akshare_sina → akshare_em（不用 yfinance）
        美股标的：akshare_us_daily → akshare_us_hist → yfinance（最终 fallback）
        """
        is_cn = ticker.endswith(".SS") or ticker.endswith(".SZ")

        if is_cn:
            # 中国标的：纯 akshare，不走 yfinance
            errors = []
            for attempt in range(2):
                try:
                    return self._fetch_akshare_cn(ticker, start, end)
                except Exception as e:
                    errors.append(f"akshare_cn[{attempt + 1}]: {e}")
                    if _is_rate_limit_error(e):
                        self.rl.report_failure(is_rate_limit=True)
            raise RuntimeError(f"{ticker} 所有数据源失败:\n  " + "\n  ".join(errors))
        else:
            # 美股标的：akshare 优先，yfinance 最终 fallback
            errors = []
            # 第一优先：akshare（重试2次）
            for attempt in range(2):
                try:
                    return self._fetch_akshare_us(ticker, start, end)
                except Exception as e:
                    errors.append(f"akshare_us[{attempt + 1}]: {e}")
            # 最终 fallback：yfinance（仅1次尝试，失败就放弃）
            try:
                logger.info(f"    akshare 全部失败，尝试 yfinance fallback...")
                return self._fetch_yfinance(ticker, start, end)
            except Exception as e:
                errors.append(f"yfinance_fallback: {e}")
                if _is_rate_limit_error(e):
                    self.rl.report_failure(is_rate_limit=True)
            raise RuntimeError(f"{ticker} 所有数据源失败:\n  " + "\n  ".join(errors))

    def _fetch_yfinance(self, ticker: str, start: str, end: str) -> pd.Series:
        """通过 yfinance 拉取数据（最终 fallback，仅在 akshare 全部失败时使用）。"""
        import yfinance as yf

        self.rl.wait()
        saved = _clear_proxy()
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        finally:
            _restore_proxy(saved)

        if df.empty:
            self.rl.report_failure(is_rate_limit=True)  # 空结果通常是限流
            raise RuntimeError(f"yfinance 返回空数据: {ticker}")

        if isinstance(df.columns, pd.MultiIndex):
            s = df["Adj Close"]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
        else:
            s = df["Adj Close"]

        s = s.dropna()
        if s.empty:
            raise RuntimeError(f"yfinance Adj Close 为空: {ticker}")

        s.name = ticker
        s.index.name = "date"
        self.rl.report_success()
        logger.info(f"    yfinance ✓ {ticker}: {len(s)} 行")
        return s

    def _fetch_akshare_cn(self, ticker: str, start: str, end: str) -> pd.Series:
        """通过 akshare 拉取中国 ETF 数据。优先 sina 接口（无限流）。"""
        import akshare as ak

        code = ticker.replace(".SS", "").replace(".SZ", "")
        prefix = "sh" if ticker.endswith(".SS") else "sz"

        self.rl.wait()
        try:
            # 方法1: fund_etf_hist_sina（新浪源，通常无限流）
            try:
                df = ak.fund_etf_hist_sina(symbol=f"{prefix}{code}")
                if not df.empty:
                    df["date"] = pd.to_datetime(df["date"])
                    s = df.set_index("date").sort_index()["close"].astype(float)
                    s = s[
                        (s.index >= pd.Timestamp(start))
                        & (s.index <= pd.Timestamp(end))
                    ].dropna()
                    if not s.empty:
                        s.name = ticker
                        s.index.name = "date"
                        self.rl.report_success()
                        logger.info(f"    akshare_sina ✓ {ticker}: {len(s)} 行")
                        return s
            except Exception:
                pass  # sina 失败，继续尝试 em

            # 方法2: fund_etf_hist_em（东方财富源）
            s_fmt = pd.Timestamp(start).strftime("%Y%m%d")
            e_fmt = pd.Timestamp(end).strftime("%Y%m%d")
            df = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=s_fmt,
                end_date=e_fmt,
                adjust="qfq",
            )
            if df.empty:
                raise RuntimeError(f"akshare 返回空数据: {ticker}")

            df["日期"] = pd.to_datetime(df["日期"])
            s = df.set_index("日期").sort_index()["收盘"].astype(float).dropna()
            if s.empty:
                raise RuntimeError(f"akshare 收盘价为空: {ticker}")

            s.name = ticker
            s.index.name = "date"
            self.rl.report_success()
            logger.info(f"    akshare_em ✓ {ticker}: {len(s)} 行")
            return s

        except Exception as e:
            self.rl.report_failure(is_rate_limit=_is_rate_limit_error(e))
            raise

    def _fetch_akshare_us(self, ticker: str, start: str, end: str) -> pd.Series:
        """
        通过 akshare 拉取美股数据。

        优先使用 stock_us_daily（实测更稳定，无网络问题），
        失败再尝试 stock_us_hist。
        """
        import akshare as ak

        self.rl.wait()
        try:
            # 方法1: stock_us_daily（实测最稳定，通过新浪源）
            try:
                df = ak.stock_us_daily(symbol=ticker, adjust="qfq")
                if df is not None and not df.empty:
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.set_index("date").sort_index()
                    s = (
                        df["close"][
                            (df.index >= pd.Timestamp(start))
                            & (df.index <= pd.Timestamp(end))
                        ]
                        .astype(float)
                        .dropna()
                    )
                    if not s.empty:
                        s.name = ticker
                        s.index.name = "date"
                        self.rl.report_success()
                        logger.info(f"    akshare_us_daily ✓ {ticker}: {len(s)} 行")
                        return s
            except Exception as e1:
                logger.debug(f"    akshare_us_daily failed: {e1}")

            # 方法2: stock_us_hist（东方财富源，网络不稳定时可能超时）
            try:
                df = ak.stock_us_hist(
                    symbol=ticker,
                    period="daily",
                    start_date=start.replace("-", ""),
                    end_date=end.replace("-", ""),
                    adjust="qfq",
                )
                if df is not None and not df.empty:
                    date_col = "日期" if "日期" in df.columns else "date"
                    close_col = "收盘" if "收盘" in df.columns else "close"
                    df[date_col] = pd.to_datetime(df[date_col])
                    s = (
                        df.set_index(date_col)
                        .sort_index()[close_col]
                        .astype(float)
                        .dropna()
                    )
                    if not s.empty:
                        s.name = ticker
                        s.index.name = "date"
                        self.rl.report_success()
                        logger.info(f"    akshare_us_hist ✓ {ticker}: {len(s)} 行")
                        return s
            except Exception as e2:
                logger.debug(f"    akshare_us_hist failed: {e2}")

            raise RuntimeError(f"akshare 所有美股接口均失败: {ticker}")

        except Exception as e:
            self.rl.report_failure(is_rate_limit=_is_rate_limit_error(e))
            raise


# ── 数据管理器 ────────────────────────────────────────


class DataManager:
    """
    统一数据管理器。

    职责：
    1. 管理本地 CSV 缓存（data/raw/*.csv）
    2. 智能增量更新（只拉缺失日期）
    3. 组装市场数据文件（data/market_*.csv）
    4. 提供回测数据加载（100% 本地）
    5. 单 ticker 失败不崩溃（跳过 + 用本地缓存兜底）
    """

    def __init__(self, min_interval: float = 3.0, max_hourly: int = 60):
        self.rl = RateLimiter(min_interval=min_interval, max_hourly_requests=max_hourly)
        self.source = DataSource(self.rl)
        RAW_DIR.mkdir(parents=True, exist_ok=True)

    # ── 本地数据加载（回测专用，零 API 调用）─────────

    def load_local(self, market: str) -> pd.DataFrame:
        """
        加载本地市场数据，100% 不联网。

        回测引擎调用此方法，如果本地没数据直接报错。
        """
        cfg = MARKET_CONFIGS.get(market)
        if cfg is None:
            raise ValueError(f"未知市场: {market}，可选: {list(MARKET_CONFIGS.keys())}")

        csv_path = DATA_DIR / cfg["file"]
        if not csv_path.exists():
            raise FileNotFoundError(
                f"市场数据文件不存在: {csv_path}\n"
                f"请先运行: python -m data.data_manager --update {market}"
            )

        prices = pd.read_csv(csv_path, index_col="date", parse_dates=True).sort_index()
        logger.info(f"加载本地数据: {market} ({csv_path.name}, {len(prices)} 行)")
        return prices

    def load_live(self, portfolio: str) -> pd.DataFrame:
        """加载 live 数据（每日运行用），100% 本地。"""
        key = f"live_{portfolio}"
        cfg = LIVE_CONFIGS.get(key)
        if cfg is None:
            raise ValueError(f"未知 live 配置: {key}")

        csv_path = DATA_DIR / cfg["file"]
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Live 数据文件不存在: {csv_path}\n"
                f"请先运行: python -m data.data_manager --update-live"
            )

        prices = pd.read_csv(csv_path, index_col="date", parse_dates=True).sort_index()

        # 中国组合模拟货币基金
        if portfolio == "cn" and "MONEY" not in prices.columns:
            daily_rate = (1.02 ** (1 / 252)) - 1
            money = pd.Series(index=prices.index, dtype=float)
            money.iloc[0] = 100.0
            for j in range(1, len(money)):
                money.iloc[j] = money.iloc[j - 1] * (1 + daily_rate)
            prices["MONEY"] = money

        return prices

    # ── 从已有市场 CSV 提取 raw 缓存 ─────────────────

    def bootstrap_raw_from_existing(self):
        """
        从已有的 market_*.csv / etf_daily.csv 提取 raw 缓存。

        这样后续增量更新只需拉少量新数据，不用从头下载。
        避免了大量 API 调用导致的限流。
        """
        count = 0

        # 从 etf_daily.csv 和 market CSV 提取
        # 策略：如果 raw 缓存不存在或比已有 CSV 短，则覆盖/合并
        csv_sources: list[tuple[str, Path]] = []
        etf_path = DATA_DIR / "etf_daily.csv"
        if etf_path.exists():
            csv_sources.append(("etf_daily.csv", etf_path))
        for mname, cfg in MARKET_CONFIGS.items():
            csv_path = DATA_DIR / cfg["file"]
            if csv_path.exists():
                csv_sources.append((cfg["file"], csv_path))

        for src_name, src_path in csv_sources:
            df = pd.read_csv(src_path, index_col="date", parse_dates=True).sort_index()
            for col in df.columns:
                if col == "MONEY":
                    continue
                raw_path = RAW_DIR / f"{col}.csv"
                s = df[col].dropna()
                if s.empty:
                    continue
                cached = self._load_raw(raw_path, col)
                if cached is None or cached.empty:
                    s.name = col
                    self._save_raw(s, raw_path)
                    logger.info(f"  从 {src_name} 提取: {col} ({len(s)} 行)")
                    count += 1
                elif len(s) > len(cached) * 1.1:
                    # CSV 源数据明显更多，合并
                    merged = pd.concat([s, cached]).sort_index()
                    merged = merged[~merged.index.duplicated(keep="last")].dropna()
                    merged.name = col
                    self._save_raw(merged, raw_path)
                    logger.info(
                        f"  从 {src_name} 补充: {col} ({len(cached)} → {len(merged)} 行)"
                    )
                    count += 1

        # 从 live CSV 提取（补充最新数据到 raw）
        for key, cfg in LIVE_CONFIGS.items():
            csv_path = DATA_DIR / cfg["file"]
            if not csv_path.exists():
                continue
            df = pd.read_csv(csv_path, index_col="date", parse_dates=True).sort_index()
            for col in df.columns:
                raw_path = RAW_DIR / f"{col}.csv"
                cached = self._load_raw(raw_path, col)
                live_s = df[col].dropna()
                if live_s.empty:
                    continue
                if cached is not None and not cached.empty:
                    # 合并 raw + live
                    merged = pd.concat([cached, live_s]).sort_index()
                    merged = merged[~merged.index.duplicated(keep="last")].dropna()
                    if len(merged) > len(cached):
                        self._save_raw(merged, raw_path)
                        logger.info(
                            f"  合并 live 到 raw: {col} ({len(cached)} → {len(merged)} 行)"
                        )
                        count += 1
                elif not raw_path.exists():
                    live_s.name = col
                    self._save_raw(live_s, raw_path)
                    logger.info(f"  从 {cfg['file']} 提取: {col} ({len(live_s)} 行)")
                    count += 1

        if count == 0:
            logger.info("  所有 raw 缓存已是最新，无需提取")
        else:
            logger.info(f"  ✓ 共提取/更新 {count} 个 raw 缓存")

    # ── 增量更新 ──────────────────────────────────────

    def update_ticker(
        self, ticker: str, start: str, end: str = None
    ) -> Optional[pd.Series]:
        """
        增量更新单个 ticker。

        策略：
        1. 检查本地 raw 缓存
        2. ≤ 2 天陈旧 → 跳过
        3. 有缓存 → 只拉增量部分
        4. 无缓存 → 全量下载
        5. 下载失败 → 返回本地缓存（如有），否则返回 None

        Returns:
            数据 Series，或 None（全部失败且无缓存）
        """
        end = end or datetime.now().strftime("%Y-%m-%d")
        raw_path = RAW_DIR / f"{ticker}.csv"
        cached = self._load_raw(raw_path, ticker)

        # 有缓存且足够新
        if cached is not None and not cached.empty:
            last_date = cached.index.max()
            days_stale = (pd.Timestamp(end) - last_date).days

            if days_stale <= 2:
                logger.info(f"  {ticker}: 本地已是最新 (截至 {last_date.date()})，跳过")
                return cached

        # 增量更新
        inc_start = (last_date - timedelta(days=5)).strftime("%Y-%m-%d")
        logger.info(f"  {ticker}: 增量更新 {inc_start} → {end} (本地 {len(cached)} 行)")
        # 添加网络重试机制
        max_retries = 3
        for retry in range(max_retries):
            try:
                new_data = self.source.fetch_ticker(ticker, inc_start, end)
                merged = pd.concat([cached, new_data]).sort_index()
                merged = merged[~merged.index.duplicated(keep="last")]
                merged = merged[merged.index >= pd.Timestamp(start)].dropna()
                self._save_raw(merged, raw_path)
                logger.info(f"  {ticker}: ✓ 更新完成 ({len(merged)} 行)")
                return merged
            except Exception as e:
                if retry == max_retries - 1:  # 最后一次重试也失败
                    logger.warning(f"  {ticker}: ✗ 增量更新失败 ({e})，使用本地缓存")
                    return cached[cached.index >= pd.Timestamp(start)]
                logger.warning(
                    f"  {ticker}: 网络错误，{retry + 1}/{max_retries} 次重试中: {e}"
                )
                time.sleep(10 * (retry + 1))  # 递增等待时间

        # 无缓存，全量下载
        logger.info(f"  {ticker}: 首次下载 {start} → {end}")
        # 添加网络重试机制
        max_retries = 3
        for retry in range(max_retries):
            try:
                data = self.source.fetch_ticker(ticker, start, end)
                self._save_raw(data, raw_path)
                logger.info(f"  {ticker}: ✓ 下载完成 ({len(data)} 行)")
                return data
            except Exception as e:
                if retry == max_retries - 1:  # 最后一次重试也失败
                    logger.error(f"  {ticker}: ✗ 下载失败: {e}")
                    return None
                logger.warning(
                    f"  {ticker}: 网络错误，{retry + 1}/{max_retries} 次重试中: {e}"
                )
                time.sleep(10 * (retry + 1))  # 递增等待时间

    def update_all(self, markets: list[str] = None):
        """
        更新所有（或指定的）市场数据。

        先收集所有唯一 ticker，逐个增量更新，然后组装各市场文件。
        单个 ticker 失败不会导致整个过程崩溃。
        """
        markets = markets or list(MARKET_CONFIGS.keys())

        # 先从已有 CSV 提取 raw 缓存（避免从零下载）
        logger.info("检查并提取已有数据到 raw 缓存...")
        self.bootstrap_raw_from_existing()

        # 收集所有唯一 ticker 及其最早起始日
        all_tickers: dict[str, str] = {}
        for m in markets:
            cfg = MARKET_CONFIGS[m]
            for t in cfg["tickers"]:
                if t not in all_tickers or cfg["start"] < all_tickers[t]:
                    all_tickers[t] = cfg["start"]

        total = len(all_tickers)
        logger.info(f"\n{'=' * 50}")
        logger.info(f"息壤数据管理器 - 增量更新")
        logger.info(f"市场: {', '.join(markets)}")
        logger.info(f"Tickers: {total} 个唯一标的")
        logger.info(f"{'=' * 50}")

        # 逐个更新（失败跳过）
        ticker_data: dict[str, pd.Series] = {}
        failed: list[str] = []

        for i, (ticker, start) in enumerate(all_tickers.items(), 1):
            logger.info(f"\n[{i}/{total}] {ticker}")
            result = self.update_ticker(ticker, start)
            if result is not None and not result.empty:
                ticker_data[ticker] = result
            else:
                failed.append(ticker)
                logger.warning(f"  {ticker}: 跳过（无数据）")

        # 组装各市场文件
        logger.info(f"\n组装市场数据文件...")
        for m in markets:
            cfg = MARKET_CONFIGS[m]
            missing = [t for t in cfg["tickers"] if t not in ticker_data]
            if missing:
                logger.warning(f"  {m}: 缺少 {missing}，跳过此市场")
                continue

            frames = {t: ticker_data[t] for t in cfg["tickers"]}
            prices = pd.DataFrame(frames).sort_index().ffill().dropna(how="any")
            prices = prices[prices.index >= pd.Timestamp(cfg["start"])]

            if cfg["money"]:
                daily_rate = (1.02 ** (1 / 252)) - 1
                money = pd.Series(index=prices.index, dtype=float)
                money.iloc[0] = 100.0
                for j in range(1, len(money)):
                    money.iloc[j] = money.iloc[j - 1] * (1 + daily_rate)
                prices["MONEY"] = money

            prices.index.name = "date"
            out_path = DATA_DIR / cfg["file"]
            prices.to_csv(out_path)
            logger.info(f"  ✓ {m}: {out_path.name} ({len(prices)} 行)")

        # 兼容旧回测
        self._generate_etf_daily(ticker_data)

        # manifest + 状态
        self._save_manifest(markets, ticker_data)
        self._save_status(markets, ticker_data)

        if failed:
            logger.warning(f"\n⚠ 以下 ticker 更新失败: {failed}")
            logger.warning(f"  可稍后重试: python -m data.data_manager --update")
        logger.info(f"\n✓ 完成")

    def update_live(self):
        """
        更新 live 数据（每日运行用）。

        策略：优先用 raw 缓存裁剪出 live 数据（零 API 调用），
        只有 raw 缓存太旧时才联网拉增量。
        周末/节假日缓存 ≤ 3 天陈旧视为正常（非交易日）。
        """
        logger.info(f"\n更新 Live 数据...")
        end = datetime.now()
        stale_threshold = 3  # 非交易日容忍天数

        for key, cfg in LIVE_CONFIGS.items():
            lookback = cfg["lookback_days"]
            start = (end - timedelta(days=lookback + 10)).strftime("%Y-%m-%d")
            end_str = end.strftime("%Y-%m-%d")

            frames = {}
            tickers_needing_api = []

            # 先检查 raw 缓存
            for ticker in cfg["tickers"]:
                raw_path = RAW_DIR / f"{ticker}.csv"
                cached = self._load_raw(raw_path, ticker)
                if cached is not None and not cached.empty:
                    last_date = cached.index.max()
                    days_stale = (pd.Timestamp(end_str) - last_date).days
                    if days_stale <= stale_threshold:
                        trimmed = cached[cached.index >= pd.Timestamp(start)]
                        if not trimmed.empty:
                            frames[ticker] = trimmed
                            logger.info(
                                f"  {ticker}: 从 raw 缓存裁剪 ({len(trimmed)} 行, "
                                f"截至 {last_date.date()})"
                            )
                            continue
                tickers_needing_api.append(ticker)

            # 需要联网的 ticker
            for ticker in tickers_needing_api:
                logger.info(f"  {ticker}: 需要联网更新...")
                try:
                    data = self.source.fetch_ticker(ticker, start, end_str)
                    frames[ticker] = data
                    # 同时更新 raw 缓存
                    raw_path = RAW_DIR / f"{ticker}.csv"
                    cached = self._load_raw(raw_path, ticker)
                    if cached is not None:
                        merged = pd.concat([cached, data]).sort_index()
                        merged = merged[~merged.index.duplicated(keep="last")].dropna()
                        self._save_raw(merged, raw_path)
                    else:
                        data_copy = data.copy()
                        data_copy.name = ticker
                        self._save_raw(data_copy, raw_path)
                except Exception as e:
                    # 用 raw 缓存兜底（即使稍旧）
                    raw_path = RAW_DIR / f"{ticker}.csv"
                    cached = self._load_raw(raw_path, ticker)
                    if cached is not None:
                        trimmed = cached[cached.index >= pd.Timestamp(start)]
                        if not trimmed.empty:
                            frames[ticker] = trimmed
                            logger.warning(
                                f"  {ticker}: API 失败，使用 raw 缓存兜底 ({e})"
                            )
                            continue
                    logger.error(f"  {ticker}: 无法获取数据且无本地缓存")

            if frames:
                prices = pd.DataFrame(frames).ffill().bfill().dropna()
                prices.index.name = "date"
                out_path = DATA_DIR / cfg["file"]
                prices.to_csv(out_path)
                logger.info(f"  ✓ {out_path.name} ({len(prices)} 行)")

        # 更新时间戳
        ts_path = DATA_DIR / "last_update.txt"
        ts_path.write_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # ── 数据状态 ──────────────────────────────────────

    def status(self) -> dict:
        """检查所有数据文件的状态。"""
        result = {"markets": {}, "raw_tickers": {}, "live": {}}

        for m, cfg in MARKET_CONFIGS.items():
            csv_path = DATA_DIR / cfg["file"]
            if csv_path.exists():
                df = pd.read_csv(csv_path, index_col="date", parse_dates=True)
                result["markets"][m] = {
                    "file": cfg["file"],
                    "rows": len(df),
                    "start": str(df.index.min().date()),
                    "end": str(df.index.max().date()),
                    "columns": list(df.columns),
                    "stale_days": (
                        datetime.now()
                        - df.index.max().to_pydatetime().replace(tzinfo=None)
                    ).days,
                }
            else:
                result["markets"][m] = {"file": cfg["file"], "exists": False}

        for f in sorted(RAW_DIR.glob("*.csv")):
            ticker = f.stem
            df = pd.read_csv(f, index_col="date", parse_dates=True)
            result["raw_tickers"][ticker] = {
                "rows": len(df),
                "start": str(df.index.min().date()),
                "end": str(df.index.max().date()),
            }

        for key, cfg in LIVE_CONFIGS.items():
            csv_path = DATA_DIR / cfg["file"]
            if csv_path.exists():
                df = pd.read_csv(csv_path, index_col="date", parse_dates=True)
                result["live"][key] = {
                    "rows": len(df),
                    "start": str(df.index.min().date()),
                    "end": str(df.index.max().date()),
                }

        return result

    # ── 内部方法 ──────────────────────────────────────

    def _load_raw(self, path: Path, ticker: str) -> Optional[pd.Series]:
        """加载 raw ticker CSV。"""
        if not path.exists():
            return None
        try:
            df = pd.read_csv(path, index_col="date", parse_dates=True).sort_index()
            col = "adj_close" if "adj_close" in df.columns else df.columns[0]
            s = df[col].copy()
            s.name = ticker
            return s
        except Exception as e:
            logger.warning(f"读取 {path} 失败: {e}")
            return None

    def _save_raw(self, series: pd.Series, path: Path):
        """保存 raw ticker 数据。"""
        df = series.to_frame("adj_close")
        df.index.name = "date"
        df.sort_index().to_csv(path)

    def _generate_etf_daily(self, ticker_data: dict[str, pd.Series]):
        """生成 etf_daily.csv（兼容旧版回测脚本）。"""
        us_tickers = ["SPY", "TLT", "GLD", "SHV"]
        if all(t in ticker_data for t in us_tickers):
            frames = {t: ticker_data[t] for t in us_tickers}
            etf = pd.DataFrame(frames).sort_index().ffill().bfill().dropna()
            etf.index.name = "date"
            out_path = DATA_DIR / "etf_daily.csv"
            etf.to_csv(out_path)
            logger.info(f"  ✓ etf_daily.csv ({len(etf)} 行) [兼容旧版]")

    def _save_status(self, markets: list, ticker_data: dict):
        """保存数据更新状态。"""
        status = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "markets_updated": markets,
            "tickers": {},
            "rate_limiter": self.rl.stats,
        }
        for t, s in ticker_data.items():
            raw_path = RAW_DIR / f"{t}.csv"
            status["tickers"][t] = {
                "rows": len(s),
                "start": str(s.index.min().date()),
                "end": str(s.index.max().date()),
                "sha256": self._sha256(raw_path) if raw_path.exists() else None,
            }
        with STATUS_FILE.open("w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _sha256(path: Path) -> str:
        d = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                d.update(chunk)
        return d.hexdigest()

    def _save_manifest(self, markets: list, ticker_data: dict):
        """生成 data_manifest.json（兼容 five_strategies_backtest 校验）。"""
        manifest_path = DATA_DIR / "data_manifest.json"
        payload = {
            "generated_at_utc": datetime.now().isoformat(),
            "source": "data_manager_v2",
            "policy": {"backtest_mode": "local_only"},
            "tickers": {},
            "markets": {},
        }
        for t, s in ticker_data.items():
            raw_path = RAW_DIR / f"{t}.csv"
            payload["tickers"][t] = {
                "rows": int(len(s)),
                "start": str(s.index.min().date()),
                "end": str(s.index.max().date()),
                "sha256": self._sha256(raw_path) if raw_path.exists() else None,
            }
        for m in markets:
            cfg = MARKET_CONFIGS[m]
            p = DATA_DIR / cfg["file"]
            if p.exists():
                df = pd.read_csv(p, index_col="date", parse_dates=True)
                payload["markets"][m] = {
                    "file": str(p).replace("\\", "/"),
                    "rows": int(len(df)),
                    "columns": list(df.columns),
                    "start": str(df.index.min().date()),
                    "end": str(df.index.max().date()),
                    "sha256": self._sha256(p),
                }
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info(f"  ✓ data_manifest.json 已更新")


# ── CLI 入口 ──────────────────────────────────────────


def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="息壤数据管理器")
    parser.add_argument(
        "--update",
        nargs="*",
        metavar="MARKET",
        help="增量更新市场数据（不指定市场则更新全部）",
    )
    parser.add_argument(
        "--update-live",
        action="store_true",
        help="更新 live 数据（每日运行用）",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="从已有 CSV 提取 raw 缓存（首次迁移用）",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="查看数据状态",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=3.0,
        help="API 请求最小间隔秒数（默认 3.0）",
    )
    parser.add_argument(
        "--max-hourly",
        type=int,
        default=60,
        help="每小时最大请求数（默认 60）",
    )
    args = parser.parse_args()

    dm = DataManager(min_interval=args.min_interval, max_hourly=args.max_hourly)

    if args.status:
        status = dm.status()
        print("\n息壤数据状态报告")
        print("=" * 50)
        for m, info in status["markets"].items():
            if info.get("exists") is False:
                print(f"  {m}: ✗ 未生成")
            else:
                stale = info.get("stale_days", "?")
                print(
                    f"  {m}: {info['rows']} 行 | "
                    f"{info['start']} → {info['end']} | 陈旧 {stale} 天"
                )
        print()
        for key, info in status["live"].items():
            print(f"  {key}: {info['rows']} 行 | {info['start']} → {info['end']}")
        print()
        for t, info in status["raw_tickers"].items():
            print(f"  raw/{t}: {info['rows']} 行 | {info['start']} → {info['end']}")
        return

    if args.bootstrap:
        dm.bootstrap_raw_from_existing()
        return

    if args.update_live:
        dm.update_live()
        return

    if args.update is not None:
        markets = args.update if args.update else None
        dm.update_all(markets=markets)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
