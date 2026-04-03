from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
MANIFEST_FILE = DATA_DIR / "data_manifest.json"


@dataclass
class MarketConfig:
    file: str
    tickers: list[str]
    start: str
    money: bool = False


MARKETS = {
    "us": MarketConfig("market_us.csv", ["SPY", "TLT", "GLD", "SHV"], "2005-01-01"),
    "cn": MarketConfig("market_cn.csv", ["510300.SS", "511010.SS", "518880.SS"], "2012-01-01", True),
    "chimerica": MarketConfig("market_chimerica.csv", ["513500.SS", "511010.SS", "518880.SS"], "2012-01-01", True),
    "eu": MarketConfig("market_eu.csv", ["EZU", "BWX", "GLD", "SHV"], "2005-01-01"),
    "india": MarketConfig("market_india.csv", ["EPI", "EMB", "GLD", "SHV"], "2007-01-01"),
    "global": MarketConfig("market_global.csv", ["VT", "BWX", "GLD", "SHV"], "2007-01-01"),
}


@contextmanager
def no_proxy():
    keys = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]
    backup = {k: os.environ.get(k) for k in keys}
    try:
        for k in keys:
            os.environ.pop(k, None)
        yield
    finally:
        for k, v in backup.items():
            if v is not None:
                os.environ[k] = v


class Fetcher:
    def __init__(self, min_interval_sec: float, max_retries: int):
        self.min_interval_sec = min_interval_sec
        self.max_retries = max_retries
        self.last_ts = 0.0

    def _rate_limit(self):
        dt = time.time() - self.last_ts
        if dt < self.min_interval_sec:
            time.sleep(self.min_interval_sec - dt)

    def _yf(self, ticker: str, start: str, end: str) -> pd.Series:
        with no_proxy():
            df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False, threads=False)
        if df.empty:
            raise RuntimeError("empty response")
        s = df["Adj Close"] if not isinstance(df.columns, pd.MultiIndex) else df["Adj Close"].iloc[:, 0]
        s = s.dropna()
        if s.empty:
            raise RuntimeError("adj close is empty")
        s.name = ticker
        return s

    def _ak_cn(self, ticker: str, start: str, end: str) -> pd.Series:
        import akshare as ak

        code = ticker.replace(".SS", "").replace(".SZ", "")
        s_ts, e_ts = pd.Timestamp(start), pd.Timestamp(end)

        prefix = "sh" if ticker.endswith(".SS") else "sz"
        try:
            with no_proxy():
                df = ak.fund_etf_hist_sina(symbol=f"{prefix}{code}")
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"])
                s = df.set_index("date").sort_index()["close"].astype(float)
                s = s[(s.index >= s_ts) & (s.index <= e_ts)].dropna()
                if not s.empty:
                    s.name = ticker
                    return s
        except Exception:
            pass

        with no_proxy():
            df = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=s_ts.strftime("%Y%m%d"),
                end_date=e_ts.strftime("%Y%m%d"),
                adjust="qfq",
            )
        if df.empty:
            raise RuntimeError("akshare empty response")
        df["日期"] = pd.to_datetime(df["日期"])
        s = df.set_index("日期").sort_index()["收盘"].astype(float).dropna()
        if s.empty:
            raise RuntimeError("akshare close empty")
        s.name = ticker
        return s

    def download(self, ticker: str, start: str, end: str) -> pd.Series:
        if ticker.endswith(".SS") or ticker.endswith(".SZ"):
            return self._ak_cn(ticker, start, end)

        last_err = None
        for i in range(1, self.max_retries + 1):
            self._rate_limit()
            self.last_ts = time.time()
            try:
                return self._yf(ticker, start, end)
            except Exception as e:  # noqa: BLE001
                last_err = e
                if i >= self.max_retries:
                    break
                msg = str(e).lower()
                backoff = min(900, 60 * (2 ** (i - 1))) if ("rate limit" in msg or "too many requests" in msg) else min(600, 30 * (2 ** (i - 1)))
                time.sleep(backoff * 1.1)
        raise RuntimeError(f"{ticker} download failed: {last_err}")


def sha256(path: Path) -> str:
    d = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            d.update(chunk)
    return d.hexdigest()


def save_series(series: pd.Series, path: Path):
    df = series.to_frame("adj_close")
    df.index.name = "date"
    df.sort_index().to_csv(path)


def load_series(path: Path, ticker: str) -> pd.Series:
    df = pd.read_csv(path, index_col="date", parse_dates=True).sort_index()
    s = df["adj_close"].copy()
    s.name = ticker
    return s


def update_ticker(ticker: str, market_start: str, fetcher: Fetcher, force_refresh: bool) -> pd.Series:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw = RAW_DIR / f"{ticker}.csv"
    cached = load_series(raw, ticker) if raw.exists() else None

    if force_refresh or cached is None or cached.empty:
        s = fetcher.download(ticker, market_start, "2025-12-31")
        save_series(s, raw)
        return s

    try:
        inc_start = (cached.index.max() - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
        latest = fetcher.download(ticker, inc_start, "2025-12-31")
        merged = pd.concat([cached, latest]).sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
        merged = merged[merged.index >= pd.Timestamp(market_start)].dropna()
        save_series(merged, raw)
        return merged
    except Exception:
        fallback = cached[cached.index >= pd.Timestamp(market_start)]
        if fallback.empty:
            raise
        return fallback


def build_market(name: str, cfg: MarketConfig, data: dict[str, pd.Series]) -> Path:
    frames = {t: data[t] for t in cfg.tickers}
    prices = pd.DataFrame(frames).sort_index().ffill().dropna(how="any")
    prices = prices[prices.index >= pd.Timestamp(cfg.start)]

    if cfg.money:
        r = (1.02 ** (1 / 252)) - 1
        money = pd.Series(index=prices.index, dtype=float)
        money.iloc[0] = 100.0
        for i in range(1, len(money)):
            money.iloc[i] = money.iloc[i - 1] * (1 + r)
        prices["MONEY"] = money

    prices.index.name = "date"
    out = DATA_DIR / cfg.file
    prices.to_csv(out)
    print(f"  ✓ {name}: {out} | {len(prices)} rows")
    return out


def write_manifest(market_files: dict[str, Path], ticker_data: dict[str, pd.Series], args):
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "yfinance+akshare",
        "policy": {
            "backtest_mode": "local_only",
            "force_refresh": args.force_refresh,
            "min_interval_sec": args.min_interval_sec,
            "max_retries": args.max_retries,
            "ticker_wait_sec": args.ticker_wait_sec,
            "market_wait_sec": args.market_wait_sec,
        },
        "tickers": {},
        "markets": {},
    }
    for t, s in ticker_data.items():
        p = RAW_DIR / f"{t}.csv"
        payload["tickers"][t] = {"rows": int(len(s)), "start": str(s.index[0].date()), "end": str(s.index[-1].date()), "raw_file": str(p).replace("\\", "/"), "sha256": sha256(p)}
    for m, p in market_files.items():
        df = pd.read_csv(p, index_col="date", parse_dates=True)
        payload["markets"][m] = {"file": str(p).replace("\\", "/"), "rows": int(len(df)), "columns": list(df.columns), "start": str(df.index.min().date()), "end": str(df.index.max().date()), "sha256": sha256(p)}
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def parse_args():
    p = argparse.ArgumentParser(description="Prepare six-market local datasets")
    p.add_argument("--force-refresh", action="store_true")
    p.add_argument("--min-interval-sec", type=float, default=6.0)
    p.add_argument("--max-retries", type=int, default=6)
    p.add_argument("--ticker-wait-sec", type=float, default=12.0)
    p.add_argument("--market-wait-sec", type=float, default=30.0)
    return p.parse_args()


def main():
    args = parse_args()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"policy: min_interval={args.min_interval_sec}s, retries={args.max_retries}, ticker_wait={args.ticker_wait_sec}s, market_wait={args.market_wait_sec}s")

    fetcher = Fetcher(args.min_interval_sec, args.max_retries)
    ticker_data: dict[str, pd.Series] = {}
    market_files: dict[str, Path] = {}

    for i, (mname, cfg) in enumerate(MARKETS.items(), start=1):
        print(f"\n[{i}/{len(MARKETS)}] market={mname}")
        for j, ticker in enumerate(cfg.tickers, start=1):
            if ticker in ticker_data:
                print(f"  - ({j}/{len(cfg.tickers)}) {ticker}: reused")
                continue
            print(f"  - ({j}/{len(cfg.tickers)}) {ticker}: updating")
            ticker_data[ticker] = update_ticker(ticker, cfg.start, fetcher, args.force_refresh)
            if args.ticker_wait_sec > 0:
                time.sleep(args.ticker_wait_sec)

        market_files[mname] = build_market(mname, cfg, ticker_data)
        if args.market_wait_sec > 0 and i < len(MARKETS):
            time.sleep(args.market_wait_sec)

    write_manifest(market_files, ticker_data, args)
    print(f"\n✓ manifest: {MANIFEST_FILE}")


if __name__ == "__main__":
    main()
