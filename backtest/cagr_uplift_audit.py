"""
CAGR uplift bypass audit.

This module keeps the audited 90/10 topology, MA150 macro filter, acute
shifter, recovery rules, and satellite size unchanged. Experiments are isolated
to bypass sleeves:

- cash-proxy replacement for the defensive core's SHV price series
- satellite-level trend/CTA replacement inside the fixed 10% satellite

Short-history cash proxies are compared only on their own overlapping windows.
Do not read a SGOV/USFR result as a 2005 full-cycle conclusion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd

from backtest.fixed_policy_audit import load_prices
from backtest.portfolio_aggregation_audit import (
    INITIAL_CAPITAL,
    SleeveMetrics,
    run_static_beta_sleeve,
)
from backtest.real_world_friction_audit import (
    PRODUCTION_BETA_SCENARIO,
    PRODUCTION_BETA_WEIGHTS,
    PRODUCTION_SLEEVE_WEIGHTS,
    RealWorldFrictionScenario,
    apply_real_world_friction,
    production_candidate_nav,
)
from backtest.return_attribution import (
    AttributionAudit,
    run_return_attribution,
    run_return_attribution_from_prices,
)
from engine.config import ASSETS, FEE_RATE
from engine.portfolio_aggregator import AggregatedPortfolio, aggregate_sleeves, calculate_cagr, calculate_mdd


DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
CASH_PROXY_SNAPSHOT_DIR = DATA_DIR / "audit_snapshots" / "2026-06-23-cash-proxy"
TREND_SNAPSHOT_DIR = DATA_DIR / "audit_snapshots" / "2026-06-23-trend-satellite"
FRICTION_MATRIX_DIR = DATA_DIR / "audit_snapshots" / "2026-06-23-friction-matrix"
FRICTION_MATRIX_FILE = FRICTION_MATRIX_DIR / "annual_friction_matrix.json"
DEFAULT_START = "2005-01-03"
DEFAULT_END = "2026-04-30"
CASH_PROXY_TICKERS = ["SHV", "BIL", "SGOV", "USFR", "TFLO"]
BASELINE_SATELLITE_WEIGHTS = {"QQQ": 0.40, "SPY": 0.30, "GLD": 0.30}
DBMF_SATELLITE_WEIGHTS = {"QQQ": 0.40, "GLD": 0.30, "DBMF": 0.30}
MOMENTUM_LOOKBACK_DAYS = 252
BASE_REAL_WORLD_SCENARIO = RealWorldFrictionScenario(
    name="unlevered_base_case",
    dividend_withholding_drag_bps=55,
    tax_drag_bps=35,
    broker_cash_financing_drag_bps=10,
    operational_failure_drag_bps=20,
    tail_failure_shock_bps=50,
)
HARSH_REAL_WORLD_SCENARIO = RealWorldFrictionScenario(
    name="unlevered_harsh_case",
    dividend_withholding_drag_bps=75,
    tax_drag_bps=75,
    broker_cash_financing_drag_bps=25,
    operational_failure_drag_bps=50,
    tail_failure_shock_bps=150,
)


@dataclass(frozen=True)
class CashProxyDefensiveRun:
    ticker: str
    attribution: AttributionAudit
    source: str


@dataclass(frozen=True)
class CashProxyAuditRow:
    ticker: str
    source: str
    start: str
    end: str
    years: float
    research_cagr: float
    research_mdd: float
    real_cagr: float
    real_mdd: float
    harsh_cagr: float
    harsh_mdd: float
    baseline_real_cagr: float
    baseline_real_mdd: float
    real_cagr_delta: float
    real_mdd_delta: float
    final_nav_delta: float
    defensive_cagr: float
    defensive_mdd: float
    cash_pnl_delta: float
    macro_net_pnl_delta: float
    avg_cash_weight: float
    high_cash_day_pct: float
    max_daily_weight_shock: float
    pass_mdd_guardrail: bool


@dataclass(frozen=True)
class TrendSatelliteAuditRow:
    scenario: str
    source: str
    confidence: str
    start: str
    end: str
    years: float
    research_cagr: float
    research_mdd: float
    real_cagr: float
    real_mdd: float
    harsh_cagr: float
    harsh_mdd: float
    baseline_real_cagr: float
    baseline_real_mdd: float
    real_cagr_delta: float
    real_mdd_delta: float
    final_nav_delta: float
    satellite_cagr: float
    satellite_mdd: float
    satellite_cost: float
    satellite_rebalances: int
    crisis_2008_return: float | None
    crisis_2008_mdd: float | None
    crisis_2022_return: float | None
    crisis_2022_mdd: float | None
    pass_mdd_guardrail: bool


@dataclass(frozen=True)
class AnnualFrictionAssumption:
    year: int
    fed_funds_rate: float
    spy_dividend_yield: float
    qqq_dividend_yield: float
    withholding_tax_rate: float
    broker_spread_bps: float
    operational_failure_bps: float
    rebalance_event_bps: float
    macro_event_bps: float
    acute_event_bps: float


@dataclass(frozen=True)
class CalibratedFrictionResult:
    name: str
    nav: pd.Series
    ledger: pd.DataFrame
    cagr: float
    mdd: float
    final: float
    research_cagr: float
    research_mdd: float
    cagr_delta: float
    mdd_delta: float
    tax_cost: float
    broker_cost: float
    operational_cost: float
    event_cost: float


def _validate_positive(series: pd.Series, ticker: str, source: Path | str) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce").dropna().sort_index()
    if clean.empty:
        raise ValueError(f"empty price series for {ticker}: {source}")
    bad = clean[clean <= 0]
    if not bad.empty:
        samples = ", ".join(
            f"{idx.date()}={value:.4f}" for idx, value in bad.head(5).items()
        )
        raise ValueError(f"non-positive prices for {ticker} in {source}: {samples}")
    clean.name = ticker
    clean.index.name = "date"
    return clean


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_cached_cash_proxy(
    ticker: str,
    snapshot_dir: Path = CASH_PROXY_SNAPSHOT_DIR,
) -> tuple[pd.Series, str]:
    raw_path = RAW_DIR / f"{ticker}.csv"
    if raw_path.exists():
        df = pd.read_csv(raw_path, index_col="date", parse_dates=True).sort_index()
        column = "adj_close" if "adj_close" in df.columns else df.columns[0]
        return _validate_positive(df[column], ticker, raw_path), str(raw_path)

    snapshot_path = snapshot_dir / f"{ticker}.csv"
    if snapshot_path.exists():
        df = pd.read_csv(snapshot_path, index_col="date", parse_dates=True).sort_index()
        column = "adj_close" if "adj_close" in df.columns else df.columns[0]
        return _validate_positive(df[column], ticker, snapshot_path), str(snapshot_path)

    raise FileNotFoundError(f"missing cash proxy data for {ticker}")


def _parse_yahoo_chart_payload(ticker: str, payload: dict) -> pd.Series:
    chart = payload.get("chart") if isinstance(payload, dict) else None
    result = chart.get("result") if isinstance(chart, dict) else None
    if not result:
        error = chart.get("error") if isinstance(chart, dict) else None
        raise RuntimeError(f"Yahoo chart returned no result for {ticker}: {error}")
    item = result[0]
    timestamps = item.get("timestamp") or []
    adjclose = (item.get("indicators") or {}).get("adjclose") or []
    if not timestamps or not adjclose:
        raise RuntimeError(f"Yahoo chart returned no adjusted close for {ticker}")
    values = adjclose[0].get("adjclose") if isinstance(adjclose[0], dict) else None
    if not values:
        raise RuntimeError(f"Yahoo chart adjusted close is empty for {ticker}")
    index = pd.to_datetime(timestamps, unit="s").normalize()
    series = pd.Series(values, index=index, name=ticker)
    return _validate_positive(series, ticker, f"yahoo_chart:{ticker}")


def download_cash_proxy_yahoo_chart(
    ticker: str,
    start: str = DEFAULT_START,
    end: str = DEFAULT_END,
) -> pd.Series:
    period1 = int(pd.Timestamp(start).timestamp())
    period2 = int((pd.Timestamp(end) + pd.Timedelta(days=1)).timestamp())
    query = urlencode(
        {
            "period1": period1,
            "period2": period2,
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }
    )
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?{query}"
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return _parse_yahoo_chart_payload(ticker, payload)


def download_cash_proxy(ticker: str, start: str = DEFAULT_START, end: str = DEFAULT_END) -> pd.Series:
    try:
        return download_cash_proxy_yahoo_chart(ticker, start=start, end=end)
    except Exception:
        pass

    import yfinance as yf

    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False, threads=False)
    if df.empty:
        raise RuntimeError(f"cash proxy download returned empty data for {ticker}")
    if isinstance(df.columns, pd.MultiIndex):
        series = df["Adj Close"]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
    else:
        series = df["Adj Close"]
    return _validate_positive(series, ticker, f"yfinance:{ticker}")


def save_cash_proxy_snapshot(
    series: pd.Series,
    snapshot_dir: Path = CASH_PROXY_SNAPSHOT_DIR,
) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    path = snapshot_dir / f"{series.name}.csv"
    frame = series.to_frame("adj_close")
    frame.index.name = "date"
    frame.to_csv(path)
    write_cash_proxy_manifest(snapshot_dir)
    return path


def save_price_snapshot(
    series: pd.Series,
    snapshot_dir: Path,
    purpose: str,
) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    path = snapshot_dir / f"{series.name}.csv"
    frame = series.to_frame("adj_close")
    frame.index.name = "date"
    frame.to_csv(path)
    write_price_snapshot_manifest(snapshot_dir, purpose=purpose)
    return path


def write_price_snapshot_manifest(snapshot_dir: Path, purpose: str) -> Path:
    tickers: dict[str, dict] = {}
    for path in sorted(snapshot_dir.glob("*.csv")):
        df = pd.read_csv(path, index_col="date", parse_dates=True).sort_index()
        column = "adj_close" if "adj_close" in df.columns else df.columns[0]
        series = _validate_positive(df[column], path.stem, path)
        tickers[path.stem] = {
            "rows": int(len(series)),
            "start": str(series.index.min().date()),
            "end": str(series.index.max().date()),
            "sha256": _sha256(path),
        }
    payload = {
        "snapshot": snapshot_dir.name,
        "source": "Yahoo Adj Close",
        "created_at": date.today().isoformat(),
        "policy": {
            "purpose": purpose,
            "routine_refresh_allowed": False,
            "production_candidate_change": False,
        },
        "tickers": tickers,
    }
    manifest_path = snapshot_dir / "manifest.json"
    manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest_path


def write_cash_proxy_manifest(snapshot_dir: Path = CASH_PROXY_SNAPSHOT_DIR) -> Path:
    tickers: dict[str, dict] = {}
    for path in sorted(snapshot_dir.glob("*.csv")):
        df = pd.read_csv(path, index_col="date", parse_dates=True).sort_index()
        column = "adj_close" if "adj_close" in df.columns else df.columns[0]
        series = _validate_positive(df[column], path.stem, path)
        tickers[path.stem] = {
            "rows": int(len(series)),
            "start": str(series.index.min().date()),
            "end": str(series.index.max().date()),
            "sha256": _sha256(path),
        }
    payload = {
        "snapshot": snapshot_dir.name,
        "source": "Yahoo Adj Close",
        "created_at": date.today().isoformat(),
        "policy": {
            "purpose": "cash proxy CAGR uplift bypass audit input",
            "routine_refresh_allowed": False,
            "production_candidate_change": False,
        },
        "tickers": tickers,
    }
    manifest_path = snapshot_dir / "manifest.json"
    manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest_path


def load_or_fetch_cash_proxy(
    ticker: str,
    allow_download: bool = True,
    snapshot_dir: Path = CASH_PROXY_SNAPSHOT_DIR,
) -> tuple[pd.Series, str]:
    if ticker == "SHV":
        prices = load_prices()
        return _validate_positive(prices["SHV"], "SHV", "data/etf_daily.csv"), "data/etf_daily.csv"
    try:
        return load_cached_cash_proxy(ticker, snapshot_dir=snapshot_dir)
    except FileNotFoundError:
        if not allow_download:
            raise
    series = download_cash_proxy(ticker)
    path = save_cash_proxy_snapshot(series, snapshot_dir=snapshot_dir)
    return series, str(path)


def load_or_fetch_trend_asset(
    ticker: str,
    allow_download: bool = True,
    snapshot_dir: Path = TREND_SNAPSHOT_DIR,
) -> tuple[pd.Series, str]:
    try:
        series, source = load_cached_cash_proxy(ticker, snapshot_dir=snapshot_dir)
        return series, source
    except FileNotFoundError:
        if not allow_download:
            raise
    series = download_cash_proxy(ticker)
    path = save_price_snapshot(
        series,
        snapshot_dir,
        purpose="trend satellite CAGR uplift bypass audit input",
    )
    return series, str(path)


def load_satellite_series(
    ticker: str,
    allow_download: bool = True,
    trend_snapshot_dir: Path = TREND_SNAPSHOT_DIR,
) -> tuple[pd.Series, str]:
    if ticker in {"SPY", "TLT", "GLD", "SHV"}:
        prices = load_prices()
        return _validate_positive(prices[ticker], ticker, "data/etf_daily.csv"), "data/etf_daily.csv"
    if ticker in {"QQQ", "VTI", "SMH"}:
        series, source = load_cached_cash_proxy(
            ticker,
            snapshot_dir=DATA_DIR / "audit_snapshots" / "2026-06-23-yahoo-adj-close",
        )
        return series, source
    return load_or_fetch_trend_asset(
        ticker,
        allow_download=allow_download,
        snapshot_dir=trend_snapshot_dir,
    )


def build_cash_proxy_prices(cash_proxy: pd.Series) -> pd.DataFrame:
    prices = load_prices()[ASSETS].sort_index()
    proxy = _validate_positive(cash_proxy, cash_proxy.name or "cash_proxy", "cash_proxy")
    frame = prices.drop(columns=["SHV"]).join(proxy.rename("SHV"), how="inner")
    frame = frame[ASSETS].sort_index().ffill().dropna(how="any")
    frame = frame.loc[:DEFAULT_END]
    if frame.empty:
        raise ValueError(f"cash proxy {proxy.name} has no overlap with production assets")
    return frame


def _sleeve_from_attribution(name: str, audit: AttributionAudit) -> SleeveMetrics:
    nav = audit.history["nav_end"].rename(name)
    metrics = audit.metrics
    return SleeveMetrics(
        name=name,
        nav=nav,
        cagr=float(metrics.cagr),
        mdd=float(metrics.mdd),
        final=float(metrics.final),
        total_cost=float(metrics.total_cost),
        rebalances=int(metrics.rebalances),
    )


def run_defensive_cash_proxy(
    ticker: str,
    allow_download: bool = True,
    snapshot_dir: Path = CASH_PROXY_SNAPSHOT_DIR,
) -> CashProxyDefensiveRun:
    if ticker == "SHV":
        return CashProxyDefensiveRun(
            ticker=ticker,
            attribution=run_return_attribution(),
            source="data/etf_daily.csv",
        )
    series, source = load_or_fetch_cash_proxy(
        ticker,
        allow_download=allow_download,
        snapshot_dir=snapshot_dir,
    )
    prices = build_cash_proxy_prices(series)
    return CashProxyDefensiveRun(
        ticker=ticker,
        attribution=run_return_attribution_from_prices(prices),
        source=source,
    )


def build_research_nav_from_defensive(audit: AttributionAudit, name: str) -> AggregatedPortfolio:
    defensive = _sleeve_from_attribution(f"defensive_{name}", audit)
    beta = run_static_beta_sleeve(PRODUCTION_BETA_SCENARIO, PRODUCTION_BETA_WEIGHTS)
    return aggregate_sleeves(
        {"defensive": defensive.nav, "beta": beta.nav},
        PRODUCTION_SLEEVE_WEIGHTS,
        initial_capital=INITIAL_CAPITAL,
    )


def _slice_nav(nav: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    sliced = nav.sort_index().loc[start:end].dropna()
    if sliced.empty:
        raise ValueError(f"NAV has no overlap for {start.date()}..{end.date()}")
    return sliced


def _years(index: pd.Index) -> float:
    return max((index[-1] - index[0]).days / 365.25, 1 / 365.25)


def _macro_net_pnl(audit: AttributionAudit, start: pd.Timestamp, end: pd.Timestamp) -> float:
    history = audit.history.loc[start:end]
    group = history[history["return_regime"] == "MACRO_DEFENSE"]
    if group.empty:
        return 0.0
    return float(group["asset_pnl_total"].sum() - group["cost"].sum())


def _cash_pnl(audit: AttributionAudit, start: pd.Timestamp, end: pd.Timestamp) -> float:
    return float(audit.history.loc[start:end, "SHV_pnl"].sum())


def _avg_cash_weight(audit: AttributionAudit, start: pd.Timestamp, end: pd.Timestamp) -> float:
    return float(audit.history.loc[start:end, "SHV_start_weight"].mean())


def _high_cash_day_pct(audit: AttributionAudit, start: pd.Timestamp, end: pd.Timestamp) -> float:
    history = audit.history.loc[start:end]
    if history.empty:
        return 0.0
    return float((history["SHV_end_weight"] >= 0.50).mean())


def _max_daily_weight_shock(audit: AttributionAudit, start: pd.Timestamp, end: pd.Timestamp) -> float:
    cols = [f"{asset}_end_weight" for asset in ASSETS]
    weights = audit.history.loc[start:end, cols].dropna()
    if len(weights) <= 1:
        return 0.0
    return float(weights.diff().abs().sum(axis=1).max() / 2.0)


def run_baseline_defensive_for_window(start: pd.Timestamp, end: pd.Timestamp) -> AttributionAudit:
    """Replay SHV baseline from the same start date and initial capital as a proxy."""
    prices = load_prices()[ASSETS].sort_index().loc[start:end]
    if prices.empty:
        raise ValueError(f"baseline prices have no overlap for {start.date()}..{end.date()}")
    return run_return_attribution_from_prices(prices)


def build_satellite_prices(
    tickers: list[str],
    allow_download: bool = True,
    trend_snapshot_dir: Path = TREND_SNAPSHOT_DIR,
) -> tuple[pd.DataFrame, dict[str, str]]:
    frames: dict[str, pd.Series] = {}
    sources: dict[str, str] = {}
    for ticker in tickers:
        series, source = load_satellite_series(
            ticker,
            allow_download=allow_download,
            trend_snapshot_dir=trend_snapshot_dir,
        )
        frames[ticker] = series
        sources[ticker] = source
    prices = pd.DataFrame(frames).sort_index().ffill().dropna(how="any")
    prices = prices.loc[:DEFAULT_END, tickers]
    if prices.empty:
        raise ValueError(f"satellite price table has no overlap for {tickers}")
    return prices, sources


def run_static_satellite_sleeve_from_prices(
    name: str,
    prices: pd.DataFrame,
    weights: dict[str, float],
    initial_capital: float = INITIAL_CAPITAL,
    fee_rate: float = FEE_RATE,
) -> SleeveMetrics:
    if abs(sum(weights.values()) - 1.0) > 1e-9:
        raise ValueError(f"weights must sum to 1.0: {weights}")
    tickers = list(weights)
    prices = prices[tickers].sort_index().dropna(how="any")
    if prices.empty:
        raise ValueError(f"empty satellite price table for {name}")
    returns = prices.pct_change().fillna(0.0)
    target = np.array([weights[ticker] for ticker in tickers], dtype=float)
    positions = target * initial_capital
    nav_history: list[float] = []
    total_cost = 0.0
    rebalances = 0

    for i, current_date in enumerate(prices.index):
        positions = positions * (1.0 + returns.iloc[i].values)
        nav = float(positions.sum())
        is_year_end = (
            i < len(prices.index) - 1
            and prices.index[i].year != prices.index[i + 1].year
        )
        if is_year_end:
            current_weights = positions / nav if nav > 0 else np.zeros(len(tickers))
            turnover = float(np.sum(np.abs(current_weights - target)) / 2.0)
            cost = nav * turnover * fee_rate
            nav = max(nav - cost, 0.0)
            positions = target * nav
            total_cost += cost
            rebalances += 1
        nav_history.append(nav)

    nav = pd.Series(nav_history, index=prices.index, name=name)
    return SleeveMetrics(
        name=name,
        nav=nav,
        cagr=calculate_cagr(nav),
        mdd=calculate_mdd(nav),
        final=float(nav.iloc[-1]),
        total_cost=float(total_cost),
        rebalances=rebalances,
    )


def run_static_satellite_sleeve(
    name: str,
    weights: dict[str, float],
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    allow_download: bool = True,
    trend_snapshot_dir: Path = TREND_SNAPSHOT_DIR,
) -> tuple[SleeveMetrics, dict[str, str]]:
    prices, sources = build_satellite_prices(
        list(weights),
        allow_download=allow_download,
        trend_snapshot_dir=trend_snapshot_dir,
    )
    if start is not None or end is not None:
        prices = prices.loc[start:end]
    return run_static_satellite_sleeve_from_prices(name, prices, weights), sources


def momentum_target_asset(prices: pd.DataFrame, current_date: pd.Timestamp) -> str:
    qqq = prices["QQQ"]
    loc = qqq.index.get_loc(current_date)
    if not isinstance(loc, int) or loc < MOMENTUM_LOOKBACK_DAYS:
        return "SHV"
    lookback_price = float(qqq.iloc[loc - MOMENTUM_LOOKBACK_DAYS])
    if lookback_price <= 0:
        return "SHV"
    return "QQQ" if float(qqq.iloc[loc] / lookback_price - 1.0) > 0 else "SHV"


def run_qqq_cash_momentum_sleeve_from_prices(
    prices: pd.DataFrame,
    name: str = "gld_qqq_cash_12m_momentum",
    initial_capital: float = INITIAL_CAPITAL,
    fee_rate: float = FEE_RATE,
) -> SleeveMetrics:
    tickers = ["GLD", "QQQ", "SHV"]
    prices = prices[tickers].sort_index().dropna(how="any")
    if prices.empty:
        raise ValueError("empty momentum price table")
    returns = prices.pct_change().fillna(0.0)

    def target_for(day: pd.Timestamp) -> np.ndarray:
        chosen = momentum_target_asset(prices, day)
        weights = {"GLD": 0.50, "QQQ": 0.0, "SHV": 0.0}
        weights[chosen] = 0.50
        return np.array([weights[ticker] for ticker in tickers], dtype=float)

    positions = target_for(prices.index[0]) * initial_capital
    nav_history: list[float] = []
    total_cost = 0.0
    rebalances = 0

    for i, current_date in enumerate(prices.index):
        positions = positions * (1.0 + returns.iloc[i].values)
        nav = float(positions.sum())
        is_quarter_end = (
            i < len(prices.index) - 1
            and prices.index[i].quarter != prices.index[i + 1].quarter
        )
        if is_quarter_end:
            target = target_for(current_date)
            current_weights = positions / nav if nav > 0 else np.zeros(len(tickers))
            turnover = float(np.sum(np.abs(current_weights - target)) / 2.0)
            cost = nav * turnover * fee_rate
            nav = max(nav - cost, 0.0)
            positions = target * nav
            total_cost += cost
            rebalances += 1
        nav_history.append(nav)

    nav = pd.Series(nav_history, index=prices.index, name=name)
    return SleeveMetrics(
        name=name,
        nav=nav,
        cagr=calculate_cagr(nav),
        mdd=calculate_mdd(nav),
        final=float(nav.iloc[-1]),
        total_cost=float(total_cost),
        rebalances=rebalances,
    )


def run_qqq_cash_momentum_sleeve(
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    allow_download: bool = True,
    trend_snapshot_dir: Path = TREND_SNAPSHOT_DIR,
) -> tuple[SleeveMetrics, dict[str, str]]:
    prices, sources = build_satellite_prices(
        ["GLD", "QQQ", "SHV"],
        allow_download=allow_download,
        trend_snapshot_dir=trend_snapshot_dir,
    )
    if start is not None or end is not None:
        prices = prices.loc[start:end]
    return run_qqq_cash_momentum_sleeve_from_prices(prices), sources


def aggregate_from_window_defensive_and_satellite(
    satellite: SleeveMetrics,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> AggregatedPortfolio:
    defensive = _sleeve_from_attribution(
        "window_defensive",
        run_baseline_defensive_for_window(start, end),
    )
    return aggregate_sleeves(
        {"defensive": defensive.nav, "beta": satellite.nav},
        PRODUCTION_SLEEVE_WEIGHTS,
        initial_capital=INITIAL_CAPITAL,
    )


def _period_return_and_mdd(nav: pd.Series, start: str, end: str) -> tuple[float, float] | None:
    sliced = nav.loc[start:end].dropna()
    if len(sliced) < 2:
        return None
    period_return = float(sliced.iloc[-1] / sliced.iloc[0] - 1.0)
    period_mdd = calculate_mdd(sliced)
    return period_return, period_mdd


def evaluate_trend_satellite(
    scenario: str,
    satellite: SleeveMetrics,
    baseline_satellite: SleeveMetrics,
    sources: dict[str, str],
    confidence: str,
) -> TrendSatelliteAuditRow:
    start = max(satellite.nav.index.min(), baseline_satellite.nav.index.min())
    end = min(satellite.nav.index.max(), baseline_satellite.nav.index.max())
    candidate_research = aggregate_from_window_defensive_and_satellite(satellite, start, end)
    baseline_research = aggregate_from_window_defensive_and_satellite(baseline_satellite, start, end)
    research_slice = _slice_nav(candidate_research.nav, start, end)
    baseline_research_slice = _slice_nav(baseline_research.nav, start, end)
    real_nav = apply_real_world_friction(research_slice, BASE_REAL_WORLD_SCENARIO)
    harsh_nav = apply_real_world_friction(research_slice, HARSH_REAL_WORLD_SCENARIO)
    baseline_real_nav = apply_real_world_friction(
        baseline_research_slice,
        BASE_REAL_WORLD_SCENARIO,
    )
    real_cagr = calculate_cagr(real_nav)
    real_mdd = calculate_mdd(real_nav)
    baseline_real_cagr = calculate_cagr(baseline_real_nav)
    baseline_real_mdd = calculate_mdd(baseline_real_nav)
    harsh_mdd = calculate_mdd(harsh_nav)
    crisis_2008 = _period_return_and_mdd(real_nav, "2008-01-01", "2008-12-31")
    crisis_2022 = _period_return_and_mdd(real_nav, "2022-01-01", "2022-12-31")
    source_note = "; ".join(f"{ticker}:{source}" for ticker, source in sorted(sources.items()))

    return TrendSatelliteAuditRow(
        scenario=scenario,
        source=source_note,
        confidence=confidence,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        years=_years(research_slice.index),
        research_cagr=calculate_cagr(research_slice),
        research_mdd=calculate_mdd(research_slice),
        real_cagr=real_cagr,
        real_mdd=real_mdd,
        harsh_cagr=calculate_cagr(harsh_nav),
        harsh_mdd=harsh_mdd,
        baseline_real_cagr=baseline_real_cagr,
        baseline_real_mdd=baseline_real_mdd,
        real_cagr_delta=real_cagr - baseline_real_cagr,
        real_mdd_delta=real_mdd - baseline_real_mdd,
        final_nav_delta=float(real_nav.iloc[-1] - baseline_real_nav.iloc[-1]),
        satellite_cagr=satellite.cagr,
        satellite_mdd=satellite.mdd,
        satellite_cost=satellite.total_cost,
        satellite_rebalances=satellite.rebalances,
        crisis_2008_return=crisis_2008[0] if crisis_2008 is not None else None,
        crisis_2008_mdd=crisis_2008[1] if crisis_2008 is not None else None,
        crisis_2022_return=crisis_2022[0] if crisis_2022 is not None else None,
        crisis_2022_mdd=crisis_2022[1] if crisis_2022 is not None else None,
        pass_mdd_guardrail=(real_mdd >= baseline_real_mdd and harsh_mdd >= -0.17),
    )


def load_annual_friction_assumptions(
    path: Path = FRICTION_MATRIX_FILE,
) -> dict[int, AnnualFrictionAssumption]:
    if not path.exists():
        raise FileNotFoundError(
            f"missing annual friction matrix: {path}. "
            "Restore data/audit_snapshots/2026-06-23-friction-matrix."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("annual_assumptions", [])
    assumptions: dict[int, AnnualFrictionAssumption] = {}
    for row in rows:
        item = AnnualFrictionAssumption(
            year=int(row["year"]),
            fed_funds_rate=float(row["fed_funds_rate"]),
            spy_dividend_yield=float(row["spy_dividend_yield"]),
            qqq_dividend_yield=float(row["qqq_dividend_yield"]),
            withholding_tax_rate=float(row["withholding_tax_rate"]),
            broker_spread_bps=float(row["broker_spread_bps"]),
            operational_failure_bps=float(row["operational_failure_bps"]),
            rebalance_event_bps=float(row["rebalance_event_bps"]),
            macro_event_bps=float(row["macro_event_bps"]),
            acute_event_bps=float(row["acute_event_bps"]),
        )
        assumptions[item.year] = item
    if not assumptions:
        raise ValueError(f"annual friction matrix has no assumptions: {path}")
    return assumptions


def build_production_exposure_frame(defensive_audit: AttributionAudit | None = None) -> pd.DataFrame:
    """Approximate daily production-candidate exposures without changing the 90/10 topology."""
    audit = defensive_audit or run_return_attribution()
    history = audit.history
    exposures = pd.DataFrame(index=history.index)
    exposures["SPY"] = 0.90 * history["SPY_start_weight"] + 0.10 * PRODUCTION_BETA_WEIGHTS["SPY"]
    exposures["TLT"] = 0.90 * history["TLT_start_weight"]
    exposures["GLD"] = 0.90 * history["GLD_start_weight"] + 0.10 * PRODUCTION_BETA_WEIGHTS["GLD"]
    exposures["SHV"] = 0.90 * history["SHV_start_weight"]
    exposures["QQQ"] = 0.10 * PRODUCTION_BETA_WEIGHTS["QQQ"]
    exposures["action"] = history["action"].fillna("")
    defensive_turnover = pd.Series(0.0, index=history.index)
    for asset in ASSETS:
        defensive_turnover += (
            history[f"{asset}_end_weight"] - history[f"{asset}_start_weight"]
        ).abs()
    exposures["event_turnover"] = 0.90 * defensive_turnover / 2.0
    return exposures


def classify_execution_event(action: str) -> str:
    if not action:
        return "none"
    if "进入非对称防御" in action or "恢复期再触发防御" in action:
        return "acute"
    if "宏观慢熊" in action:
        return "macro"
    if "再平衡" in action or "恢复" in action or "防御" in action:
        return "rebalance"
    return "rebalance"


def _assumption_for_year(
    assumptions: dict[int, AnnualFrictionAssumption],
    year: int,
) -> AnnualFrictionAssumption:
    if year in assumptions:
        return assumptions[year]
    available = sorted(assumptions)
    if year < available[0]:
        return assumptions[available[0]]
    return assumptions[available[-1]]


def _event_bps(assumption: AnnualFrictionAssumption, event: str) -> float:
    if event == "acute":
        return assumption.acute_event_bps
    if event == "macro":
        return assumption.macro_event_bps
    if event == "rebalance":
        return assumption.rebalance_event_bps
    return 0.0


def apply_calibrated_friction(
    research_nav: pd.Series,
    exposures: pd.DataFrame,
    assumptions: dict[int, AnnualFrictionAssumption],
    name: str = "calibrated_base_case",
    tax_multiplier: float = 1.0,
    broker_multiplier: float = 1.0,
    operational_multiplier: float = 1.0,
    event_multiplier: float = 1.0,
) -> CalibratedFrictionResult:
    research_nav = research_nav.sort_index().astype(float)
    exposure = exposures.reindex(research_nav.index).ffill().bfill()
    if research_nav.empty:
        raise ValueError("research_nav cannot be empty")
    if (research_nav <= 0).any():
        raise ValueError("research_nav must contain only positive values")
    missing = [col for col in ["SPY", "QQQ", "SHV", "action", "event_turnover"] if col not in exposure.columns]
    if missing:
        raise ValueError(f"exposure frame missing columns: {missing}")

    research_returns = research_nav.pct_change().fillna(0.0)
    current_nav = float(research_nav.iloc[0])
    nav_values: list[float] = []
    rows: list[dict] = []

    for current_date, research_return in research_returns.items():
        assumption = _assumption_for_year(assumptions, int(current_date.year))
        row = exposure.loc[current_date]
        start_nav = current_nav
        tax_drag = (
            (
                float(row["SPY"]) * assumption.spy_dividend_yield
                + float(row["QQQ"]) * assumption.qqq_dividend_yield
            )
            * assumption.withholding_tax_rate
            * tax_multiplier
            / 252.0
        )
        broker_drag = (
            float(row["SHV"])
            * (assumption.broker_spread_bps * broker_multiplier / 10_000.0)
            / 252.0
        )
        operational_drag = (
            assumption.operational_failure_bps
            * operational_multiplier
            / 10_000.0
            / 252.0
        )
        event = classify_execution_event(str(row["action"]))
        event_drag = (
            max(float(row["event_turnover"]), 0.0)
            * _event_bps(assumption, event)
            * event_multiplier
            / 10_000.0
        )
        tax_cost = start_nav * tax_drag
        broker_cost = start_nav * broker_drag
        operational_cost = start_nav * operational_drag
        event_cost = start_nav * event_drag
        research_pnl = start_nav * float(research_return)
        total_cost = tax_cost + broker_cost + operational_cost + event_cost
        current_nav = max(start_nav + research_pnl - total_cost, 0.0)
        nav_values.append(current_nav)
        rows.append(
            {
                "date": current_date,
                "year": int(current_date.year),
                "start_nav": start_nav,
                "research_return": float(research_return),
                "research_pnl": research_pnl,
                "tax_cost": tax_cost,
                "broker_cost": broker_cost,
                "operational_cost": operational_cost,
                "event_cost": event_cost,
                "total_cost": total_cost,
                "end_nav": current_nav,
                "event": event,
                "spy_exposure": float(row["SPY"]),
                "qqq_exposure": float(row["QQQ"]),
                "shv_exposure": float(row["SHV"]),
                "event_turnover": float(row["event_turnover"]),
            }
        )

    nav = pd.Series(nav_values, index=research_nav.index, name=name)
    ledger = pd.DataFrame(rows).set_index("date")
    cagr = calculate_cagr(nav)
    mdd = calculate_mdd(nav)
    research_cagr = calculate_cagr(research_nav)
    research_mdd = calculate_mdd(research_nav)
    return CalibratedFrictionResult(
        name=name,
        nav=nav,
        ledger=ledger,
        cagr=cagr,
        mdd=mdd,
        final=float(nav.iloc[-1]),
        research_cagr=research_cagr,
        research_mdd=research_mdd,
        cagr_delta=cagr - research_cagr,
        mdd_delta=mdd - research_mdd,
        tax_cost=float(ledger["tax_cost"].sum()),
        broker_cost=float(ledger["broker_cost"].sum()),
        operational_cost=float(ledger["operational_cost"].sum()),
        event_cost=float(ledger["event_cost"].sum()),
    )


def annual_calibrated_ledger(result: CalibratedFrictionResult) -> pd.DataFrame:
    rows = []
    for year, group in result.ledger.groupby("year"):
        rows.append(
            {
                "year": int(year),
                "start_nav": float(group["start_nav"].iloc[0]),
                "end_nav": float(group["end_nav"].iloc[-1]),
                "research_pnl": float(group["research_pnl"].sum()),
                "tax_cost": float(group["tax_cost"].sum()),
                "broker_cost": float(group["broker_cost"].sum()),
                "operational_cost": float(group["operational_cost"].sum()),
                "event_cost": float(group["event_cost"].sum()),
                "total_cost": float(group["total_cost"].sum()),
            }
        )
    return pd.DataFrame(rows).set_index("year")


def run_calibrated_friction_audit(
    matrix_path: Path = FRICTION_MATRIX_FILE,
) -> dict[str, CalibratedFrictionResult]:
    assumptions = load_annual_friction_assumptions(matrix_path)
    research = production_candidate_nav()
    exposures = build_production_exposure_frame()
    return {
        "calibrated_base_case": apply_calibrated_friction(
            research.nav,
            exposures,
            assumptions,
            name="calibrated_base_case",
        ),
        "calibrated_harsh_case": apply_calibrated_friction(
            research.nav,
            exposures,
            assumptions,
            name="calibrated_harsh_case",
            tax_multiplier=1.15,
            broker_multiplier=1.75,
            operational_multiplier=2.0,
            event_multiplier=2.0,
        ),
    }


def run_trend_satellite_uplift_audit(
    allow_download: bool = True,
    trend_snapshot_dir: Path = TREND_SNAPSHOT_DIR,
) -> list[TrendSatelliteAuditRow]:
    rows: list[TrendSatelliteAuditRow] = []

    dbmf_satellite, dbmf_sources = run_static_satellite_sleeve(
        "qqq_gld_dbmf",
        DBMF_SATELLITE_WEIGHTS,
        allow_download=allow_download,
        trend_snapshot_dir=trend_snapshot_dir,
    )
    dbmf_start = dbmf_satellite.nav.index.min()
    dbmf_end = dbmf_satellite.nav.index.max()
    dbmf_baseline, _ = run_static_satellite_sleeve(
        "baseline_qqq_spy_gld_dbmf_window",
        BASELINE_SATELLITE_WEIGHTS,
        start=dbmf_start,
        end=dbmf_end,
        allow_download=allow_download,
        trend_snapshot_dir=trend_snapshot_dir,
    )
    rows.append(
        evaluate_trend_satellite(
            "qqq_gld_dbmf",
            dbmf_satellite,
            dbmf_baseline,
            dbmf_sources,
            confidence="LOW_SHORT_HISTORY",
        )
    )

    momentum_satellite, momentum_sources = run_qqq_cash_momentum_sleeve(
        allow_download=allow_download,
        trend_snapshot_dir=trend_snapshot_dir,
    )
    momentum_start = momentum_satellite.nav.index.min()
    momentum_end = momentum_satellite.nav.index.max()
    momentum_baseline, _ = run_static_satellite_sleeve(
        "baseline_qqq_spy_gld_full_window",
        BASELINE_SATELLITE_WEIGHTS,
        start=momentum_start,
        end=momentum_end,
        allow_download=allow_download,
        trend_snapshot_dir=trend_snapshot_dir,
    )
    rows.append(
        evaluate_trend_satellite(
            "gld_qqq_cash_12m_momentum",
            momentum_satellite,
            momentum_baseline,
            momentum_sources,
            confidence="FULL_WINDOW_RULE_BASED",
        )
    )
    return rows


def evaluate_cash_proxy(
    defensive_run: CashProxyDefensiveRun,
) -> CashProxyAuditRow:
    proxy_nav = defensive_run.attribution.history["nav_end"]
    proxy_start = proxy_nav.index.min()
    proxy_end = proxy_nav.index.max()
    baseline_defensive = run_baseline_defensive_for_window(proxy_start, proxy_end)
    baseline_research = build_research_nav_from_defensive(
        baseline_defensive,
        f"baseline_{defensive_run.ticker}",
    )
    research = build_research_nav_from_defensive(defensive_run.attribution, defensive_run.ticker)
    start = max(research.nav.index.min(), baseline_research.nav.index.min())
    end = min(research.nav.index.max(), baseline_research.nav.index.max())

    research_slice = _slice_nav(research.nav, start, end)
    baseline_research_slice = _slice_nav(baseline_research.nav, start, end)
    real_nav = apply_real_world_friction(research_slice, BASE_REAL_WORLD_SCENARIO)
    harsh_nav = apply_real_world_friction(research_slice, HARSH_REAL_WORLD_SCENARIO)
    baseline_real_nav = apply_real_world_friction(
        baseline_research_slice,
        BASE_REAL_WORLD_SCENARIO,
    )

    real_cagr = calculate_cagr(real_nav)
    real_mdd = calculate_mdd(real_nav)
    baseline_real_cagr = calculate_cagr(baseline_real_nav)
    baseline_real_mdd = calculate_mdd(baseline_real_nav)
    defensive_nav = defensive_run.attribution.history["nav_end"].loc[start:end]
    baseline_cash_pnl = _cash_pnl(baseline_defensive, start, end)
    cash_pnl = _cash_pnl(defensive_run.attribution, start, end)
    baseline_macro = _macro_net_pnl(baseline_defensive, start, end)
    macro = _macro_net_pnl(defensive_run.attribution, start, end)

    return CashProxyAuditRow(
        ticker=defensive_run.ticker,
        source=defensive_run.source,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        years=_years(research_slice.index),
        research_cagr=calculate_cagr(research_slice),
        research_mdd=calculate_mdd(research_slice),
        real_cagr=real_cagr,
        real_mdd=real_mdd,
        harsh_cagr=calculate_cagr(harsh_nav),
        harsh_mdd=calculate_mdd(harsh_nav),
        baseline_real_cagr=baseline_real_cagr,
        baseline_real_mdd=baseline_real_mdd,
        real_cagr_delta=real_cagr - baseline_real_cagr,
        real_mdd_delta=real_mdd - baseline_real_mdd,
        final_nav_delta=float(real_nav.iloc[-1] - baseline_real_nav.iloc[-1]),
        defensive_cagr=calculate_cagr(defensive_nav),
        defensive_mdd=calculate_mdd(defensive_nav),
        cash_pnl_delta=cash_pnl - baseline_cash_pnl,
        macro_net_pnl_delta=macro - baseline_macro,
        avg_cash_weight=_avg_cash_weight(defensive_run.attribution, start, end),
        high_cash_day_pct=_high_cash_day_pct(defensive_run.attribution, start, end),
        max_daily_weight_shock=_max_daily_weight_shock(defensive_run.attribution, start, end),
        pass_mdd_guardrail=(real_mdd >= baseline_real_mdd and calculate_mdd(harsh_nav) >= -0.17),
    )


def run_cash_proxy_uplift_audit(
    tickers: list[str] | None = None,
    allow_download: bool = True,
    snapshot_dir: Path = CASH_PROXY_SNAPSHOT_DIR,
) -> list[CashProxyAuditRow]:
    rows: list[CashProxyAuditRow] = []
    for ticker in tickers or CASH_PROXY_TICKERS:
        defensive_run = run_defensive_cash_proxy(
            ticker,
            allow_download=allow_download,
            snapshot_dir=snapshot_dir,
        )
        rows.append(
            evaluate_cash_proxy(defensive_run)
        )
    return rows


def print_report(rows: list[CashProxyAuditRow]) -> None:
    print("=" * 172)
    print("  CAGR Uplift Audit: Cash Proxy Replacement Only")
    print("=" * 172)
    print(
        "  Boundary: 90/10 topology, MA150, 8w recovery, satellite size, and "
        "real-world friction model are unchanged."
    )
    print("  Short-history proxies are compared only against same-window SHV baselines.")
    print("-" * 172)
    print(
        f"  {'Proxy':<6} {'Window':<23} {'Years':>5} "
        f"{'Real CAGR':>10} {'Real MDD':>9} {'Base CAGR':>10} {'Base MDD':>9} "
        f"{'CAGR Δ':>8} {'MDD Δ':>8} {'Final Δ':>11} {'Cash PnL Δ':>12} "
        f"{'Macro Δ':>11} {'High Cash':>10} {'Verdict':>9}"
    )
    for row in rows:
        verdict = "PASS" if row.pass_mdd_guardrail and row.real_cagr_delta > 0 else "WATCH"
        print(
            f"  {row.ticker:<6} {row.start}..{row.end:<10} {row.years:>5.1f} "
            f"{row.real_cagr:>9.2%} {row.real_mdd:>8.2%} "
            f"{row.baseline_real_cagr:>9.2%} {row.baseline_real_mdd:>8.2%} "
            f"{row.real_cagr_delta:>7.2%} {row.real_mdd_delta:>7.2%} "
            f"{row.final_nav_delta:>11,.2f} {row.cash_pnl_delta:>12,.2f} "
            f"{row.macro_net_pnl_delta:>11,.2f} {row.high_cash_day_pct:>9.2%} {verdict:>9}"
        )
    print("-" * 172)
    print("  Guardrail:")
    print("  - PASS requires same-window real-world MDD no worse than SHV baseline and harsh MDD >= -17%.")
    print("  - A higher short-window CAGR is not production approval.")
    print("  - Stage 9.5 remains read-only; this audit does not change live execution status.")
    print("=" * 172)


def _fmt_optional(value: float | None, percentage: bool = True) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2%}" if percentage else f"{value:,.2f}"


def print_trend_report(rows: list[TrendSatelliteAuditRow]) -> None:
    print("\n" + "=" * 172)
    print("  CAGR Uplift Audit: Satellite-Level CTA / Trend Following")
    print("=" * 172)
    print("  Boundary: total satellite remains 10%; defensive core and Stage 9.5 are unchanged.")
    print("  DBMF is short-history and cannot be read as a 2005 full-cycle conclusion.")
    print("-" * 172)
    print(
        f"  {'Scenario':<28} {'Confidence':<22} {'Window':<23} {'Years':>5} "
        f"{'Real CAGR':>10} {'Real MDD':>9} {'Base CAGR':>10} {'Base MDD':>9} "
        f"{'CAGR Δ':>8} {'MDD Δ':>8} {'2022 Ret':>9} {'2022 MDD':>9} {'Verdict':>9}"
    )
    for row in rows:
        verdict = "PASS" if row.pass_mdd_guardrail and row.real_cagr_delta > 0 else "WATCH"
        print(
            f"  {row.scenario:<28} {row.confidence:<22} {row.start}..{row.end:<10} "
            f"{row.years:>5.1f} {row.real_cagr:>9.2%} {row.real_mdd:>8.2%} "
            f"{row.baseline_real_cagr:>9.2%} {row.baseline_real_mdd:>8.2%} "
            f"{row.real_cagr_delta:>7.2%} {row.real_mdd_delta:>7.2%} "
            f"{_fmt_optional(row.crisis_2022_return):>9} {_fmt_optional(row.crisis_2022_mdd):>9} "
            f"{verdict:>9}"
        )
    print("-" * 172)
    print("  Guardrail:")
    print("  - PASS requires same-window real-world MDD no worse than baseline and harsh MDD >= -17%.")
    print("  - Short-history CTA proxy results must remain downgraded until a long-history proxy exists.")
    print("  - A trend sleeve cannot increase the total satellite above 10%.")
    print("=" * 172)


def print_calibrated_friction_report(results: dict[str, CalibratedFrictionResult]) -> None:
    research = production_candidate_nav()
    static_base = apply_real_world_friction(research.nav, BASE_REAL_WORLD_SCENARIO)
    static_harsh = apply_real_world_friction(research.nav, HARSH_REAL_WORLD_SCENARIO)
    static_rows = {
        "research_current": (research.nav, 0.0, 0.0, 0.0, 0.0),
        "static_unlevered_base": (static_base, None, None, None, None),
        "static_unlevered_harsh": (static_harsh, None, None, None, None),
    }

    print("\n" + "=" * 172)
    print("  CAGR Uplift Audit: Calibrated Friction Matrix")
    print("=" * 172)
    print("  Boundary: this is an accounting audit only; it does not change strategy weights or live status.")
    print("  Static drag remains the conservative pressure lens; calibrated drag tests possible over-penalization.")
    print("-" * 172)
    print(
        f"  {'Scenario':<26} {'CAGR':>8} {'MDD':>9} {'Final NAV':>13} "
        f"{'Tax':>11} {'Broker':>11} {'Ops':>11} {'Events':>11} {'CAGR Δ vs research':>17}"
    )
    for name, (nav, tax, broker, ops, events) in static_rows.items():
        cagr = calculate_cagr(nav)
        mdd = calculate_mdd(nav)
        delta = cagr - research.cagr
        def fmt(value):
            return "N/A" if value is None else f"{value:,.2f}"
        print(
            f"  {name:<26} {cagr:>7.2%} {mdd:>8.2%} {float(nav.iloc[-1]):>13,.2f} "
            f"{fmt(tax):>11} {fmt(broker):>11} {fmt(ops):>11} {fmt(events):>11} {delta:>16.2%}"
        )
    for result in results.values():
        print(
            f"  {result.name:<26} {result.cagr:>7.2%} {result.mdd:>8.2%} {result.final:>13,.2f} "
            f"{result.tax_cost:>11,.2f} {result.broker_cost:>11,.2f} "
            f"{result.operational_cost:>11,.2f} {result.event_cost:>11,.2f} "
            f"{result.cagr_delta:>16.2%}"
        )
    print("-" * 172)
    print("  Interpretation:")
    print("  - Calibrated costs are daily dollar ledger costs, not a flat annual haircut.")
    print("  - Dividend withholding is exposure-weighted for SPY/QQQ only in this model.")
    print("  - Broker spread is applied to SHV/cash exposure; event drag is applied only to estimated turnover on action dates.")
    print("  - Replace this matrix with account statements before treating it as a live tax or broker ledger.")
    print("=" * 172)


def main() -> None:
    cash_rows = run_cash_proxy_uplift_audit()
    print_report(cash_rows)
    trend_rows = run_trend_satellite_uplift_audit()
    print_trend_report(trend_rows)
    calibrated = run_calibrated_friction_audit()
    print_calibrated_friction_report(calibrated)


if __name__ == "__main__":
    main()
