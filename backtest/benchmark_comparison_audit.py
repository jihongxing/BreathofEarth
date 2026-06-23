"""
Benchmark comparison audit for the 90/10 production candidate.

This audit compares the candidate against public fund/ETF benchmarks with two
candidate lenses:

- research NAV: the current 90/10 research result
- real-world base NAV: the same curve after parameterized real-world frictions

Benchmarks are evaluated only on overlapping date ranges. This matters because
RPAR starts much later than the 2005 research window.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from backtest.real_world_friction_audit import (
    RealWorldFrictionScenario,
    apply_real_world_friction,
    production_candidate_nav,
)
from engine.portfolio_aggregator import calculate_cagr, calculate_mdd


BENCHMARK_DIR = Path("data") / "benchmarks"
DEFAULT_START = "2005-01-03"
DEFAULT_END = "2026-04-30"

BENCHMARKS = {
    "VBIAX": "Vanguard Balanced Index Fund",
    "PRPFX": "Permanent Portfolio Fund",
    "RPAR": "RPAR Risk Parity ETF",
}


@dataclass(frozen=True)
class BenchmarkRow:
    benchmark: str
    label: str
    start: str
    end: str
    years: float
    candidate_research_cagr: float
    candidate_research_mdd: float
    candidate_real_world_cagr: float
    candidate_real_world_mdd: float
    benchmark_cagr: float
    benchmark_mdd: float
    real_world_cagr_edge: float
    real_world_mdd_edge: float


def _validate_positive(series: pd.Series, symbol: str, source: Path | str) -> pd.Series:
    clean = pd.to_numeric(series, errors="coerce").dropna().sort_index()
    if clean.empty:
        raise ValueError(f"empty benchmark series for {symbol}: {source}")
    bad = clean[clean <= 0]
    if not bad.empty:
        samples = ", ".join(
            f"{idx.date()}={value:.4f}" for idx, value in bad.head(5).items()
        )
        raise ValueError(f"non-positive benchmark prices for {symbol} in {source}: {samples}")
    clean.name = symbol
    return clean


def load_cached_benchmark(symbol: str, benchmark_dir: Path = BENCHMARK_DIR) -> pd.Series:
    path = benchmark_dir / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path, index_col="date", parse_dates=True).sort_index()
    column = "adj_close" if "adj_close" in df.columns else df.columns[0]
    return _validate_positive(df[column], symbol, path)


def _parse_yahoo_chart_payload(symbol: str, payload: dict) -> pd.Series:
    chart = payload.get("chart") if isinstance(payload, dict) else None
    result = chart.get("result") if isinstance(chart, dict) else None
    if not result:
        error = chart.get("error") if isinstance(chart, dict) else None
        raise RuntimeError(f"Yahoo chart returned no result for {symbol}: {error}")
    item = result[0]
    timestamps = item.get("timestamp") or []
    indicators = item.get("indicators") or {}
    adjclose = indicators.get("adjclose") or []
    if not timestamps or not adjclose:
        raise RuntimeError(f"Yahoo chart returned no adjusted close for {symbol}")
    values = adjclose[0].get("adjclose") if isinstance(adjclose[0], dict) else None
    if not values:
        raise RuntimeError(f"Yahoo chart adjusted close is empty for {symbol}")
    index = pd.to_datetime(timestamps, unit="s").normalize()
    series = pd.Series(values, index=index, name=symbol)
    return _validate_positive(series, symbol, f"yahoo_chart:{symbol}")


def download_benchmark_yahoo_chart(
    symbol: str,
    start: str = DEFAULT_START,
    end: str = DEFAULT_END,
) -> pd.Series:
    period1 = int(pd.Timestamp(start).timestamp())
    # Yahoo chart treats period2 as exclusive. Add one day so the requested end
    # date can be included when data exists.
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
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{query}"
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return _parse_yahoo_chart_payload(symbol, payload)


def download_benchmark(symbol: str, start: str = DEFAULT_START, end: str = DEFAULT_END) -> pd.Series:
    try:
        return download_benchmark_yahoo_chart(symbol, start=start, end=end)
    except Exception:
        pass

    import yfinance as yf

    try:
        df = yf.download(symbol, start=start, end=end, auto_adjust=False, progress=False, threads=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                series = df["Adj Close"]
                if isinstance(series, pd.DataFrame):
                    series = series.iloc[:, 0]
            else:
                series = df["Adj Close"]
            return _validate_positive(series, symbol, f"yfinance:{symbol}")
    except Exception as exc:
        raise RuntimeError(f"benchmark download failed for {symbol}") from exc
    raise RuntimeError(f"benchmark download returned empty data for {symbol}")


def save_benchmark(series: pd.Series, benchmark_dir: Path = BENCHMARK_DIR) -> Path:
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    path = benchmark_dir / f"{series.name}.csv"
    frame = series.to_frame("adj_close")
    frame.index.name = "date"
    frame.to_csv(path)
    return path


def load_or_fetch_benchmark(symbol: str, allow_download: bool = True) -> pd.Series:
    try:
        return load_cached_benchmark(symbol)
    except FileNotFoundError:
        if not allow_download:
            raise
    series = download_benchmark(symbol)
    save_benchmark(series)
    return series


def _slice_overlap(candidate_nav: pd.Series, benchmark_nav: pd.Series) -> tuple[pd.Series, pd.Series]:
    frame = pd.DataFrame(
        {
            "candidate": candidate_nav.sort_index().astype(float),
            "benchmark": benchmark_nav.sort_index().astype(float),
        }
    ).dropna(how="any")
    if frame.empty:
        raise ValueError("candidate and benchmark have no overlapping dates")
    return frame["candidate"], frame["benchmark"]


def compare_to_benchmark(
    symbol: str,
    label: str,
    benchmark_nav: pd.Series,
    candidate_research_nav: pd.Series,
    candidate_real_world_nav: pd.Series,
) -> BenchmarkRow:
    research_slice, benchmark_slice = _slice_overlap(candidate_research_nav, benchmark_nav)
    real_world_slice, benchmark_for_real = _slice_overlap(candidate_real_world_nav, benchmark_nav)
    if not research_slice.index.equals(real_world_slice.index):
        shared = research_slice.index.intersection(real_world_slice.index)
        research_slice = research_slice.loc[shared]
        real_world_slice = real_world_slice.loc[shared]
        benchmark_slice = benchmark_slice.loc[shared]
        benchmark_for_real = benchmark_for_real.loc[shared]

    years = max((research_slice.index[-1] - research_slice.index[0]).days / 365.25, 1 / 365.25)
    research_cagr = calculate_cagr(research_slice)
    research_mdd = calculate_mdd(research_slice)
    real_world_cagr = calculate_cagr(real_world_slice)
    real_world_mdd = calculate_mdd(real_world_slice)
    benchmark_cagr = calculate_cagr(benchmark_for_real)
    benchmark_mdd = calculate_mdd(benchmark_for_real)
    return BenchmarkRow(
        benchmark=symbol,
        label=label,
        start=research_slice.index[0].strftime("%Y-%m-%d"),
        end=research_slice.index[-1].strftime("%Y-%m-%d"),
        years=years,
        candidate_research_cagr=research_cagr,
        candidate_research_mdd=research_mdd,
        candidate_real_world_cagr=real_world_cagr,
        candidate_real_world_mdd=real_world_mdd,
        benchmark_cagr=benchmark_cagr,
        benchmark_mdd=benchmark_mdd,
        real_world_cagr_edge=real_world_cagr - benchmark_cagr,
        real_world_mdd_edge=real_world_mdd - benchmark_mdd,
    )


def run_benchmark_comparison_audit(allow_download: bool = True) -> list[BenchmarkRow]:
    research = production_candidate_nav()
    real_world_nav = apply_real_world_friction(
        research.nav,
        RealWorldFrictionScenario(
            name="unlevered_base_case",
            dividend_withholding_drag_bps=55,
            tax_drag_bps=35,
            broker_cash_financing_drag_bps=10,
            operational_failure_drag_bps=20,
            tail_failure_shock_bps=50,
        ),
    )
    rows: list[BenchmarkRow] = []
    for symbol, label in BENCHMARKS.items():
        benchmark = load_or_fetch_benchmark(symbol, allow_download=allow_download)
        rows.append(compare_to_benchmark(symbol, label, benchmark, research.nav, real_world_nav))
    return rows


def print_report(rows: list[BenchmarkRow]) -> None:
    print("=" * 156)
    print("  Benchmark Comparison Audit: Research Lens + Real-World Base Lens")
    print("=" * 156)
    print(
        f"  {'Benchmark':<8} {'Window':<23} {'Years':>5} "
        f"{'Research CAGR':>13} {'Research MDD':>12} "
        f"{'Real CAGR':>10} {'Real MDD':>10} "
        f"{'Bench CAGR':>11} {'Bench MDD':>10} {'CAGR Edge':>10} {'MDD Edge':>9}"
    )
    for row in rows:
        print(
            f"  {row.benchmark:<8} {row.start}..{row.end:<10} {row.years:>5.1f} "
            f"{row.candidate_research_cagr:>12.2%} {row.candidate_research_mdd:>11.2%} "
            f"{row.candidate_real_world_cagr:>9.2%} {row.candidate_real_world_mdd:>9.2%} "
            f"{row.benchmark_cagr:>10.2%} {row.benchmark_mdd:>9.2%} "
            f"{row.real_world_cagr_edge:>9.2%} {row.real_world_mdd_edge:>8.2%}"
        )
    print("-" * 156)
    print("  Interpretation:")
    print("  - Research CAGR is not the live-reference lens.")
    print("  - Real CAGR applies the unlevered base real-world friction scenario.")
    print("  - MDD Edge is positive when the candidate has shallower drawdown than the benchmark.")
    print("  - RPAR has a shorter history, so its comparison window starts later.")
    print("=" * 156)


def main() -> None:
    print_report(run_benchmark_comparison_audit())


if __name__ == "__main__":
    main()
