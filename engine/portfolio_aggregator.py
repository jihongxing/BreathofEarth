"""Portfolio-level sleeve aggregation utilities."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class AggregatedPortfolio:
    nav: pd.Series
    sleeve_navs: pd.DataFrame
    cagr: float
    mdd: float
    final: float


def calculate_cagr(nav: pd.Series) -> float:
    if nav.empty:
        raise ValueError("nav cannot be empty")
    years = max((nav.index[-1] - nav.index[0]).days / 365.25, 1 / 365.25)
    return float((nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1)


def calculate_mdd(nav: pd.Series) -> float:
    if nav.empty:
        raise ValueError("nav cannot be empty")
    drawdown = (nav - nav.cummax()) / nav.cummax()
    return float(drawdown.min())


def aggregate_sleeves(
    sleeve_navs: dict[str, pd.Series],
    sleeve_weights: dict[str, float],
    initial_capital: float = 100000.0,
) -> AggregatedPortfolio:
    """Linearly combine independent sleeve NAV curves at fixed initial weights."""
    if not sleeve_navs:
        raise ValueError("at least one sleeve NAV series is required")
    missing = set(sleeve_navs) ^ set(sleeve_weights)
    if missing:
        raise ValueError(f"sleeve NAV/weight mismatch: {sorted(missing)}")
    weight_sum = sum(float(w) for w in sleeve_weights.values())
    if abs(weight_sum - 1.0) > 1e-9:
        raise ValueError(f"sleeve weights must sum to 1.0, got {weight_sum:.12f}")

    normalized_frames = {}
    for name, nav in sleeve_navs.items():
        series = nav.dropna().sort_index().astype(float)
        if series.empty:
            raise ValueError(f"sleeve {name} NAV is empty")
        if (series <= 0).any():
            raise ValueError(f"sleeve {name} NAV contains non-positive values")
        normalized_frames[name] = series / series.iloc[0]

    normalized = pd.DataFrame(normalized_frames).dropna(how="any")
    if normalized.empty:
        raise ValueError("sleeve NAV curves have no overlapping dates")
    weighted_nav = sum(
        normalized[name] * float(sleeve_weights[name]) * initial_capital
        for name in normalized.columns
    )
    weighted_nav.name = "NAV"
    sleeve_values = pd.DataFrame(
        {
            name: normalized[name] * float(sleeve_weights[name]) * initial_capital
            for name in normalized.columns
        },
        index=normalized.index,
    )
    return AggregatedPortfolio(
        nav=weighted_nav,
        sleeve_navs=sleeve_values,
        cagr=calculate_cagr(weighted_nav),
        mdd=calculate_mdd(weighted_nav),
        final=float(weighted_nav.iloc[-1]),
    )
