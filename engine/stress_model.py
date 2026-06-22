"""
Stress slippage model for backtests.

The production portfolio engine keeps its fixed friction cost. This module is
used by stress backtests to estimate asset-specific bid/ask widening when
market volatility is far above its recent history.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


DEFAULT_STRESS_EXTRA_SIDE_RATES = {
    "SPY": 0.0015,
    "TLT": 0.0025,
    "GLD": 0.0035,
    "SHV": 0.0003,
}


@dataclass(frozen=True)
class StressSlippageConfig:
    """Configuration for dynamic per-side slippage."""

    base_side_rate: float = 0.0005
    volatility_window: int = 20
    baseline_window: int = 252
    baseline_min_periods: int = 60
    anomaly_threshold: float = 3.0
    stress_extra_side_rates: dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_STRESS_EXTRA_SIDE_RATES)
    )


@dataclass(frozen=True)
class SlippageEstimate:
    """Cost estimate for a rebalance order."""

    total_cost: float
    buy_notional: float
    sell_notional: float
    stressed_assets: tuple[str, ...]
    max_anomaly: float


class StressSlippageModel:
    """
    Estimate rebalance cost with per-asset stress widening.

    Rates are per side. A normal buy pays +5 bps and a normal sell pays -5 bps,
    which is equivalent to the existing 10 bps turnover cost when all assets
    use the base rate.
    """

    def __init__(
        self,
        volatility_anomaly: pd.DataFrame,
        config: StressSlippageConfig | None = None,
    ):
        self.config = config or StressSlippageConfig()
        self.volatility_anomaly = volatility_anomaly.sort_index()

    @classmethod
    def from_prices(
        cls,
        prices: pd.DataFrame,
        config: StressSlippageConfig | None = None,
    ) -> "StressSlippageModel":
        cfg = config or StressSlippageConfig()
        returns = prices.pct_change(fill_method=None)
        rolling_vol = returns.rolling(cfg.volatility_window).std()
        baseline_vol = (
            rolling_vol.rolling(
                cfg.baseline_window,
                min_periods=cfg.baseline_min_periods,
            )
            .median()
            .shift(1)
        )
        anomaly = rolling_vol / baseline_vol.replace(0, np.nan)
        anomaly = anomaly.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        return cls(anomaly, cfg)

    def side_rate(self, asset: str, current_date) -> float:
        anomaly = self._asset_anomaly(asset, current_date)
        if anomaly >= self.config.anomaly_threshold:
            return self.config.base_side_rate + self.config.stress_extra_side_rates.get(asset, 0.0)
        return self.config.base_side_rate

    def estimate_rebalance_cost(
        self,
        current_positions: np.ndarray,
        target_weights: np.ndarray,
        current_date,
        assets: list[str],
    ) -> SlippageEstimate:
        value = float(np.sum(current_positions))
        if value <= 0:
            return SlippageEstimate(0.0, 0.0, 0.0, tuple(), 0.0)

        target_positions = value * np.array(target_weights, dtype=float)
        deltas = target_positions - current_positions

        total_cost = 0.0
        buy_notional = 0.0
        sell_notional = 0.0
        stressed_assets: list[str] = []
        max_anomaly = 0.0

        for asset, delta in zip(assets, deltas):
            anomaly = self._asset_anomaly(asset, current_date)
            max_anomaly = max(max_anomaly, anomaly)
            if anomaly >= self.config.anomaly_threshold:
                stressed_assets.append(asset)

            notional = abs(float(delta))
            if delta > 0:
                buy_notional += notional
                total_cost += notional * self.side_rate(asset, current_date)
            elif delta < 0:
                sell_notional += notional
                total_cost += notional * self.side_rate(asset, current_date)

        return SlippageEstimate(
            total_cost=total_cost,
            buy_notional=buy_notional,
            sell_notional=sell_notional,
            stressed_assets=tuple(stressed_assets),
            max_anomaly=max_anomaly,
        )

    def _asset_anomaly(self, asset: str, current_date) -> float:
        if asset not in self.volatility_anomaly.columns:
            return 0.0
        ts = pd.Timestamp(current_date)
        if ts not in self.volatility_anomaly.index:
            loc = self.volatility_anomaly.index.searchsorted(ts, side="right") - 1
            if loc < 0:
                return 0.0
            ts = self.volatility_anomaly.index[loc]
        value = self.volatility_anomaly.at[ts, asset]
        if pd.isna(value):
            return 0.0
        return float(value)
