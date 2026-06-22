"""
Macro regime filter for slow stock/bond breakdown experiments.

This module is independent from PortfolioEngine. It detects chronic regimes
where SPY and TLT both trade below long moving averages, then proposes a
defensive inflation-bear allocation.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class MacroRegimeSignal:
    """Inputs for the macro regime filter."""

    spy_price: float
    tlt_price: float
    spy_ma: float
    tlt_ma: float


@dataclass(frozen=True)
class MacroRegimeConfig:
    """Configuration for slow-regime defensive weights."""

    name: str
    ma_window: int = 120
    defense_weights: tuple[float, float, float, float] = (0.10, 0.05, 0.35, 0.50)
    release_on_any_recovery: bool = True


@dataclass(frozen=True)
class MacroRegimeDecision:
    """A daily macro regime decision."""

    target_weights: tuple[float, float, float, float] | None
    action: str | None
    active: bool


class MacroRegimeFilter:
    """Stateful slow stock/bond breakdown detector."""

    def __init__(self, config: MacroRegimeConfig):
        if abs(sum(config.defense_weights) - 1.0) > 1e-9:
            raise ValueError("defense_weights must sum to 1.0")
        if config.ma_window < 20:
            raise ValueError("ma_window should be at least 20 trading days")
        self.config = config
        self.active = False
        self.trigger_count = 0
        self.release_count = 0
        self.active_days = 0

    def decide(self, signal: MacroRegimeSignal) -> MacroRegimeDecision:
        breakdown = self._is_breakdown(signal)
        recovered = self._is_recovered(signal)

        if self.active:
            if recovered:
                self.active = False
                self.release_count += 1
                return MacroRegimeDecision(None, "解除宏观慢熊防御", False)
            self.active_days += 1
            return MacroRegimeDecision(self.config.defense_weights, None, True)

        if breakdown:
            self.active = True
            self.trigger_count += 1
            self.active_days += 1
            return MacroRegimeDecision(self.config.defense_weights, "进入宏观慢熊防御", True)

        return MacroRegimeDecision(None, None, False)

    def _is_breakdown(self, signal: MacroRegimeSignal) -> bool:
        if signal.spy_ma <= 0 or signal.tlt_ma <= 0:
            return False
        return signal.spy_price < signal.spy_ma and signal.tlt_price < signal.tlt_ma

    def _is_recovered(self, signal: MacroRegimeSignal) -> bool:
        if signal.spy_ma <= 0 or signal.tlt_ma <= 0:
            return False
        spy_recovered = signal.spy_price >= signal.spy_ma
        tlt_recovered = signal.tlt_price >= signal.tlt_ma
        if self.config.release_on_any_recovery:
            return spy_recovered or tlt_recovered
        return spy_recovered and tlt_recovered
