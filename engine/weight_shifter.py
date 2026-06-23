"""
Active weight shifting policy for backtest experiments.

The shifter is deliberately independent from PortfolioEngine. It observes
market risk signals and proposes a temporary defensive target weight.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketShiftSignal:
    """Market inputs used by active shift experiments."""

    spy_vol_anomaly: float
    tlt_vol_anomaly: float
    rolling_corr: float
    spy_trend: float
    tlt_trend: float


@dataclass(frozen=True)
class WeightShiftConfig:
    """Configuration for active defensive weight shifting."""

    name: str
    defense_weights: tuple[float, float, float, float]
    vol_anomaly_threshold: float | None = None
    corr_threshold: float | None = None
    trend_window: int = 30
    cooldown_days: int = 30
    release_corr_threshold: float = 0.2
    release_vol_anomaly: float = 1.5
    require_both_trends_down: bool = True
    use_vol_trigger: bool = True
    use_corr_trigger: bool = True


@dataclass
class WeightShiftDecision:
    """A daily shifter decision."""

    target_weights: tuple[float, float, float, float] | None
    action: str | None
    in_defense_mode: bool
    cooldown_counter: int


class WeightShifter:
    """Stateful active-shift policy."""

    def __init__(self, config: WeightShiftConfig):
        if abs(sum(config.defense_weights) - 1.0) > 1e-9:
            raise ValueError("defense_weights must sum to 1.0")
        self.config = config
        self.cooldown_counter = 0
        self.in_defense_mode = False
        self.trigger_count = 0
        self.release_count = 0
        self.defense_days = 0

    def decide(self, signal: MarketShiftSignal) -> WeightShiftDecision:
        if self.cooldown_counter > 0:
            self.cooldown_counter -= 1

        triggered = self._is_triggered(signal)
        if triggered and not self.in_defense_mode:
            self.in_defense_mode = True
            self.cooldown_counter = self.config.cooldown_days
            self.trigger_count += 1
            self.defense_days += 1
            return WeightShiftDecision(
                target_weights=self.config.defense_weights,
                action="进入主动防御",
                in_defense_mode=True,
                cooldown_counter=self.cooldown_counter,
            )

        if self.in_defense_mode:
            if self.cooldown_counter == 0 and self._can_release(signal):
                self.in_defense_mode = False
                self.release_count += 1
                return WeightShiftDecision(
                    target_weights=None,
                    action="解除主动防御",
                    in_defense_mode=False,
                    cooldown_counter=0,
                )

            self.defense_days += 1
            return WeightShiftDecision(
                target_weights=self.config.defense_weights,
                action=None,
                in_defense_mode=True,
                cooldown_counter=self.cooldown_counter,
            )

        return WeightShiftDecision(
            target_weights=None,
            action=None,
            in_defense_mode=False,
            cooldown_counter=self.cooldown_counter,
        )

    def _is_triggered(self, signal: MarketShiftSignal) -> bool:
        vol_trigger = False
        if self.config.use_vol_trigger and self.config.vol_anomaly_threshold is not None:
            vol_trigger = (
                signal.spy_vol_anomaly >= self.config.vol_anomaly_threshold
                or signal.tlt_vol_anomaly >= self.config.vol_anomaly_threshold
            )

        corr_trigger = False
        if self.config.use_corr_trigger and self.config.corr_threshold is not None:
            if self.config.require_both_trends_down:
                trend_ok = signal.spy_trend < 0 and signal.tlt_trend < 0
            else:
                trend_ok = signal.spy_trend < 0 or signal.tlt_trend < 0
            corr_trigger = signal.rolling_corr >= self.config.corr_threshold and trend_ok

        return vol_trigger or corr_trigger

    def _can_release(self, signal: MarketShiftSignal) -> bool:
        return (
            signal.rolling_corr <= self.config.release_corr_threshold
            and signal.spy_vol_anomaly <= self.config.release_vol_anomaly
            and signal.tlt_vol_anomaly <= self.config.release_vol_anomaly
        )
