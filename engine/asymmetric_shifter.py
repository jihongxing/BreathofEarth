"""
Asymmetric active shifting policy for backtest experiments.

Fast exit, slow re-entry:
- NORMAL: hold baseline weights until a crisis signal appears.
- CRISIS: immediately move to defensive weights for a short hard lock.
- RECOVERY: linearly step back toward baseline weights over several weeks.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class AsymmetricShiftSignal:
    """Market inputs used by the asymmetric shifter."""

    spy_vol_anomaly: float
    tlt_vol_anomaly: float
    rolling_corr: float
    spy_trend: float
    tlt_trend: float
    is_weekly_rebalance_day: bool


@dataclass(frozen=True)
class AsymmetricShiftConfig:
    """Configuration for fast-exit / slow-re-entry shifting."""

    name: str
    baseline_weights: tuple[float, float, float, float] = (0.25, 0.25, 0.25, 0.25)
    defense_weights: tuple[float, float, float, float] = (0.05, 0.05, 0.20, 0.70)
    vol_anomaly_threshold: float = 2.0
    corr_threshold: float = 0.5
    crisis_lock_days: int = 15
    recovery_weeks: int = 4
    use_vol_trigger: bool = True
    use_corr_trigger: bool = True


@dataclass(frozen=True)
class AsymmetricShiftDecision:
    """A daily shifter decision."""

    target_weights: tuple[float, float, float, float] | None
    action: str | None
    state: str
    crisis_timer: int
    recovery_step: int


class AsymmetricShifter:
    """Stateful fast-exit / slow-re-entry policy."""

    NORMAL = "NORMAL"
    CRISIS = "CRISIS"
    RECOVERY = "RECOVERY"

    def __init__(self, config: AsymmetricShiftConfig):
        if abs(sum(config.baseline_weights) - 1.0) > 1e-9:
            raise ValueError("baseline_weights must sum to 1.0")
        if abs(sum(config.defense_weights) - 1.0) > 1e-9:
            raise ValueError("defense_weights must sum to 1.0")
        if config.crisis_lock_days < 1:
            raise ValueError("crisis_lock_days must be positive")
        if config.recovery_weeks < 1:
            raise ValueError("recovery_weeks must be positive")

        self.config = config
        self.state = self.NORMAL
        self.crisis_timer = 0
        self.recovery_step = 0
        self.trigger_count = 0
        self.crisis_days = 0
        self.recovery_days = 0
        self.release_count = 0

    def decide(self, signal: AsymmetricShiftSignal) -> AsymmetricShiftDecision:
        if self.state == self.NORMAL:
            if self._is_triggered(signal):
                self.state = self.CRISIS
                self.crisis_timer = self.config.crisis_lock_days
                self.recovery_step = 0
                self.trigger_count += 1
                self.crisis_days += 1
                return self._decision(self.config.defense_weights, "进入非对称防御")
            return self._decision(None, None)

        if self.state == self.CRISIS:
            self.crisis_days += 1
            self.crisis_timer -= 1
            if self.crisis_timer <= 0:
                self.state = self.RECOVERY
                self.recovery_step = 0
            return self._decision(self.config.defense_weights, None)

        if self.state == self.RECOVERY:
            self.recovery_days += 1
            if self._is_triggered(signal):
                self.state = self.CRISIS
                self.crisis_timer = self.config.crisis_lock_days
                self.recovery_step = 0
                self.trigger_count += 1
                self.crisis_days += 1
                return self._decision(self.config.defense_weights, "恢复期再触发防御")

            if signal.is_weekly_rebalance_day:
                self.recovery_step += 1

            if self.recovery_step >= self.config.recovery_weeks:
                self.state = self.NORMAL
                self.release_count += 1
                return self._decision(None, "完成非对称复位")

            return self._decision(self._recovery_weights(), "非对称分批复位" if signal.is_weekly_rebalance_day else None)

        return self._decision(None, None)

    def _is_triggered(self, signal: AsymmetricShiftSignal) -> bool:
        vol_trigger = (
            self.config.use_vol_trigger
            and (
                signal.spy_vol_anomaly >= self.config.vol_anomaly_threshold
                or signal.tlt_vol_anomaly >= self.config.vol_anomaly_threshold
            )
        )
        corr_trigger = (
            self.config.use_corr_trigger
            and signal.rolling_corr >= self.config.corr_threshold
            and signal.spy_trend < 0
            and signal.tlt_trend < 0
        )
        return vol_trigger or corr_trigger

    def _recovery_weights(self) -> tuple[float, float, float, float]:
        alpha = self.recovery_step / self.config.recovery_weeks
        return tuple(
            defense + alpha * (base - defense)
            for defense, base in zip(self.config.defense_weights, self.config.baseline_weights)
        )

    def _decision(self, target, action) -> AsymmetricShiftDecision:
        return AsymmetricShiftDecision(
            target_weights=target,
            action=action,
            state=self.state,
            crisis_timer=self.crisis_timer,
            recovery_step=self.recovery_step,
        )
