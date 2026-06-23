"""
Circuit breaker state for backtest experiments.

This module intentionally contains policy state only. It does not place trades
or change production portfolio behavior by itself.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Hard-hold circuit breaker thresholds."""

    trigger_drawdown: float = -0.15
    release_drawdown: float = -0.10
    release_volatility_anomaly: float = 1.5
    min_hold_days: int = 20


@dataclass
class CircuitBreakerState:
    """Tracks whether automatic rebalancing should be frozen."""

    config: CircuitBreakerConfig
    active: bool = False
    hold_days: int = 0
    trigger_count: int = 0
    release_count: int = 0

    def update(self, current_drawdown: float, macro_volatility_anomaly: float) -> str | None:
        """
        Update the breaker and return an audit action if state changed.

        When active, the caller should skip automatic rebalance evaluation for
        the day. Release requires both drawdown recovery and calmer volatility.
        """
        if self.active:
            self.hold_days += 1
            can_release = (
                self.hold_days >= self.config.min_hold_days
                and current_drawdown >= self.config.release_drawdown
                and macro_volatility_anomaly <= self.config.release_volatility_anomaly
            )
            if can_release:
                self.active = False
                self.hold_days = 0
                self.release_count += 1
                return "解除熔断: 回撤与波动恢复"
            return None

        if current_drawdown <= self.config.trigger_drawdown:
            self.active = True
            self.hold_days = 1
            self.trigger_count += 1
            return "触发熔断: Hard Hold"

        return None
