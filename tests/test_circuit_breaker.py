from engine.circuit_breaker import CircuitBreakerConfig, CircuitBreakerState


def test_circuit_breaker_triggers_on_deep_drawdown():
    breaker = CircuitBreakerState(CircuitBreakerConfig(trigger_drawdown=-0.15))

    action = breaker.update(current_drawdown=-0.151, macro_volatility_anomaly=1.0)

    assert breaker.active is True
    assert breaker.hold_days == 1
    assert breaker.trigger_count == 1
    assert action == "触发熔断: Hard Hold"


def test_circuit_breaker_requires_recovery_and_min_hold_to_release():
    breaker = CircuitBreakerState(
        CircuitBreakerConfig(
            trigger_drawdown=-0.15,
            release_drawdown=-0.10,
            release_volatility_anomaly=1.5,
            min_hold_days=3,
        )
    )

    breaker.update(current_drawdown=-0.16, macro_volatility_anomaly=2.0)
    assert breaker.update(current_drawdown=-0.09, macro_volatility_anomaly=2.0) is None
    assert breaker.active is True
    assert breaker.update(current_drawdown=-0.09, macro_volatility_anomaly=1.0) == "解除熔断: 回撤与波动恢复"

    assert breaker.active is False
    assert breaker.release_count == 1
