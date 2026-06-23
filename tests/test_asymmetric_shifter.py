import pytest

from engine.asymmetric_shifter import (
    AsymmetricShiftConfig,
    AsymmetricShifter,
    AsymmetricShiftSignal,
)


def _signal(**overrides):
    values = {
        "spy_vol_anomaly": 1.0,
        "tlt_vol_anomaly": 1.0,
        "rolling_corr": -0.2,
        "spy_trend": 0.01,
        "tlt_trend": 0.01,
        "is_weekly_rebalance_day": False,
    }
    values.update(overrides)
    return AsymmetricShiftSignal(**values)


def test_asymmetric_shifter_fast_exit_on_volatility():
    shifter = AsymmetricShifter(
        AsymmetricShiftConfig(name="test", crisis_lock_days=3, recovery_weeks=4)
    )

    decision = shifter.decide(_signal(spy_vol_anomaly=2.1))

    assert decision.state == AsymmetricShifter.CRISIS
    assert decision.target_weights == (0.05, 0.05, 0.20, 0.70)
    assert decision.action == "进入非对称防御"
    assert shifter.trigger_count == 1


def test_asymmetric_shifter_slow_reentry_then_release():
    shifter = AsymmetricShifter(
        AsymmetricShiftConfig(name="test", crisis_lock_days=1, recovery_weeks=2)
    )

    shifter.decide(_signal(spy_vol_anomaly=2.1))
    shifter.decide(_signal())
    step = shifter.decide(_signal(is_weekly_rebalance_day=True))
    done = shifter.decide(_signal(is_weekly_rebalance_day=True))

    assert step.state == AsymmetricShifter.RECOVERY
    assert step.target_weights == pytest.approx((0.15, 0.15, 0.225, 0.475))
    assert done.state == AsymmetricShifter.NORMAL
    assert done.target_weights is None
    assert done.action == "完成非对称复位"


def test_asymmetric_shifter_retriggers_during_recovery():
    shifter = AsymmetricShifter(
        AsymmetricShiftConfig(name="test", crisis_lock_days=1, recovery_weeks=4)
    )

    shifter.decide(_signal(spy_vol_anomaly=2.1))
    shifter.decide(_signal())
    decision = shifter.decide(_signal(tlt_vol_anomaly=2.2))

    assert decision.state == AsymmetricShifter.CRISIS
    assert decision.action == "恢复期再触发防御"
    assert shifter.trigger_count == 2


def test_asymmetric_shifter_rejects_bad_weights():
    with pytest.raises(ValueError):
        AsymmetricShifter(
            AsymmetricShiftConfig(name="bad", defense_weights=(0.1, 0.1, 0.1, 0.1))
        )
