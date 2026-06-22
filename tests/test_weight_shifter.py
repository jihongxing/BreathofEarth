import pytest

from engine.weight_shifter import MarketShiftSignal, WeightShiftConfig, WeightShifter


def _signal(**overrides):
    values = {
        "spy_vol_anomaly": 1.0,
        "tlt_vol_anomaly": 1.0,
        "rolling_corr": -0.2,
        "spy_trend": 0.01,
        "tlt_trend": 0.01,
    }
    values.update(overrides)
    return MarketShiftSignal(**values)


def test_weight_shifter_triggers_on_volatility_anomaly():
    shifter = WeightShifter(
        WeightShiftConfig(
            name="test",
            defense_weights=(0.1, 0.1, 0.25, 0.55),
            vol_anomaly_threshold=2.5,
            use_corr_trigger=False,
        )
    )

    decision = shifter.decide(_signal(spy_vol_anomaly=2.6))

    assert decision.target_weights == (0.1, 0.1, 0.25, 0.55)
    assert decision.action == "进入主动防御"
    assert shifter.trigger_count == 1


def test_weight_shifter_requires_cooldown_before_release():
    shifter = WeightShifter(
        WeightShiftConfig(
            name="test",
            defense_weights=(0.1, 0.1, 0.25, 0.55),
            vol_anomaly_threshold=2.5,
            cooldown_days=2,
            use_corr_trigger=False,
        )
    )

    shifter.decide(_signal(spy_vol_anomaly=3.0))
    hold = shifter.decide(_signal())
    release = shifter.decide(_signal())

    assert hold.target_weights == (0.1, 0.1, 0.25, 0.55)
    assert hold.action is None
    assert release.action == "解除主动防御"
    assert release.target_weights is None
    assert shifter.release_count == 1


def test_weight_shifter_triggers_on_correlation_breakdown():
    shifter = WeightShifter(
        WeightShiftConfig(
            name="test",
            defense_weights=(0.0, 0.0, 0.3, 0.7),
            corr_threshold=0.4,
            use_vol_trigger=False,
        )
    )

    decision = shifter.decide(
        _signal(rolling_corr=0.45, spy_trend=-0.01, tlt_trend=-0.01)
    )

    assert decision.in_defense_mode is True
    assert decision.target_weights == (0.0, 0.0, 0.3, 0.7)


def test_weight_shifter_rejects_invalid_weights():
    with pytest.raises(ValueError):
        WeightShifter(
            WeightShiftConfig(
                name="bad",
                defense_weights=(0.1, 0.1, 0.1, 0.1),
            )
        )
