import pytest

from engine.macro_filter import MacroRegimeConfig, MacroRegimeFilter, MacroRegimeSignal


def test_macro_filter_triggers_when_spy_and_tlt_below_ma():
    filt = MacroRegimeFilter(MacroRegimeConfig(name="test"))

    decision = filt.decide(MacroRegimeSignal(90, 95, 100, 100))

    assert decision.active is True
    assert decision.action == "进入宏观慢熊防御"
    assert decision.target_weights == (0.10, 0.05, 0.35, 0.50)
    assert filt.trigger_count == 1


def test_macro_filter_releases_on_any_recovery_by_default():
    filt = MacroRegimeFilter(MacroRegimeConfig(name="test"))

    filt.decide(MacroRegimeSignal(90, 95, 100, 100))
    decision = filt.decide(MacroRegimeSignal(101, 95, 100, 100))

    assert decision.active is False
    assert decision.action == "解除宏观慢熊防御"
    assert decision.target_weights is None
    assert filt.release_count == 1


def test_macro_filter_can_require_both_assets_to_recover():
    filt = MacroRegimeFilter(
        MacroRegimeConfig(name="test", release_on_any_recovery=False)
    )

    filt.decide(MacroRegimeSignal(90, 95, 100, 100))
    hold = filt.decide(MacroRegimeSignal(101, 95, 100, 100))
    release = filt.decide(MacroRegimeSignal(101, 101, 100, 100))

    assert hold.active is True
    assert release.active is False


def test_macro_filter_rejects_invalid_weights():
    with pytest.raises(ValueError):
        MacroRegimeFilter(
            MacroRegimeConfig(name="bad", defense_weights=(0.1, 0.1, 0.1, 0.1))
        )
