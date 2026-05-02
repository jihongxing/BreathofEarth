import pytest

from engine.cashflow import build_stability_signal
from engine.data_validator import build_data_integrity_signal
from engine.insurance import SignalSeverity
from engine.risk import RiskEngine


def test_risk_engine_emits_market_signal_for_drawdown():
    engine = RiskEngine()
    engine.high_water_mark = 100000.0
    risk_signal = engine.evaluate(
        nav=87000.0,
        spy_tlt_corr=0.1,
        spy_30d_ret=-0.05,
        tlt_30d_ret=0.02,
    )

    signal = engine.to_insurance_signal(risk_signal)

    assert signal.source == "market"
    assert signal.severity == SignalSeverity.ERROR
    assert signal.score > 0
    assert signal.hard_veto is False
    assert "回撤" in signal.reason


def test_stability_signal_warns_below_floor():
    signal = build_stability_signal(
        stability_balance=4000.0,
        nav=100000.0,
    )

    assert signal.source == "stability"
    assert signal.severity == SignalSeverity.WARNING
    assert signal.hard_veto is False
    assert signal.evidence["stability_ratio"] == pytest.approx(0.04)


def test_data_integrity_signal_can_represent_failure():
    signal = build_data_integrity_signal(
        ok=False,
        reason="DATA_INTEGRITY_FAILED",
        evidence={"asset": "SPY"},
    )

    assert signal.source == "data"
    assert signal.severity == SignalSeverity.CRITICAL
    assert signal.hard_veto is True
