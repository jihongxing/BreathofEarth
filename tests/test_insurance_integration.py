import tempfile
from pathlib import Path

import pytest

from db.database import Database
from engine.cashflow import build_stability_signal
from engine.data_validator import build_data_integrity_signal
from engine.insurance import build_authority_decision, InsuranceState, SignalSeverity
from engine.risk import RiskEngine


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    yield db
    db_path.unlink()


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


def test_locked_decision_blocks_withdrawal_execution_before_capital_moves(temp_db):
    from engine.cashflow import CashflowEngine

    engine = CashflowEngine(temp_db)
    decision = build_authority_decision(InsuranceState.LOCKED, reasons=["locked"])

    result = engine.enforce_withdrawal_authority(decision)

    assert result.status == "ERROR"
    assert "Insurance Layer blocked withdrawal execution" in result.message


def test_protected_decision_blocks_alpha_arena_execution(temp_db):
    from engine.alpha.arena import StrategyArena

    arena = StrategyArena(temp_db)
    decision = build_authority_decision(InsuranceState.PROTECTED, reasons=["protected"])

    result = arena.enforce_alpha_authority(decision)

    assert result["action"] == "BLOCKED"
    assert result["reason"] == "Insurance Layer blocked Alpha execution"


def test_save_and_load_insurance_decision(temp_db):
    decision = build_authority_decision(
        InsuranceState.PROTECTED,
        reasons=["drawdown protection"],
    )

    decision_id = temp_db.save_insurance_decision(
        portfolio_id="us",
        previous_state="SAFE",
        decision=decision,
        risk_score=0.55,
        hard_blocks=[],
        source_signals=[{"source": "market", "reason": "drawdown protection"}],
        actor="insurance",
    )

    stored = temp_db.get_insurance_decision(decision_id)

    assert stored["portfolio_id"] == "us"
    assert stored["new_state"] == "PROTECTED"
    assert stored["risk_score"] == pytest.approx(0.55)
    assert "drawdown protection" in stored["reasons"]
