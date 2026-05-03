import json
import inspect
import tempfile
from pathlib import Path

import pytest

from db.database import Database
from engine.cashflow import build_stability_signal
from engine.data_validator import build_data_integrity_signal
from engine.insurance import (
    RecoveryProposal,
    RecoveryStatus,
    build_authority_decision,
    InsuranceState,
    SignalSeverity,
    validate_recovery_proposal,
)
from engine.risk import RiskEngine


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    yield db
    db_path.unlink()


def _save_insurance_decision(db: Database, **kwargs):
    with db.insurance_decision_writer("test"):
        return db.save_insurance_decision(**kwargs)


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


def test_cashflow_deposit_uses_persisted_insurance_authority(temp_db):
    from engine.cashflow import CashflowEngine

    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    locked = build_authority_decision(InsuranceState.LOCKED, reasons=["locked"])
    _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=locked,
        risk_score=1.0,
        hard_blocks=["locked"],
        source_signals=[{"source": "broker", "hard_veto": True}],
    )

    result = CashflowEngine(temp_db).deposit(1_000.0, "alice", portfolio_id="us")

    assert result.status == "ERROR"
    assert "Insurance Layer blocked deposit" in result.message


def test_cashflow_deposit_without_persisted_insurance_decision_fails_closed(temp_db):
    from engine.cashflow import CashflowEngine

    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])

    result = CashflowEngine(temp_db).deposit(1_000.0, "alice", portfolio_id="us")

    assert result.status == "ERROR"
    assert result.extra["insurance_state"] == "LOCKED"
    assert "missing persisted InsuranceDecision" in result.extra["reasons"]


def test_insurance_decision_write_requires_controlled_context(temp_db):
    decision = build_authority_decision(InsuranceState.SAFE, reasons=["safe"])

    with pytest.raises(ValueError, match="受控写入上下文"):
        temp_db.save_insurance_decision(
            portfolio_id="us",
            previous_state="SAFE",
            decision=decision,
            risk_score=0.0,
            hard_blocks=[],
            source_signals=[],
        )


def test_cashflow_execution_methods_do_not_accept_external_insurance_decision():
    from engine.cashflow import CashflowEngine

    assert "insurance_decision" not in inspect.signature(CashflowEngine.deposit).parameters
    assert "insurance_decision" not in inspect.signature(CashflowEngine.execute_withdrawal).parameters


def test_cashflow_deposit_audit_references_insurance_decision_id(temp_db):
    from engine.cashflow import CashflowEngine

    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    safe = build_authority_decision(InsuranceState.SAFE, reasons=["safe"])
    decision_id = _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=safe,
        risk_score=0.0,
        hard_blocks=[],
        source_signals=[],
    )

    result = CashflowEngine(temp_db).deposit(1_000.0, "alice", portfolio_id="us")

    assert result.status == "SUCCESS"
    with temp_db._conn() as conn:
        tx = conn.execute(
            "SELECT reason FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("us",),
        ).fetchone()
    audit = temp_db.get_audit_log(limit=1)[0]

    assert f"InsuranceDecision={decision_id}" in tx["reason"]
    assert f"InsuranceDecision={decision_id}" in audit["detail"]


def test_cashflow_withdrawal_audit_references_insurance_decision_id(temp_db):
    from engine.cashflow import CashflowEngine

    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    safe = build_authority_decision(InsuranceState.SAFE, reasons=["safe"])
    decision_id = _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=safe,
        risk_score=0.0,
        hard_blocks=[],
        source_signals=[],
    )
    temp_db.create_withdrawal_request(
        withdrawal_id="wd-audit",
        amount=1_000.0,
        reason="cash need",
        requester="alice",
        expires_at="2099-01-01",
        portfolio_id="us",
        required_approvals=1,
        cooling_days=0,
    )
    temp_db.update_withdrawal_status("wd-audit", "APPROVED")

    result = CashflowEngine(temp_db).execute_withdrawal("wd-audit", "bob")

    assert result.status == "SUCCESS"
    with temp_db._conn() as conn:
        tx = conn.execute(
            "SELECT reason FROM transactions WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
            ("us",),
        ).fetchone()
    audit = temp_db.get_audit_log(limit=1)[0]

    assert f"InsuranceDecision={decision_id}" in tx["reason"]
    assert f"InsuranceDecision={decision_id}" in audit["detail"]


def test_governance_request_without_persisted_insurance_decision_fails_closed(temp_db):
    from engine.governance import WithdrawalGovernance

    result = WithdrawalGovernance(temp_db).request_withdrawal(
        amount=1_000.0,
        reason="cash need",
        requester="alice",
        portfolio_id="us",
    )

    assert result.status == "ERROR"
    assert result.extra["insurance_state"] == "LOCKED"


def test_protected_decision_blocks_alpha_arena_execution(temp_db):
    from engine.alpha.arena import StrategyArena

    arena = StrategyArena(temp_db)
    decision = build_authority_decision(InsuranceState.PROTECTED, reasons=["protected"])

    result = arena.enforce_alpha_authority(decision)

    assert result["action"] == "BLOCKED"
    assert result["reason"] == "Insurance Layer blocked Alpha execution"


def test_alpha_arena_without_persisted_insurance_decision_fails_closed(temp_db):
    from engine.alpha.arena import StrategyArena

    result = StrategyArena(temp_db).run_all(
        portfolio_id="us",
        current_date="2026-01-02",
        spy_price=450.0,
    )

    assert result == [
        {
            "strategy_id": "ALL",
            "action": "BLOCKED",
            "reason": "Insurance Layer blocked Alpha execution",
            "insurance_state": "LOCKED",
            "reasons": ["missing persisted InsuranceDecision"],
        }
    ]


def test_alpha_arena_run_all_uses_persisted_insurance_authority(temp_db):
    from engine.alpha.arena import StrategyArena

    protected = build_authority_decision(InsuranceState.PROTECTED, reasons=["protected"])
    _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=protected,
        risk_score=0.6,
        hard_blocks=[],
        source_signals=[{"source": "market"}],
    )

    result = StrategyArena(temp_db).run_all(
        portfolio_id="us",
        current_date="2026-01-02",
        spy_price=450.0,
    )

    assert result == [
        {
            "strategy_id": "ALL",
            "action": "BLOCKED",
            "reason": "Insurance Layer blocked Alpha execution",
            "insurance_state": "PROTECTED",
            "reasons": ["protected"],
        }
    ]


def test_alpha_arena_run_all_does_not_accept_external_insurance_decision():
    from engine.alpha.arena import StrategyArena

    assert "insurance_decision" not in inspect.signature(StrategyArena.run_all).parameters


def test_single_alpha_strategy_run_uses_persisted_insurance_authority(temp_db):
    from engine.alpha.covered_call import CoveredCallStrategy

    strategy = CoveredCallStrategy(temp_db)
    strategy.ensure_registered("us")
    protected = build_authority_decision(InsuranceState.PROTECTED, reasons=["protected"])
    _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=protected,
        risk_score=0.6,
        hard_blocks=[],
        source_signals=[],
    )

    result = strategy.run(
        portfolio_id="us",
        current_date="2026-01-02",
        spy_price=450.0,
    )

    assert result["action"] == "BLOCKED"
    assert result["insurance_state"] == "PROTECTED"


def test_alpha_transaction_requires_current_insurance_decision(temp_db):
    safe = build_authority_decision(InsuranceState.SAFE, reasons=["safe"])
    decision_id = _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=safe,
        risk_score=0.0,
        hard_blocks=[],
        source_signals=[],
    )

    with pytest.raises(ValueError, match="Alpha 执行缺少 InsuranceDecision"):
        temp_db.save_alpha_transaction(
            strategy_id="covered_call",
            portfolio_id="us",
            date="2026-01-02",
            action="SELL_CALL",
        )

    temp_db.save_alpha_transaction(
        strategy_id="covered_call",
        portfolio_id="us",
        date="2026-01-02",
        action="SELL_CALL",
        insurance_decision_id=decision_id,
    )

    tx = temp_db.get_alpha_transactions("covered_call", portfolio_id="us", limit=1)[0]
    assert tx["insurance_decision_id"] == decision_id


def test_enabling_alpha_strategy_requires_insurance_authority(temp_db):
    from engine.alpha.covered_call import CoveredCallStrategy

    CoveredCallStrategy(temp_db).ensure_registered("us")

    with pytest.raises(ValueError, match="Alpha 执行缺少 InsuranceDecision"):
        temp_db.update_strategy_status("covered_call", "ENABLED", portfolio_id="us")


def test_alpha_ledger_requires_current_persisted_insurance_decision(temp_db):
    safe = build_authority_decision(InsuranceState.SAFE, reasons=["safe"])
    stale_decision_id = _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=safe,
        risk_score=0.0,
        hard_blocks=[],
        source_signals=[],
    )
    locked = build_authority_decision(InsuranceState.LOCKED, reasons=["locked"])
    latest_decision_id = _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=locked,
        risk_score=1.0,
        hard_blocks=["locked"],
        source_signals=[],
    )

    with pytest.raises(ValueError, match="不是当前组合最新授权"):
        temp_db.record_alpha_ledger_entry(
            portfolio_id="us",
            direction="IN",
            amount=1_000.0,
            actor="alice",
            insurance_decision_id=stale_decision_id,
        )

    with pytest.raises(ValueError, match="Insurance Layer blocked Alpha ledger"):
        temp_db.record_alpha_ledger_entry(
            portfolio_id="us",
            direction="IN",
            amount=1_000.0,
            actor="alice",
            insurance_decision_id=latest_decision_id,
        )


def test_alpha_balance_adjustment_is_not_public_bypass():
    assert not hasattr(Database, "adjust_alpha_account_balance")
    assert not hasattr(Database, "update_alpha_account")


def test_alpha_balance_private_adjustment_requires_ledger_transaction(temp_db):
    with pytest.raises(ValueError, match="受控 ledger 事务"):
        temp_db._adjust_alpha_account_balance("us", 1_000.0, note="bypass")


def test_tax_harvester_direct_entry_fails_closed_without_insurance_decision(temp_db):
    from engine.tax_optimizer import HarvestablePosition, TaxLossHarvester

    harvester = TaxLossHarvester(temp_db)
    result = harvester.run_year_end_harvest(
        portfolio_id="us",
        current_prices={"SPY": 450.0},
        current_date="2026-12-31",
    )

    assert result.success is False
    assert "Insurance Layer blocked tax-loss harvest" in result.message
    assert "InsuranceDecision=missing" in result.message

    direct_success = harvester.execute_harvest(
        HarvestablePosition(
            asset="SPY",
            quantity=100,
            cost_basis=50_000.0,
            current_value=45_000.0,
            unrealized_loss=-5_000.0,
            loss_pct=-0.10,
            substitute="VOO",
            purchase_date="2026-01-01",
        ),
        current_date="2026-12-31",
        portfolio_id="us",
    )

    assert direct_success is False
    assert temp_db.get_pending_reversals("2027-02-01", "us") == []


def test_save_and_load_insurance_decision(temp_db):
    decision = build_authority_decision(
        InsuranceState.PROTECTED,
        reasons=["drawdown protection"],
    )

    decision_id = _save_insurance_decision(
        temp_db,
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


def test_latest_insurance_decision_is_loaded_as_authoritative_state(temp_db):
    first = build_authority_decision(InsuranceState.PROTECTED, reasons=["old protection"])
    second = build_authority_decision(InsuranceState.LOCKED, reasons=["broker hard block"])

    _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=first,
        risk_score=0.55,
        hard_blocks=[],
        source_signals=[],
    )
    latest_id = _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="PROTECTED",
        decision=second,
        risk_score=1.0,
        hard_blocks=["broker hard block"],
        source_signals=[{"source": "broker", "hard_veto": True}],
    )

    latest = temp_db.get_latest_insurance_decision("us")

    assert latest["id"] == latest_id
    assert latest["new_state"] == "LOCKED"
    assert latest["hard_blocks"] == ["broker hard block"]


def test_recovery_proposal_can_be_persisted_and_validated(temp_db):
    from datetime import datetime, timedelta

    now = datetime(2026, 1, 3, 12, 0, 0)
    proposal = RecoveryProposal(
        id="rp-1",
        portfolio_id="us",
        from_state=InsuranceState.LOCKED,
        proposed_to_state=InsuranceState.EMERGENCY,
        created_at=now - timedelta(days=2),
        cooldown_until=now - timedelta(hours=1),
        validation_evidence={"broker_sync": "MATCHED", "data_integrity": "OK"},
        unresolved_blocks=[],
        required_approvals=2,
        approvals=["alice", "bob"],
        audit_log_ids=["audit-1"],
        status=RecoveryStatus.APPROVED,
    )

    temp_db.save_recovery_proposal(proposal, actor="alice")
    stored = temp_db.get_recovery_proposal("rp-1")

    assert stored["portfolio_id"] == "us"
    assert stored["from_state"] == "LOCKED"
    assert stored["proposed_to_state"] == "EMERGENCY"
    assert stored["validation_evidence"]["broker_sync"] == "MATCHED"
    assert stored["approvals"] == ["alice", "bob"]

    loaded = temp_db.load_recovery_proposal("rp-1")
    assert validate_recovery_proposal(loaded, now=now).allowed is True


def test_notifier_prefers_insurance_state_for_protection_messages():
    from engine.notifier import format_message

    report = {
        "date": "2026-12-30",
        "state": "IDLE",
        "action": "风险保护已触发",
        "nav": 100000.0,
        "drawdown": 0.05,
        "spy_tlt_corr": 0.1,
        "protection_count": 1,
        "insurance": {"state": "LOCKED"},
    }

    message = format_message(report)

    assert message is not None
    assert "LOCKED" in message


def test_generate_report_prefers_daily_run_insurance_state(temp_db, capsys, monkeypatch):
    import runner.report as report_module

    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    temp_db.save_snapshot(
        date="2026-12-30",
        state="IDLE",
        nav=100000.0,
        positions=[25000.0, 25000.0, 25000.0, 25000.0],
        weights=[0.25, 0.25, 0.25, 0.25],
        drawdown=0.0,
        spy_tlt_corr=0.1,
        action=None,
        trigger_reason=None,
        portfolio_id="us",
    )
    temp_db.record_run(
        "2026-12-30",
        "SUCCESS",
        json.dumps({"insurance": {"state": "LOCKED"}}),
        portfolio_id="us",
    )

    monkeypatch.setattr(report_module, "Database", lambda: temp_db)
    report_module.generate_report(days=0, portfolio_id="us")
    output = capsys.readouterr().out

    assert "当前状态:" in output
    assert "LOCKED" in output


def test_portfolio_state_maps_from_insurance_state_for_compatibility():
    from engine.insurance import portfolio_state_from_insurance_state

    assert portfolio_state_from_insurance_state(InsuranceState.SAFE) == "IDLE"
    assert portfolio_state_from_insurance_state(InsuranceState.DEGRADED) == "IDLE"
    assert portfolio_state_from_insurance_state(InsuranceState.PROTECTED) == "PROTECTION"
    assert portfolio_state_from_insurance_state(InsuranceState.EMERGENCY) == "PROTECTION"
    assert portfolio_state_from_insurance_state(InsuranceState.LOCKED) == "PROTECTION"
