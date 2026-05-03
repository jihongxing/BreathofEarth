"""
Alpha 组合隔离测试
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from api.routes.alpha_routes import (
    AlphaLedgerEntryRequest,
    AlphaLedgerWithdrawalRequest,
    AlphaLedgerWithdrawalStatusRequest,
    create_alpha_ledger_entry,
    run_strategy,
    update_alpha_withdrawal_request_status,
    withdraw_alpha_ledger,
)
from db.database import Database
from engine.alpha.arena import StrategyArena
from engine.alpha.covered_call import CoveredCallStrategy
from engine.alpha.momentum import MomentumRotationStrategy
from engine.alpha.registry import list_available_strategies
from engine.cashflow import CashflowEngine
from engine.insurance import build_authority_decision, InsuranceState


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    yield db
    db_path.unlink()


def _save_safe_insurance_decision(db: Database, portfolio_id: str = "us"):
    safe = build_authority_decision(InsuranceState.SAFE, reasons=["test safe"])
    with db.insurance_decision_writer("test"):
        return db.save_insurance_decision(
            portfolio_id=portfolio_id,
            previous_state="SAFE",
            decision=safe,
            risk_score=0.0,
            hard_blocks=[],
            source_signals=[],
        )


def _seed_alpha_cash(db: Database, amount: float, portfolio_id: str = "us"):
    decision_id = _save_safe_insurance_decision(db, portfolio_id)
    db.record_alpha_ledger_entry(
        portfolio_id=portfolio_id,
        direction="IN",
        amount=amount,
        actor="test",
        insurance_decision_id=decision_id,
        note="seed alpha",
    )
    return decision_id


def test_same_strategy_is_scoped_by_portfolio(temp_db):
    strategy = CoveredCallStrategy(temp_db)

    strategy.ensure_registered("us")
    strategy.ensure_registered("cn")

    us_decision_id = _save_safe_insurance_decision(temp_db, "us")
    cn_decision_id = _save_safe_insurance_decision(temp_db, "cn")
    temp_db.update_strategy_status(
        "covered_call",
        "ENABLED",
        portfolio_id="us",
        insurance_decision_id=us_decision_id,
    )
    temp_db.update_strategy_status("covered_call", "DISABLED", portfolio_id="cn")

    us_strategy = temp_db.get_strategy("covered_call", portfolio_id="us")
    cn_strategy = temp_db.get_strategy("covered_call", portfolio_id="cn")

    assert us_strategy is not None
    assert cn_strategy is not None
    assert us_strategy["status"] == "ENABLED"
    assert cn_strategy["status"] == "DISABLED"

    temp_db.save_alpha_transaction(
        strategy_id="covered_call",
        portfolio_id="us",
        date="2026-01-02",
        action="SELL_CALL",
        premium=100,
        pnl=0,
        insurance_decision_id=us_decision_id,
    )
    temp_db.save_alpha_transaction(
        strategy_id="covered_call",
        portfolio_id="cn",
        date="2026-01-02",
        action="SELL_CALL",
        premium=50,
        pnl=0,
        insurance_decision_id=cn_decision_id,
    )

    us_txs = temp_db.get_alpha_transactions("covered_call", portfolio_id="us", limit=10)
    cn_txs = temp_db.get_alpha_transactions("covered_call", portfolio_id="cn", limit=10)

    assert len(us_txs) == 1
    assert len(cn_txs) == 1
    assert us_txs[0]["portfolio_id"] == "us"
    assert cn_txs[0]["portfolio_id"] == "cn"

    temp_db.save_alpha_snapshot(
        strategy_id="covered_call",
        portfolio_id="us",
        date="2026-01-02",
        capital=10_000,
        nav=10_100,
    )
    temp_db.save_alpha_snapshot(
        strategy_id="covered_call",
        portfolio_id="cn",
        date="2026-01-02",
        capital=8_000,
        nav=8_050,
    )

    with temp_db._conn() as conn:
        count = conn.execute(
            """SELECT COUNT(*) AS cnt
               FROM alpha_snapshots
               WHERE strategy_id = ? AND date = ?""",
            ("covered_call", "2026-01-02"),
        ).fetchone()["cnt"]

    assert count == 2


def test_alpha_strategy_uses_independent_ledger_not_main_nav(temp_db):
    strategy = CoveredCallStrategy(temp_db)
    strategy.ensure_registered("us")
    decision_id = _seed_alpha_cash(temp_db, 100_000)
    temp_db.update_strategy_status(
        "covered_call",
        "ENABLED",
        portfolio_id="us",
        insurance_decision_id=decision_id,
    )

    result = strategy.run(
        portfolio_id="us",
        current_date="2026-01-02",
        spy_price=50.0,
    )

    assert result["action"] == "SELL_CALL"
    assert result["capital"] == pytest.approx(10_000.0)
    assert result["alpha_balance"] == pytest.approx(100_000.0)


def test_alpha_manual_run_route_requires_insurance_authority(temp_db):
    strategy = CoveredCallStrategy(temp_db)
    strategy.ensure_registered("us")
    decision_id = _seed_alpha_cash(temp_db, 100_000)
    temp_db.update_strategy_status(
        "covered_call",
        "ENABLED",
        portfolio_id="us",
        insurance_decision_id=decision_id,
    )
    protected = build_authority_decision(InsuranceState.PROTECTED, reasons=["protected"])
    with temp_db.insurance_decision_writer("test"):
        temp_db.save_insurance_decision(
            portfolio_id="us",
            previous_state="SAFE",
            decision=protected,
            risk_score=0.6,
            hard_blocks=[],
            source_signals=[{"source": "market"}],
        )

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            run_strategy(
                strategy_id="covered_call",
                spy_price=450.0,
                portfolio_id="us",
                db=temp_db,
                user={"username": "alice", "role": "admin"},
            )
        )

    assert "Insurance Layer blocked Alpha execution" in str(exc_info.value)


def test_layer_status_reports_alpha_as_independent_layer(temp_db):
    temp_db.ensure_portfolio("us", ["SPY", "TLT", "GLD", "SHV"])
    _seed_alpha_cash(temp_db, 25_000)

    engine = CashflowEngine(temp_db)
    result = engine.get_layer_status("us").to_dict()

    assert result["status"] == "SUCCESS"
    assert result["alpha"]["balance"] == pytest.approx(25_000.0)
    assert result["family_nav"] == pytest.approx(result["nav"] + 25_000.0)
    assert result["alpha"]["ratio"] > 0


def test_sandbox_strategies_are_excluded_from_formal_leaderboard(temp_db):
    strategy = CoveredCallStrategy(temp_db)
    strategy.ensure_registered("us")
    _seed_alpha_cash(temp_db, 40_000)

    board = StrategyArena(temp_db).get_leaderboard("us")
    assert board == []

    available = {item["id"]: item for item in list_available_strategies()}
    assert available["covered_call"]["formal_reporting_eligible"] is False
    assert available["covered_call"]["reporting_scope"] == "sandbox"


def test_arena_reallocation_uses_alpha_budget_not_main_nav_ratio(temp_db):
    covered_call = CoveredCallStrategy(temp_db)
    momentum = MomentumRotationStrategy(temp_db)
    covered_call.ensure_registered("us")
    momentum.ensure_registered("us")

    arena = StrategyArena(temp_db)
    allocation = arena._reallocate(
        [
            {"strategy_id": "covered_call", "verdict": "PASS", "metrics": {"sharpe": 1.0}},
            {"strategy_id": "momentum_rotation", "verdict": "PASS", "metrics": {"sharpe": 3.0}},
        ],
        portfolio_id="us",
    )

    covered_call_db = temp_db.get_strategy("covered_call", portfolio_id="us")
    momentum_db = temp_db.get_strategy("momentum_rotation", portfolio_id="us")

    assert allocation["covered_call"]["allocation_pct"] == pytest.approx(0.125)
    assert allocation["momentum_rotation"]["allocation_pct"] == pytest.approx(0.375)
    assert covered_call_db["allocation_pct"] == pytest.approx(0.125)
    assert momentum_db["allocation_pct"] == pytest.approx(0.375)


def test_quarterly_evaluation_skips_sandbox_only_strategies(temp_db):
    CoveredCallStrategy(temp_db).ensure_registered("us")

    report = StrategyArena(temp_db).quarterly_evaluation("us")

    assert report["evaluations"] == []
    assert report["reporting_scope"] == "formal_only"
    assert report["summary"] == "Insurance Layer blocked Alpha evaluation"
    assert report["insurance_decision_id"] is None


def test_alpha_withdraw_creates_manual_request_without_changing_balance(temp_db):
    _seed_alpha_cash(temp_db, 20_000)

    result = asyncio.run(
        withdraw_alpha_ledger(
            req=AlphaLedgerWithdrawalRequest(amount=5_000, reason="manual rebalance"),
            portfolio_id="us",
            db=temp_db,
            user={"username": "alice", "role": "admin"},
        )
    )

    assert result["status"] == "PENDING_MANUAL"
    assert result["execution_mode"] == "manual_only"
    assert result["cash_balance"] == pytest.approx(20_000.0)

    stored = temp_db.get_alpha_withdrawal_request(result["request_id"])
    assert stored is not None
    assert stored["amount"] == pytest.approx(5_000.0)
    assert stored["status"] == "PENDING_MANUAL"
    assert stored["requester"] == "alice"

    account = temp_db.get_alpha_account("us")
    assert account["cash_balance"] == pytest.approx(20_000.0)


def test_alpha_withdraw_status_update_only_records_manual_handling(temp_db):
    _seed_alpha_cash(temp_db, 12_000)
    temp_db.create_alpha_withdrawal_request(
        request_id="alpha123",
        amount=3_000,
        reason="offline transfer",
        requester="alice",
        portfolio_id="us",
    )

    result = asyncio.run(
        update_alpha_withdrawal_request_status(
            request_id="alpha123",
            req=AlphaLedgerWithdrawalStatusRequest(
                status="HANDLED",
                note="confirmed by bank",
                external_reference="bank-001",
            ),
            db=temp_db,
            user={"username": "bob", "role": "admin"},
        )
    )

    assert result["execution_mode"] == "manual_only"
    updated = result["request"]
    assert updated["status"] == "HANDLED"
    assert updated["handled_by"] == "bob"
    assert updated["external_reference"] == "bank-001"

    account = temp_db.get_alpha_account("us")
    assert account["cash_balance"] == pytest.approx(12_000.0)


def test_alpha_manual_inflow_entry_updates_balance_and_history(temp_db):
    decision_id = _save_safe_insurance_decision(temp_db)

    result = asyncio.run(
        create_alpha_ledger_entry(
            req=AlphaLedgerEntryRequest(
                direction="IN",
                amount=8_000,
                note="offline family transfer",
                external_reference="wire-in-001",
            ),
            portfolio_id="us",
            db=temp_db,
            user={"username": "alice", "role": "admin"},
        )
    )

    assert result["execution_mode"] == "manual_bookkeeping_only"
    assert result["cash_balance"] == pytest.approx(8_000.0)
    assert result["entry"]["direction"] == "IN"
    assert result["entry"]["external_reference"] == "wire-in-001"
    assert result["entry"]["insurance_decision_id"] == decision_id

    entries = temp_db.list_alpha_ledger_entries("us", limit=10)
    assert len(entries) == 1
    assert entries[0]["direction"] == "IN"
    assert entries[0]["balance_after"] == pytest.approx(8_000.0)
    assert entries[0]["insurance_decision_id"] == decision_id
    audit = temp_db.get_audit_log(limit=1)[0]
    assert f"InsuranceDecision={decision_id}" in audit["detail"]


def test_alpha_manual_outflow_entry_updates_balance_and_can_link_request(temp_db):
    _seed_alpha_cash(temp_db, 15_000)
    decision_id = _save_safe_insurance_decision(temp_db)
    temp_db.create_alpha_withdrawal_request(
        request_id="alpha-link-1",
        amount=4_000,
        reason="offline cash need",
        requester="alice",
        portfolio_id="us",
    )

    result = asyncio.run(
        create_alpha_ledger_entry(
            req=AlphaLedgerEntryRequest(
                direction="OUT",
                amount=4_000,
                note="bank transfer confirmed",
                external_reference="wire-out-001",
                related_request_id="alpha-link-1",
            ),
            portfolio_id="us",
            db=temp_db,
            user={"username": "bob", "role": "admin"},
        )
    )

    assert result["cash_balance"] == pytest.approx(11_000.0)
    assert result["entry"]["direction"] == "OUT"
    assert result["entry"]["related_request_id"] == "alpha-link-1"
    assert result["entry"]["insurance_decision_id"] == decision_id

    account = temp_db.get_alpha_account("us")
    assert account["total_outflows"] == pytest.approx(4_000.0)
    assert account["cash_balance"] == pytest.approx(11_000.0)
