"""
出金治理测试
"""

import tempfile
from pathlib import Path

import pytest

from db.database import Database
from engine.governance import WithdrawalGovernance, SMALL_WITHDRAWAL_APPROVALS
from engine.insurance import InsuranceState, build_authority_decision


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    yield db
    db_path.unlink()


def _save_insurance_decision(db, **kwargs):
    with db.insurance_decision_writer("test"):
        return db.save_insurance_decision(**kwargs)


def test_small_withdrawal_stays_pending_until_manual_approval(temp_db):
    safe = build_authority_decision(InsuranceState.SAFE, reasons=["test safe"])
    _save_insurance_decision(
        temp_db,
        portfolio_id="us",
        previous_state="SAFE",
        decision=safe,
        risk_score=0.0,
        hard_blocks=[],
        source_signals=[],
    )
    gov = WithdrawalGovernance(temp_db)

    result = gov.request_withdrawal(
        amount=10_000,
        reason="family expense",
        requester="alice",
        portfolio_id="us",
    )

    assert result.status == "PENDING"
    assert result.extra["required_approvals"] == SMALL_WITHDRAWAL_APPROVALS

    stored = temp_db.get_withdrawal_request(result.withdrawal_id)
    assert stored["status"] == "PENDING"
    assert stored["required_approvals"] == SMALL_WITHDRAWAL_APPROVALS
    assert stored["cooling_days"] == 0

    approval = gov.approve_withdrawal(result.withdrawal_id, approver="bob")
    assert approval.status == "APPROVED"

    stored_after = temp_db.get_withdrawal_request(result.withdrawal_id)
    assert stored_after["status"] == "APPROVED"
