"""
出金治理测试
"""

import tempfile
from pathlib import Path

import pytest

from db.database import Database
from engine.governance import WithdrawalGovernance, SMALL_WITHDRAWAL_APPROVALS


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = Database(db_path)
    yield db
    db_path.unlink()


def test_small_withdrawal_stays_pending_until_manual_approval(temp_db):
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
