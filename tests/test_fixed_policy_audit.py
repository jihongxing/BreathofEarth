import pandas as pd
import pytest

from backtest.fixed_policy_audit import (
    CapacityAuditResult,
    capacity_audit,
    validate_positive_prices,
)


class DummyRun:
    actions = pd.DataFrame(
        [{"date": pd.Timestamp("2025-01-02"), "action": "进入宏观慢熊防御"}]
    )


def test_capacity_audit_skips_when_ohlcv_missing():
    df = pd.DataFrame(
        {
            "SPY": [100.0],
            "TLT": [100.0],
            "GLD": [100.0],
            "SHV": [100.0],
        },
        index=[pd.Timestamp("2025-01-02")],
    )

    result = capacity_audit(df, DummyRun())

    assert isinstance(result, CapacityAuditResult)
    assert result.status == "SKIPPED"
    assert "adjusted-close" in result.reason


def test_validate_positive_prices_rejects_non_positive_values():
    prices = pd.DataFrame(
        {
            "SPY": [100.0, -1.0],
            "TLT": [100.0, 101.0],
        },
        index=pd.date_range("2025-01-01", periods=2),
    )

    with pytest.raises(ValueError, match="non-positive prices"):
        validate_positive_prices(prices)
