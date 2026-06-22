import numpy as np
import pandas as pd
import pytest

from engine.portfolio_aggregator import aggregate_sleeves


def test_aggregate_sleeves_combines_normalized_navs():
    idx = pd.date_range("2025-01-01", periods=3)
    a = pd.Series([100.0, 110.0, 120.0], index=idx)
    b = pd.Series([200.0, 180.0, 220.0], index=idx)

    result = aggregate_sleeves(
        {"a": a, "b": b},
        {"a": 0.25, "b": 0.75},
        initial_capital=1000.0,
    )

    expected_final = 1000.0 * (0.25 * 1.2 + 0.75 * 1.1)
    assert np.isclose(result.final, expected_final)
    assert list(result.sleeve_navs.columns) == ["a", "b"]


def test_aggregate_sleeves_rejects_bad_weights():
    idx = pd.date_range("2025-01-01", periods=2)
    nav = pd.Series([100.0, 101.0], index=idx)

    with pytest.raises(ValueError, match="sum to 1.0"):
        aggregate_sleeves({"a": nav}, {"a": 0.5})
