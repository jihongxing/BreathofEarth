import pandas as pd

from backtest.mdd_zone_locator import locate_drawdown_episodes


def test_locate_drawdown_episodes_orders_by_depth():
    nav = pd.Series(
        [100, 110, 100, 111, 90, 112, 108, 113],
        index=pd.date_range("2025-01-01", periods=8),
    )

    episodes = locate_drawdown_episodes(nav)

    assert len(episodes) == 3
    assert episodes[0].peak_nav == 111
    assert episodes[0].valley_nav == 90
    assert episodes[0].mdd == 90 / 111 - 1
    assert episodes[0].recovery_date == pd.Timestamp("2025-01-06")
