import numpy as np

from backtest.stepped_recovery_audit import run_stepped_recovery_audit


def test_stepped_recovery_baseline_matches_fixed_policy():
    results = run_stepped_recovery_audit()
    baseline = results["baseline_instant"].audit.metrics

    assert np.isclose(baseline.final, 365897.47, atol=0.05)
    assert np.isclose(baseline.cagr, 0.0627, atol=0.0001)
    assert np.isclose(baseline.mdd, -0.1129, atol=0.0001)


def test_stepped_20d_reduces_cost_but_does_not_clear_cagr_target():
    results = run_stepped_recovery_audit()
    baseline = results["baseline_instant"].audit.metrics
    stepped = results["stepped_20d"].audit.metrics

    assert stepped.total_cost < baseline.total_cost
    assert stepped.cagr < 0.075
    assert stepped.mdd >= -0.13


def test_stepped_45d_is_too_slow_for_return_recovery():
    results = run_stepped_recovery_audit()
    baseline = results["baseline_instant"].audit.metrics
    stepped = results["stepped_45d"].audit.metrics

    assert stepped.total_cost < baseline.total_cost
    assert stepped.cagr < baseline.cagr
    assert stepped.mdd >= -0.13
