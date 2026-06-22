import numpy as np

from backtest.asymmetric_defense_audit import run_asymmetric_defense_audit


def test_asymmetric_defense_baseline_matches_fixed_policy():
    results = run_asymmetric_defense_audit()
    baseline = results["baseline_10_05_35_50"].audit.metrics

    assert np.isclose(baseline.final, 365897.47, atol=0.05)
    assert np.isclose(baseline.cagr, 0.0627, atol=0.0001)
    assert np.isclose(baseline.mdd, -0.1129, atol=0.0001)


def test_cutting_only_tlt_does_not_improve_cagr_or_mdd():
    results = run_asymmetric_defense_audit()
    baseline = results["baseline_10_05_35_50"].audit.metrics
    cut_tlt = results["cut_tlt_keep_spy_gld"].audit.metrics

    assert cut_tlt.cagr < baseline.cagr
    assert cut_tlt.mdd < baseline.mdd


def test_cutting_tlt_tilting_gld_still_fails_target():
    results = run_asymmetric_defense_audit()
    tilted = results["cut_tlt_tilt_gld"].audit.metrics

    assert tilted.cagr < 0.075
    assert tilted.mdd > -0.13
