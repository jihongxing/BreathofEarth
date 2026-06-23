import pandas as pd

import backtest.benchmark_comparison_audit as audit
from backtest.benchmark_comparison_audit import (
    compare_to_benchmark,
    load_cached_benchmark,
    _parse_yahoo_chart_payload,
    save_benchmark,
)


def test_compare_to_benchmark_uses_overlapping_window():
    candidate_research = pd.Series(
        [100.0, 102.0, 104.0, 106.0],
        index=pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"]),
    )
    candidate_real = pd.Series(
        [100.0, 101.0, 102.0, 103.0],
        index=candidate_research.index,
    )
    benchmark = pd.Series(
        [50.0, 51.0],
        index=pd.to_datetime(["2020-01-06", "2020-01-07"]),
        name="TEST",
    )

    row = compare_to_benchmark(
        "TEST",
        "Temporary benchmark",
        benchmark,
        candidate_research,
        candidate_real,
    )

    assert row.start == "2020-01-06"
    assert row.end == "2020-01-07"
    assert row.candidate_research_cagr > row.candidate_real_world_cagr


def test_benchmark_cache_roundtrip(tmp_path):
    series = pd.Series(
        [100.0, 101.0],
        index=pd.to_datetime(["2020-01-02", "2020-01-03"]),
        name="CACHE",
    )

    path = save_benchmark(series, tmp_path)
    loaded = load_cached_benchmark("CACHE", tmp_path)

    assert path.name == "CACHE.csv"
    assert loaded.tolist() == [100.0, 101.0]


def test_parse_yahoo_chart_payload_uses_adjusted_close():
    payload = {
        "chart": {
            "result": [
                {
                    "timestamp": [1577923200, 1578009600],
                    "indicators": {
                        "adjclose": [
                            {
                                "adjclose": [10.0, 10.5],
                            }
                        ],
                    },
                }
            ],
            "error": None,
        }
    }

    series = _parse_yahoo_chart_payload("YHOO", payload)

    assert series.name == "YHOO"
    assert series.tolist() == [10.0, 10.5]


def test_run_benchmark_comparison_audit_without_download(monkeypatch):
    index = pd.to_datetime(["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"])
    research_nav = pd.Series([100.0, 101.0, 102.0, 103.0], index=index, name="NAV")

    class StubAggregated:
        nav = research_nav

    monkeypatch.setattr(audit, "production_candidate_nav", lambda: StubAggregated())
    monkeypatch.setattr(
        audit,
        "BENCHMARKS",
        {"AAA": "Alpha", "BBB": "Beta"},
    )
    monkeypatch.setattr(
        audit,
        "load_or_fetch_benchmark",
        lambda symbol, allow_download=True: pd.Series(
            [100.0, 100.5, 101.0, 101.5],
            index=index,
            name=symbol,
        ),
    )

    rows = audit.run_benchmark_comparison_audit(allow_download=False)

    assert [row.benchmark for row in rows] == ["AAA", "BBB"]
    assert all(row.candidate_research_cagr >= row.candidate_real_world_cagr for row in rows)
