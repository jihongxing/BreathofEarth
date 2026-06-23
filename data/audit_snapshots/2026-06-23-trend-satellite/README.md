# 2026-06-23 Trend Satellite Audit Snapshot

This directory stores frozen Yahoo Adj Close inputs for the second CAGR uplift
satellite-level trend/CTA bypass audit.

Tickers:

- `DBMF`

Policy:

- These files are research inputs only.
- They do not change the production candidate.
- They do not authorize live execution or leverage.
- `DBMF` has a short history starting in 2019, so audit conclusions using it
  must remain downgraded until a long-history CTA proxy is available.
- `manifest.json` is the source-of-truth metadata for row counts, date ranges,
  and SHA256 hashes.

Use:

```bash
python -m backtest.cagr_uplift_audit
```

The audit compares `DBMF` only against the same-window `QQQ / SPY / GLD`
satellite baseline.
