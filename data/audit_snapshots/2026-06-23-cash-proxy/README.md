# 2026-06-23 Cash Proxy Audit Snapshot

This directory stores frozen Yahoo Adj Close inputs for the first CAGR uplift
cash-proxy bypass audit.

Tickers:

- `BIL`
- `SGOV`
- `USFR`
- `TFLO`

Policy:

- These files are research inputs only.
- They do not change the production candidate.
- They do not authorize live execution or leverage.
- `manifest.json` is the source-of-truth metadata for row counts, date ranges,
  and SHA256 hashes.

Use:

```bash
python -m backtest.cagr_uplift_audit
```

The audit compares each proxy only against the same-window SHV baseline because
the proxy ETFs do not all have a 2005-start history.
