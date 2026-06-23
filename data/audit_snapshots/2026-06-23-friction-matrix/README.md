# 2026-06-23 Friction Matrix Audit Snapshot

This directory stores the annual assumption matrix used by the calibrated
friction bypass audit.

Important boundary:

- This is not an IBKR account statement.
- This is not tax advice.
- This is not a live broker ledger.
- It is a research matrix used to test whether the older flat annual drag model
  over-penalizes the 90/10 candidate.

Use:

```bash
python -m backtest.cagr_uplift_audit
```

The calibrated friction report applies:

- SPY/QQQ dividend withholding drag as an exposure-weighted daily cost.
- Broker spread drag only to SHV/cash exposure.
- Operational failure drag as a small annualized daily cost.
- Event drag only on state-machine action dates.

Replace this matrix with actual broker interest records, dividend withholding
records, tax lots, and execution logs before using it for live decisions.
