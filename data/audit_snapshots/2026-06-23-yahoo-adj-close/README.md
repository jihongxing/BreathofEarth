# 2026-06-23 Yahoo Adj Close Audit Snapshot

This directory stores the frozen Yahoo Adj Close inputs used by the Stage 9 research audits.

Purpose:

- reproduce bond substitution and portfolio aggregation audits in clean checkouts
- keep long-horizon research inputs separate from regenerable `data/raw/*.csv` cache files
- avoid daily data refresh noise in normal code review diffs

Rules:

- Do not update these files as part of routine data refreshes.
- If research inputs change, create a new dated snapshot directory.
- Keep `manifest.json` in this directory as the frozen snapshot manifest.
- Keep `data/data_manifest.json` and `data/data_status.json` as the machine-readable local cache manifest with rows, date range, and SHA256 metadata.
