# AGENTS.md

## gstack Skills Configuration

Use `/browse` from gstack for all web browsing. Never use `mcp__claude-in-chrome__*` tools.

Available skills: `/office-hours`, `/plan-ceo-review`, `/plan-eng-review`, `/plan-design-review`, `/design-consultation`, `/design-shotgun`, `/design-html`, `/review`, `/ship`, `/land-and-deploy`, `/canary`, `/benchmark`, `/browse`, `/connect-chrome`, `/qa`, `/qa-only`, `/design-review`, `/setup-browser-cookies`, `/setup-deploy`, `/retro`, `/investigate`, `/document-release`, `/codex`, `/cso`, `/autoplan`, `/plan-devex-review`, `/devex-review`, `/careful`, `/freeze`, `/guard`, `/unfreeze`, `/gstack-upgrade`, `/learn`.

## Project Canon

Before changing strategy, broker execution, live observation, or frontend dashboard code, read:

- `docs/15-生产候选方案与后续开发指南.md`
- `README.md`
- `docs/09-实盘路线图.md`
- `docs/13-券商接入主备与沙箱实施方案.md`

Current project verdict:

- `Research PASS`
- `Production design APPROVED`
- `Live leveraged execution NOT YET APPROVED`

Do not reinterpret this as permission to trade, lever, or auto-enable broker execution.

## Strategy Boundary

The current production candidate is:

- 90% defensive core: `SPY / TLT / GLD / SHV`
- 10% satellite: default `QQQ / SPY / GLD`
- Aggregate weights: `SPY 25.5% / TLT 22.5% / GLD 25.5% / SHV 22.5% / QQQ 4.0%`

Any change to weights, tickers, MA windows, recovery rules, slippage assumptions, or satellite size must start as a bypass audit under `backtest/`, with tests, before touching production code.

Do not add `SMH`, raise the satellite above 10%, or loosen risk thresholds unless a new audit explicitly proves the change and updates `docs/15-生产候选方案与后续开发指南.md`.

## Data Boundary

All price data must fail closed:

- US ETF research uses clean Yahoo Adj Close.
- Do not use AkShare US `qfq` prices for long-horizon US ETF backtests.
- Non-positive prices, empty series, stale market dates, or mismatched assets must stop the run.
- Keep source and as-of metadata in live/shadow outputs.
- Treat `data/raw/*.csv` as regenerable local cache, not ordinary review input.
- Commit only approved frozen research inputs under `data/audit_snapshots/<date>-<source>/`.
- Every frozen snapshot must include a local `manifest.json`; `data/data_manifest.json` and `data/data_status.json` track regenerable raw cache metadata.

## Execution Boundary

Default posture: no real orders.

`live/` scripts are observation-only. They may read broker state, quotes, local data, and account snapshots. They must not call:

- `place_order`
- `cancel_order`
- trading session confirmation flows

`live.margin_monitor` must report missing broker/margin data as `UNAVAILABLE` or `PARTIAL`, never as safe.

Real execution requires all of the following before any live Core executor can even be created:

- global Core execution gate: `XIRANG_ENABLE_LIVE_CORE_EXECUTION=1`
- human approval reference: `XIRANG_LIVE_CORE_APPROVAL_ID`
- broker-level order submission gate, such as `IBKR_ENABLE_ORDER_SUBMISSION=1`
- whitelist gates, broker sync coverage, reconciliation, audit persistence, and post-execution reconciliation

Missing any gate must fail closed before broker order submission. Withdrawals are always human-governed.

## Frontend Boundary

The frontend is a static read-only observation panel:

- `frontend/index.html`
- `frontend/app.js`
- `frontend/i18n.js`
- `frontend/style.css`

Allowed frontend work:

- display NAV, drawdown, positions, risk events
- display Broker Sync, Shadow Run, Stage 9.5 shadow audit, and margin monitor state
- display warnings, missing data, and fail-closed states clearly
- submit governance requests such as withdrawal applications

Forbidden frontend work:

- one-click trade
- one-click leverage
- one-click Shadow to Live promotion
- hiding `UNAVAILABLE`, `WARNING`, or `FAIL_CLOSED`
- turning the app into a trading cockpit

When adding frontend features, expose a tested read-only FastAPI JSON payload first, then render it in `frontend/app.js` with bilingual copy in `frontend/i18n.js`.

## Testing Expectations

Before handing off changes, run the narrowest relevant tests plus any affected integration tests.

Common commands:

```bash
python -m pytest -q
python -m pytest tests/test_shadow_sync_live.py tests/test_margin_monitor_live.py -q
```

For frontend changes, run the local API server and verify the page in a browser. The UI must be readable on desktop and mobile widths, with no overlapping text or hidden risk states.

## Completion Protocol

Every completed task must end with a concrete next-step recommendation.

The final response should include:

- what changed
- what was verified
- any remaining risk or blocker
- the most reasonable next task to execute

The recommendation must be specific enough to start work immediately, for example:

- "Next: expose `latest_shadow_sync.json` and `latest_margin_snapshot.json` through a read-only FastAPI endpoint."
- "Next: add a Stage 9.5 read-only panel to `frontend/app.js` after the API is in place."
- "Next: run 60 trading days of shadow sync before discussing live leverage."

Do not end with a vague "let me know what you want next" when the project state clearly implies a good next step.

## Windows Directive Rule

When emitting Codex Desktop git directives on Windows, always use forward slashes in `cwd`, for example:

```text
cwd="D:/codeSpace/BreathofEarth"
```

Do not use backslashes in directive attributes.
