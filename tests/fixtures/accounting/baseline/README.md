# Tier-1 Accounting Baseline DBs

Captured 2026-05-03 against PR #2014 commit `8b1a3ae2` (VIB-3914 wallet-anchored
Deployed). These DBs represent the state of LP + Looping accounting **before
any of the 16 Tier-1 ticket commits land**. Each commit on PR #2014 should
strictly reduce the failure count surfaced by
`scripts/ci/accounting_regression_check.sh`.

## Baseline ship-gate posture

### LP (`lp.db` — Uniswap V3 USDC/WETH 0.05% on Arbitrum, post-graceful teardown)

| Gate | Status | Notes |
|---|---|---|
| G1 NAV equation reconciles            | FAIL  | total=$4.19 cash=$45.81 deployed=$0 — VIB-3894 (deployed_capital_usd ordering). Snapshot writer commits before LP_OPEN finalises. |
| G2 No HIGH on degraded data           | PASS  | CONF (VIB-3886) holding the line. |
| G3 Gas tracking complete              | PASS  | Both successful tx have gas_usd. |
| G4 Outbox drains                      | PASS  | All processed. |
| G5 Teardown writes complete trail     | FAIL  | discover-lane finds 0 on-chain positions even though one was just opened. No teardown_requests row, no LP_CLOSE accounting_event, no position_events.CLOSE row. Touches VIB-3892 (discover-lane writes zero) + VIB-3919 (CLOSE columns) + VIB-3920 (positions_closed counter). |
| G6 Snapshot envelope honest           | FAIL  | 2 snapshots written as legacy bare-list — VIB-3923 (envelope-on-write). |
| G7 Audit posture rendered             | PASS  | Posture not cached in strategy_state. |

### Looping (`looping.db` — Aave V3 supply+borrow on Arbitrum)

| Gate | Status | Notes |
|---|---|---|
| G1 NAV equation reconciles            | FAIL  | total=$4.00 cash=$45.99 deployed=$0 — same VIB-3894 class. |
| G2 No HIGH on degraded data           | PASS  | 0 HIGH rows. |
| G3 Gas tracking complete              | PASS  | 1 successful tx, gas tracked. |
| G4 Outbox drains                      | PASS  | Processed. |
| G5 Teardown writes complete trail     | FAIL  | No teardown_requests row, no REPAY/WITHDRAW accounting_event. |
| G6 Snapshot envelope honest           | FAIL  | Single snapshot is legacy bare-list. |
| L4 Principal vs interest split        | XFAIL | VIB-3474 — connector pre/post-state pipeline blocked, out of scope this iteration. |

## How to use

1. Re-run the harness after a commit:
   ```bash
   scripts/ci/accounting_regression_check.sh
   ```
2. Compare the new DB at `strategies/accounting/{lp,looping}/almanak_state.db`
   against the baselines here. The new DB should pass at least every gate the
   baseline already passed plus the gate(s) the commit was supposed to fix.

## Re-capturing the baseline

The baseline is **frozen** for the duration of this iteration. Do not
re-capture without first commenting in PR #2014 — a baseline change resets
the regression-tracking signal for every prior commit.

If the baseline must be regenerated (e.g., the harness itself changes shape):

```bash
scripts/ci/accounting_regression_check.sh
cp strategies/accounting/lp/almanak_state.db        tests/fixtures/accounting/baseline/lp.db
cp strategies/accounting/looping/almanak_state.db   tests/fixtures/accounting/baseline/looping.db
```
