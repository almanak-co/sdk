# E2E Strategy Test Report: copy_trader (Anvil)

**Date:** 2026-02-20 14:30
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | copy_trader (incubating) |
| Strategy ID | demo_copy_trader |
| Chain | arbitrum |
| Network | Anvil fork (Arbitrum mainnet fork) |
| Managed Anvil Port | 54833 (auto-assigned by managed gateway) |
| Gateway Port | 50052 (auto-assigned by managed gateway) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

The following fields in `strategies/incubating/copy_trader/config.json` were reduced to enforce the $50 USD budget cap:

| Field | Before | After | Reason |
|-------|--------|-------|--------|
| `copy_trading.leaders[0].max_notional_usd` | `"2500"` | `"50"` | Per-leader notional cap |
| `copy_trading.global_policy.max_usd_value` | `"2500"` | `"50"` | Policy filter upper bound |
| `copy_trading.risk.max_trade_usd` | `"200"` | `"50"` | Hard risk cap per trade |

Fields already within budget (no change needed):
- `sizing.fixed_usd`: `"50"` -- already at cap
- `sizing.mode`: `"fixed_usd"` -- uses fixed amount, not percentage

## force_action

The strategy has no `force_action` mechanism. It relies on `market.wallet_activity()` to detect
on-chain signals from the leader wallet (Wintermute: `0x489ee077994B6658eFaCA1507F1FBB620B9308aa`)
within the last `lookback_blocks: 50` blocks on the Anvil fork. No live trades were detected in
the lookback window, so the strategy correctly returned HOLD.

## Execution

### Setup
- [x] Anvil fork started (managed by `almanak strat run --network anvil`)
- [x] Gateway started on port 50052 (managed, insecure mode)
- [x] Wallet auto-funded: 100 ETH, 1 WETH, 10,000 USDC (via `anvil_funding` in config.json)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] `WalletMonitor` polled blocks 434014670-434014720: 0 events
- [x] Intent returned: HOLD (reason: "No new leader activity")
- [x] Runner status: `HOLD | Intent: HOLD | Duration: 3426ms`
- [x] Gateway and Anvil fork shut down cleanly

### Key Log Output

```text
WalletMonitor polled blocks 434014670-434014720: 0 events
demo_copy_trader HOLD: No new leader activity
Status: HOLD | Intent: HOLD | Duration: 3426ms
Iteration completed successfully.
```

## On-Chain Transactions

No on-chain transactions were produced. The strategy returned HOLD because the Anvil fork
(a snapshot of mainnet) had no leader wallet activity in the recent 50-block lookback window.
This is the correct and expected behaviour -- the copy trader only acts when it detects real
signals from the monitored leader address.

To trigger an actual trade on Anvil, the leader wallet's recent transactions would need to
be replayed via `execution_policy.replay_file` (replay mode), or the `lookback_blocks` window
would need to coincide with a block where Wintermute was active on-chain.

## Result

**PASS** -- Strategy loaded, initialized all copy-trading components (CopyPolicyEngine,
CopySizer, CopyIntentBuilder, CopyCircuitBreaker, CopyLedger), polled the leader wallet,
found no signals in the Anvil fork window, and returned HOLD cleanly. No errors or exceptions.
