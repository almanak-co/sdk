# E2E Strategy Test Report: whale_follower (Anvil)

**Date:** 2026-02-20 16:30
**Result:** PASS
**Mode:** Anvil
**Duration:** ~7 minutes (including setup)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | whale_follower |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 59610 (auto-assigned by managed gateway) |
| Strategy Path | strategies/incubating/whale_follower/ |

## Config Changes Made

Two fields were modified in `strategies/incubating/whale_follower/config.json` before the run:

| Field | Before | After | Reason |
|-------|--------|-------|--------|
| `copy_trading.risk.max_trade_usd` | 100 | 50 | Budget cap: max $50 per trade |
| `dry_run` | true | false | Enable real intent execution (not dry-run logging) |

Note: The strategy does NOT support a `force_action` field. Signal generation is entirely reactive,
driven by on-chain whale wallet activity detected by `WalletMonitor`.

## Execution

### Setup
- Anvil forked Arbitrum mainnet at block 434043747 (chain ID 42161)
- Managed gateway started on port 50052
- Wallet funded: 95 ETH, 10,000 USDC (slot 9), 5 WETH (via deposit())
- Wallet: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` (Anvil default)

### Strategy Initialization
- Leaders configured: wintermute (weight=1.0), jump_trading (weight=0.5)
- Sizing mode: proportion_of_leader (0.1% of leader trade)
- Max trade: $50 USD
- Copy trading initialized successfully (2 leaders on arbitrum)
- Note: config used legacy-compatible mode (strict schema validation warning)

### Strategy Run
- WalletMonitor polled blocks 434043645-434043745 (100 block lookback)
- Result: 0 leader events detected in that block range on the Anvil fork
- Strategy returned: `HOLD` -- reason: "No new leader activity"
- No transaction was submitted (expected: no whale signals available in the Anvil snapshot)

### Key Log Output
```text
WalletMonitor polled blocks 434043645-434043745: 0 events
whale_follower HOLD: No new leader activity
Status: HOLD | Intent: HOLD | Duration: 6640ms
Iteration completed successfully.
```

## On-chain Transaction

**No transaction was submitted.**

This is the correct behavior for this strategy on Anvil. The whale_follower strategy is
signal-driven: it only executes swaps when the WalletMonitor detects a qualifying swap
by Wintermute or Jump Trading in the recent block history. The Anvil fork captures a
static snapshot of the chain; the monitored leader wallets did not perform any qualifying
swaps (WETH/USDC/USDT/ARB/WBTC/GMX on uniswap_v3/pancakeswap_v3/sushiswap_v3) in the
100-block lookback window that was scanned.

**Why no `force_action` exists:** The strategy has no synthetic signal injection mechanism.
To produce a transaction on Anvil, the WalletMonitor would need real on-chain whale activity
(or a test fixture that injects mock signals into the WalletActivityProvider). This is a
known limitation of reactive copy-trading strategies in forked environments.

## Result

**PASS** -- Strategy loaded, initialized, executed one full iteration, and shut down cleanly
with `HOLD` (no signals found). This is the correct, expected outcome for this strategy type
on a static Anvil fork with no live whale activity.
