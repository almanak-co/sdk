# E2E Strategy Test Report: copy_trader (Anvil)

**Date:** 2026-03-16 01:58
**Result:** PASS (HOLD)
**Mode:** Anvil
**Duration:** ~5 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | copy_trader (incubating) |
| Location | strategies/incubating/copy_trader |
| Strategy ID | demo_copy_trader |
| Chain | arbitrum |
| Network | Anvil fork (managed gateway) |
| Managed Anvil Port | 60495 (auto-assigned) |
| Gateway Port | 50052 (managed gateway) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |

## Config Changes Made

No config changes were required. All fields were already within the $50 budget cap:

| Field | Value | Within Budget? |
|-------|-------|---------------|
| `risk.max_trade_usd` | "50" | Yes |
| `global_policy.max_usd_value` | "50" | Yes |
| `sizing.fixed_usd` | "50" | Yes |
| `leaders[0].max_notional_usd` | "50" | Yes |

`force_action` is not supported by this strategy. It relies on on-chain wallet activity signals
via `market.wallet_activity()`. No simulated signals exist in a fresh Anvil fork, so HOLD
is the expected outcome.

## Execution

### Setup
- Anvil fork started (managed, port 60495, forked Arbitrum mainnet via Alchemy)
- Gateway auto-started on port 50052 (managed gateway, insecure mode for Anvil)
- Wallet auto-funded: 100 ETH, 1 WETH, 10,000 USDC (via `anvil_funding` in config.json)

### Strategy Run

```
uv run almanak strat run -d strategies/incubating/copy_trader --network anvil --once
```

- Strategy initialized: CopyTraderStrategy, mode=live, submission_mode=auto, strict=False
- 1 leader monitored: `0x489ee077994B6658eFaCA1507F1FBB620B9308aa` (wintermute, arbitrum)
- WalletMonitor polled blocks 442140231-442140281 (50-block lookback window)
- Result: 0 copy signals detected in monitored range
- Intent returned: HOLD -- "No new leader activity"
- Exit code: 0

### Key Log Output

```text
CopyTraderStrategy initialized: mode=live, submission_mode=auto, strict=False
Copy trading initialized: monitoring 1 leader(s) on arbitrum
WalletMonitor polled blocks 442140231-442140281: 0 events
demo_copy_trader HOLD: No new leader activity
Status: HOLD | Intent: HOLD | Duration: 4688ms
Iteration completed successfully.
```

## On-Chain Transaction

None -- strategy returned HOLD. This is expected behavior when the leader wallet has no
activity in the monitored block range. In a live Anvil fork (a snapshot), the WalletMonitor
polls a historical window that may not contain leader activity.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | No CoinGecko API key, using Chainlink primary + free fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | gateway | WARNING | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |

**Notes:**
- **Finding 1**: Informational only. Chainlink on-chain pricing is the correct primary oracle for
  Anvil testing. No pricing was actually needed since the strategy returned HOLD.
- **Finding 2**: Expected warning in insecure/local mode. Correct behavior for Anvil testing.
  No auth token is the right configuration for local development.
- No zero prices, no API fetch failures, no token resolution errors, no reverts, no timeouts,
  no circular imports, no port collisions (clean test environment this run).

## Result

**PASS (HOLD)** -- The copy_trader strategy initialized cleanly on iter-81, auto-started a managed
gateway with Anvil fork, started a WalletMonitor for the Wintermute leader on Arbitrum, found no
copy signals in the 50-block lookback window (blocks 442140231-442140281), and correctly returned
HOLD in 4,688ms. All infrastructure components (WalletMonitor, CopyPolicyEngine, CopySizer,
CircuitBreaker, CopyLedger) initialized without errors. No on-chain transaction was submitted.
Log scan was clean with only two expected informational/warning entries.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
