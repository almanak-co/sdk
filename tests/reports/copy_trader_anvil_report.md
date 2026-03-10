# E2E Strategy Test Report: copy_trader (Anvil)

**Date:** 2026-03-06 06:13
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | copy_trader (incubating) |
| Location | strategies/incubating/copy_trader |
| Strategy ID | demo_copy_trader |
| Chain | arbitrum |
| Network | Anvil fork (managed gateway) |
| Managed Anvil Port | 64974 (auto-assigned) |
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
- Anvil fork started (managed, port 53605, forked Arbitrum mainnet via public RPC: arb1.arbitrum.io/rpc)
- Gateway started on port 50053 (managed gateway, insecure mode for Anvil)
- Wallet auto-funded: 100 ETH, 1 WETH, 10,000 USDC (via `anvil_funding` in config.json)

### Strategy Run

```
uv run almanak strat run -d strategies/incubating/copy_trader --network anvil --once
```

- Strategy initialized: CopyTraderStrategy, mode=live, submission_mode=auto, strict=False
- 1 leader monitored: `0x489ee077994B6658eFaCA1507F1FBB620B9308aa` (wintermute, arbitrum)
- WalletMonitor polled blocks 438746232-438746282 (50-block lookback window)
- Result: 0 copy signals detected in monitored range
- Intent returned: HOLD -- "No new leader activity"
- Exit code: 0

### Key Log Output

```text
CopyTraderStrategy initialized: mode=live, submission_mode=auto, strict=False
Copy trading initialized: monitoring 1 leader(s) on arbitrum
WalletMonitor polled blocks 438746232-438746282: 0 events
demo_copy_trader HOLD: No new leader activity
Status: HOLD | Intent: HOLD | Duration: 14218ms
Iteration completed successfully.
```

## On-Chain Transaction

None -- strategy returned HOLD. This is expected behavior when the leader wallet has no
activity in the monitored block range. In a live Anvil fork (a snapshot), the WalletMonitor
polls a historical window that may not contain leader activity.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | Circular import in pendle_pt_swap_arbitrum strategy | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy: cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |
| 2 | gateway | ERROR | Metrics HTTP port 9090 already in use | `OSError: [Errno 48] Address already in use` (metrics server, pre-existing gateway held port) |
| 3 | gateway | INFO | No CoinGecko API key, using Chainlink + free fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 4 | strategy | INFO | IntentCompiler using placeholder prices | `IntentCompiler initialized for chain=arbitrum ... using_placeholders=True` |
| 5 | strategy | INFO | No Alchemy API key, using free public RPC | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |

**Notes:**
- **Finding 1**: A circular import error surfaced in `pendle_pt_swap_arbitrum.strategy` during
  strategy discovery. This is unrelated to copy_trader but is a real bug that should be
  ticketed separately. Severity is ERROR because it silently prevents that strategy from loading.
- **Finding 2**: Port 9090 collision occurred because a pre-existing standalone gateway from the
  test setup was still holding the metrics port. The strategy's auto-managed gateway started
  cleanly on port 50053 instead. Non-blocking to this test, but indicates test isolation
  needs improvement when running manual gateway alongside auto-managed gateway.
- **Findings 3-5**: Informational only. Expected for Anvil testing without API keys configured.
  No pricing was actually needed since the strategy returned HOLD without any intent compilation.

## Result

**PASS** -- The copy_trader strategy initialized cleanly, started a WalletMonitor for the
Wintermute leader on Arbitrum, found no copy signals in the 50-block lookback window (blocks
438746232-438746282), and correctly returned HOLD in 14,218ms. No on-chain transaction was
submitted. Two ERROR-class findings were detected: a pre-existing circular import bug in
`pendle_pt_swap_arbitrum` (unrelated to copy_trader) and a metrics port collision from the test
setup. Neither blocked the copy_trader run.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 2
