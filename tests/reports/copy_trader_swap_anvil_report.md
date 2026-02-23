# E2E Strategy Test Report: copy_trader_swap (Anvil)

**Date:** 2026-02-20 14:26
**Result:** PASS
**Mode:** Anvil
**Duration:** ~24 seconds (strategy iteration)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | CopyTraderSwapStrategy |
| Strategy ID | copy_trader_swap_demo |
| Chain | arbitrum |
| Network | Anvil fork (block 434013834) |
| Strategy Dir | strategies/incubating/copy_trader_swap/ |

## Config Changes Made

The following changes were applied to `strategies/incubating/copy_trader_swap/config.json`
to meet the $50 budget cap and enable forced signal injection:

| Field | Before | After | Reason |
|-------|--------|-------|--------|
| `sizing.fixed_usd` | 100 | 25 | Reduce trade size under $50 cap |
| `risk.max_trade_usd` | 500 | 50 | Enforce $50 hard cap |
| `risk.max_daily_notional_usd` | 5000 | 250 | Proportional reduction |
| `sizing.percentage_of_leader` | 0.1 (float) | "0.1" (string) | V2 schema requires string for Decimal fields |
| `risk.max_slippage` | 0.01 (float) | "0.01" (string) | V2 schema requires string for Decimal fields |
| `filters` key | present | renamed to `global_policy` | V2 schema does not allow `filters` at top level |
| `copy_trading.execution_policy` | absent | added (`copy_mode: replay`, `replay_file: ...`) | Force an immediate trade via replay signal injection |
| `anvil_funding` | absent | `{ETH: 10, USDC: 1000}` | Fund wallet on Anvil fork automatically |

A synthetic replay signal file was created at:
`strategies/incubating/copy_trader_swap/test_signal.json`

This injects a single SWAP signal (USDC -> WETH, $25 notional, via uniswap_v3) from the
configured demo leader address. This is the framework's intended mechanism for forced testing
(`execution_policy.copy_mode = "replay"` with a `replay_file`).

## Execution

### Setup

- Managed gateway auto-started on port 50051 (network=anvil)
- Anvil fork started: arbitrum, port 54076, block 434013834 (chain_id=42161)
- Wallet funded: 10 ETH, 1000 USDC (via `anvil_funding` in config)

### Signal Injection

- 1 replay signal loaded from `test_signal.json`
- Signal: SWAP USDC -> WETH, protocol=uniswap_v3, leader=0x0000000000000000000000000000000000000001
- Signal notional: $25 (within $50 cap)
- Signal passed `CopySizer` checks (size=$25.00, daily cap OK, position cap OK)

### Strategy Run

- Strategy executed with `uv run almanak strat run -d strategies/incubating/copy_trader_swap --network anvil --once`
- `decide()` returned: `SwapIntent(from_token=USDC, to_token=WETH, amount_usd=25.00, max_slippage=0.01, protocol=uniswap_v3)`
- Intent compiled: 25.0000 USDC -> 0.0125 WETH (min: 0.0123 WETH)
- 2 transactions submitted and confirmed

### Key Log Output

```text
Copy replay loaded: 1 signal(s) from strategies/incubating/copy_trader_swap/test_signal.json
Copy trading initialized: monitoring 1 leader(s) on arbitrum

WalletMonitor polled blocks 434013784-434013834: 0 events
Copying swap: USDC -> WETH, size=$25, protocol=uniswap_v3, leader=0x00000000...
intent: SWAP: $25.00 USDC -> WETH (slippage: 1.00%) via uniswap_v3

Compiled SWAP: 25.0000 USDC -> 0.0125 WETH (min: 0.0123 WETH)
Slippage: 1.00% | Txs: 2 | Gas: 280,000

Transaction submitted: tx_hash=07b9b1df...892b, latency=6.1ms
Transaction submitted: tx_hash=fa7d3819...338a, latency=3.0ms
Transaction confirmed: tx_hash=07b9b1df...892b, block=434013836, gas_used=55449
Transaction confirmed: tx_hash=fa7d3819...338a, block=434013837, gas_used=142678

EXECUTED: SWAP completed successfully
  Txs: 2 (07b9b1...892b, fa7d38...338a) | 198,127 gas

Copy trade executed: USDC -> WETH, size=$25.00

Status: SUCCESS | Intent: SWAP | Gas used: 198127 | Duration: 23685ms
```

## Transactions

| # | Purpose | TX Hash | Block | Gas Used | Status |
|---|---------|---------|-------|----------|--------|
| 1 | USDC approve (Permit2) | `0x07b9b1df7417bb08a33ff1ad50882acfea5997e2ad5aae3ee560cb2f4bf6892b` | 434013836 | 55,449 | SUCCESS |
| 2 | Uniswap V3 swap | `0xfa7d3819164eb048fe0d7e776693975b90541ce811ee0508844741197dc9338a` | 434013837 | 142,678 | SUCCESS |

Total gas: 198,127

Note: TX hashes are from Anvil fork (local only, not on real Arbitrum mainnet).

## Issues Encountered

Three issues were found and fixed in `config.json` to enable the strategy to run correctly:

1. **Float values in Decimal fields**: `percentage_of_leader: 0.1` and `max_slippage: 0.01`
   were floats. The V2 schema (`AlmanakImmutableModel`) rejects floats for `SafeDecimal` fields
   to prevent precision loss. Fixed by converting to strings (`"0.1"`, `"0.01"`).

2. **`filters` key not allowed in V2**: The top-level `filters` key inside `copy_trading`
   is a legacy alias -- V2 schema requires `global_policy`. Without this fix, V2 validation
   fails, `ct_v2` is `None`, and the signal injection code path is never reached (signals
   are only injected when V2 config parses successfully). Fixed by renaming to `global_policy`.

3. **Signal injection requires V2 parse to succeed**: The CLI only injects replay signals
   when `CopyTradingConfigV2.from_config()` succeeds. When it fails (legacy-compatible mode),
   `ct_v2` is `None` and the injection block at lines 1704-1735 of `cli/run.py` is skipped.
   This caused the first run to produce HOLD ("No new leader activity") even though replay
   mode was configured.

## Result

**PASS** - CopyTraderSwapStrategy executed a USDC->WETH swap of $25.00 on Arbitrum (Anvil fork)
via Uniswap V3, producing 2 on-chain transactions (approve + swap), with total gas 198,127.
