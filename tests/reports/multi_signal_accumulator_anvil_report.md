# E2E Strategy Test Report: multi_signal_accumulator (Anvil)

**Date:** 2026-02-20 08:39
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes (including one failed attempt)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | multi_signal_accumulator |
| Class | MultiSignalAccumulatorStrategy |
| Chain | arbitrum |
| Network | Anvil fork (Arbitrum, block 434031299) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |
| trade_size_usd | $5.00 |
| max_accumulated_usd | $50.00 |
| force_action | buy |
| Pair | WETH / USDC |
| Protocol | enso |

## Config Changes Made

Two changes were made to `strategies/incubating/multi_signal_accumulator/config.json`:

1. `trade_size_usd` was already `"5"` (within $50 budget cap) — no change needed.
2. `max_accumulated_usd` was already `50` — no change needed.
3. **Added** `anvil_funding` block to fund the managed gateway's Anvil fork wallet:

```json
"anvil_funding": {
    "ETH": 100,
    "USDC": 10000
}
```

This was required because `uv run almanak strat run --network anvil` auto-starts its own managed
gateway with a fresh Anvil fork (not the manually-started one). Without `anvil_funding`, the
strategy wallet had no USDC to swap, causing all transactions to revert with
`ERC20: transfer amount exceeds balance`.

## Execution

### Setup
- Managed gateway auto-started on `127.0.0.1:50052`
- Anvil fork started on port `50572` (Arbitrum, block 434031299)
- Wallet funded by managed gateway: 100 ETH + 10,000 USDC

### Strategy Run

- `force_action: "buy"` triggered immediately (bypassed RSI + Bollinger Band checks)
- Strategy created a `SwapIntent`: USDC -> WETH, $5.00, 1% slippage, via Enso
- Enso route found: 5.0000 USDC -> 0.0026 WETH (price impact: 0bp)
- 2 transactions submitted and confirmed

### Transactions (Anvil — no block explorer)

| Step | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| USDC approve (Permit2) | `c94d7fe71602793a08c9fe0c6088d9b409cb53ae5727827abe2d7b15f443ff1b` | 55,437 | SUCCESS |
| Enso swap USDC→WETH | `216672ec8ce84b8a7ff501d392a79ff0a998c036a95abe719cffcb2b9589963f` | 1,079,024 | SUCCESS |
| **Total** | | **1,134,461** | |

### Key Log Output

```text
Anvil funding for 0xf39Fd6e5...: {'ETH': 100, 'USDC': 10000}
Funded 0xf39Fd6e5... with 100 ETH
Funded 0xf39Fd6e5... with USDC via known slot 9
Force action: buy
MultiSignalAccumulatorStrategy intent: SWAP: $5.00 USDC -> WETH (slippage: 1.00%) via enso
Route found: 0xaf88d065... -> 0x82aF4944..., amount_out=2553459493516407, price_impact=0bp
Compiled SWAP (Enso): 5.0000 USDC -> 0.0026 WETH (min: 0.0025 WETH)
Transaction confirmed: tx_hash=c94d7fe7..., block=434031301, gas_used=55437
Transaction confirmed: tx_hash=216672ec..., block=434031302, gas_used=1079024
EXECUTED: SWAP completed successfully
Status: SUCCESS | Intent: SWAP | Gas used: 1134461 | Duration: 33310ms
Iteration completed successfully.
```

## First Attempt (FAIL — for reference)

The first run failed because the config had no `anvil_funding`. The managed gateway's Anvil
fork had no USDC. The USDC `approve` TX succeeded but the Enso swap TX reverted on all 3
attempts with `Error: ERC20: transfer amount exceeds balance`. Adding `anvil_funding` fixed this.

## Minor Warning Observed

```text
Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail
```

This warning appears after a successful SWAP but indicates that result enrichment did not extract
the output token amount. This does not affect the current strategy (which uses fixed USD amounts,
not `amount='all'`), but may affect teardown if it issues an `amount='all'` sell intent.

## Result

**PASS** - Strategy executed a forced buy, swapping $5.00 USDC to ~0.0026 WETH via Enso on
Arbitrum Anvil fork. Two on-chain transactions confirmed (approve + swap), total gas 1,134,461.
