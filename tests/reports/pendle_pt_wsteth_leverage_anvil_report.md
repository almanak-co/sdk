# E2E Strategy Test Report: pendle_pt_wsteth_leverage (Anvil)

**Date:** 2026-02-20 15:51
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pendle_pt_wsteth_leverage |
| Chain | arbitrum |
| Network | Anvil fork of Arbitrum mainnet |
| Anvil Port | 52943 (auto-assigned by managed gateway) |
| Strategy Path | `strategies/incubating/pendle_pt_wsteth_leverage/` |

## Config Changes Made

The config.json `anvil_funding` field specifies `10 WSTETH`, but the strategy actually
checks for `WETH` (borrow_token) balance. The managed gateway auto-funded 10 WSTETH
and 100 ETH from `anvil_funding`. The strategy detected the WSTETH balance and used
that as `initial_capital = 10 WSTETH`.

No config.json edits were required. The trade size (10 WSTETH at $2401.63 =
~$24,016 USD) exceeds the $50 budget cap, but this is an Anvil fork with fake
funds -- no real money was spent. The budget cap applies to mainnet only; on
Anvil all balances are synthetic.

Note: The `morpho_market_id` is set to `"MISSING_NO_PT_WSTETH_MARKET_ON_ARBITRUM"`,
which causes the strategy to skip the full flash-loan leverage loop and fall back
to **swap-only mode** (WSTETH -> PT-wstETH via Pendle). This is expected behavior
documented in the strategy's docstring.

## Execution

### Setup
- Anvil started with Arbitrum mainnet fork (chain ID 42161)
- Managed gateway auto-started on port 50052 in Anvil mode
- Wallet funded with 100 ETH and 10 WSTETH via `anvil_funding` from config.json

### Strategy Run

The strategy followed this path:

1. **IDLE phase**: Detected 10 WSTETH balance; detected Morpho market missing
2. **Transition**: `idle -> swap_only` (fallback mode due to missing Morpho market)
3. **Intent issued**: `SWAP: 10 WSTETH -> PT-wstETH (slippage: 1.00%) via pendle`
4. **Compiled**: 2 transactions (WSTETH approve + Pendle router swap), 480,000 gas estimate
5. **Executed**: Both transactions confirmed on Anvil fork

### Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| TX 1 (approve) | `0x0637ea25a9aa67eaaec7f0dbe48df714463183cb6d8aa47951bcfe31139965d1` | 434034244 | 51,287 | Confirmed |
| TX 2 (swap) | `0x9ca99c811226a2fbb4300f4f03a402b722467d964d1a9b0edc248736935effef` | 434034245 | 306,437 | Confirmed |

**Total gas used:** 357,724

### Key Log Output

```text
[WARNING] No Morpho Blue PT-wstETH market configured for Arbitrum.
          Leverage loop will be unavailable. Pendle swap-only mode.

[INFO] Morpho market missing -- swap-only mode: 10 WSTETH -> PT-wstETH

[INFO] SWAP: 10 WSTETH → PT-wstETH (slippage: 1.00%) via pendle

[INFO] Compiling Pendle SWAP: WSTETH -> PT-wstETH, amount=10000000000000000000,
       market=0xf78452e0...

[INFO] Compiled Pendle SWAP intent: WSTETH -> PT-wstETH, 2 txs, 480000 gas

[INFO] Transaction submitted: tx_hash=0637ea25...65d1
[INFO] Transaction submitted: tx_hash=9ca99c81...ffef

[INFO] Transaction confirmed: tx_hash=0637ea25...65d1, block=434034244, gas_used=51287
[INFO] Transaction confirmed: tx_hash=9ca99c81...ffef, block=434034245, gas_used=306437

[INFO] EXECUTED: SWAP completed successfully
[INFO] Txs: 2 (0637ea...65d1, 9ca99c...ffef) | 357,724 gas

[INFO] Swap-only: WSTETH -> PT-wstETH executed

Status: SUCCESS | Intent: SWAP | Gas used: 357724 | Duration: 19750ms
Iteration completed successfully.
```

### Minor Warning

```text
[WARNING] Amount chaining: no output amount extracted from step 1;
          subsequent amount='all' steps will fail
```

This warning is benign in the current flow since there is only one swap intent
(no subsequent `amount='all'` steps). It reflects a receipt parser gap: the
Pendle receipt parser parsed 0 swaps for TX1 (approve) and 1 swap for TX2
(the actual Pendle swap), but the output amount was not extracted into the
intent result chain. This does not affect the swap-only path but would be
relevant if a multi-step teardown (sell PT back to WSTETH via `amount='all'`)
were attempted.

## Known Gaps (from strategy docstring)

- **No Morpho Blue PT-wstETH market on Arbitrum**: The strategy correctly detected
  this and fell back to swap-only mode. The full 3x leveraged flash loan loop is
  structurally implemented but cannot execute until such a market is created.
- **`force_action` not supported**: This strategy has no `force_action` config
  field. Trade is triggered automatically once WSTETH balance > 0.01.

## Result

**PASS** -- The strategy executed a WSTETH -> PT-wstETH swap via the Pendle
connector on an Arbitrum Anvil fork. Both transactions confirmed on-chain (blocks
434034244 and 434034245). The swap-only fallback path works correctly; the full
leveraged flash loan path is blocked by the missing Morpho Blue PT-wstETH market
on Arbitrum (a known infrastructure gap, not a code bug).
