# E2E Strategy Test Report: buy_the_dip (Anvil)

**Date:** 2026-02-20 07:15
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | buy_the_dip |
| Chain | arbitrum |
| Network | Anvil fork (managed, port 51769) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 (Anvil default) |
| Protocol | uniswap_v3 |
| Token Pair | WETH / USDC |
| Buy percentage | 20% of USDC balance |
| Sell percentage | 15% of WETH balance |

## Config Changes Made

The strategy does not support a `force_action` field. Two temporary changes were applied to
`config.json` to trigger an immediate trade on the first run and to fund the wallet:

| Field | Original | Test Value | Reason |
|-------|----------|------------|--------|
| `rsi_oversold` | 30 | 99 | Forces NEUTRAL→OVERSOLD signal change on first call (RSI was 42.49) |
| `anvil_funding` | (absent) | `{"ETH": 10, "USDC": 50}` | Funds managed Anvil fork wallet via config |

Both changes were **fully reverted** after the test. The config is restored to its original state.

### Budget Cap

No fixed trade size exists in config — trades are percentage-based (20% of USDC balance).
The managed Anvil fork was funded with exactly **50 USDC**, so the maximum trade was:
50 USDC × 20% = **10 USDC per buy**. This is well within the $50 budget cap.

## Execution

### Setup

- First run attempt: HOLD — the managed gateway auto-starts a fresh Anvil fork (unfunded wallet, $0 USDC)
- Root cause: `--network anvil` starts its own fork; pre-funded wallet on port 8545 was not used
- Fix: Added `"anvil_funding": {"ETH": 10, "USDC": 50}` to config.json so the managed gateway funds the wallet automatically
- Second run: managed gateway started fresh Anvil fork on port 51769, funded wallet successfully

### Strategy Run

- Wallet funded: 10 ETH + 50 USDC via managed gateway anvil_funding
- RSI fetched: 42.49 (from Binance OHLCV data, 34 candles)
- Signal change detected: NEUTRAL -> OVERSOLD (rsi_oversold threshold set to 99 for test)
- Decision: BUY — spending 20% of USDC (10.0000 USDC) on WETH
- Compiled: SWAP 10.000000 USDC -> 0.0050 WETH (min: 0.0049 WETH), slippage 1.00%
- Transactions: 2 (approve + swap via Uniswap V3)
- Both transactions confirmed on-chain (Anvil fork)

### Key Log Output

```text
Anvil fork started for arbitrum on port 51769
Funded 0xf39Fd6e5... with 10 ETH
Funded 0xf39Fd6e5... with USDC via known slot 9
RSI signal change: NEUTRAL -> OVERSOLD (RSI=42.49)
BUY: RSI=42.49 | Spending 20% of USDC (10.0000 USDC) on WETH
intent: SWAP: 10.000000 USDC -> WETH (slippage: 1.00%) via uniswap_v3
Compiled SWAP: 10.0000 USDC -> 0.0050 WETH (min: 0.0049 WETH)
Transaction submitted: tx_hash=b11b9ce0ccd25d3fc5eba2c37635c56200554b5a428190df39e5a78b60d58592
Transaction submitted: tx_hash=a03e2839967a1e409acbd7e94e0a6211fdcd003f0bf6518d22c7129f473e47f5
Transaction confirmed: tx_hash=b11b9ce0...8592, block=434011157, gas_used=55437
Transaction confirmed: tx_hash=a03e2839...47f5, block=434011158, gas_used=141672
EXECUTED: SWAP completed successfully
Txs: 2 (b11b9c...8592, a03e28...47f5) | 197,109 gas
Status: SUCCESS | Intent: SWAP | Gas used: 197109 | Duration: 21244ms
```

## Transactions (Anvil fork — not on mainnet)

| Intent | TX Hash | Gas Used | Status |
|--------|---------|----------|--------|
| APPROVE (USDC) | `b11b9ce0ccd25d3fc5eba2c37635c56200554b5a428190df39e5a78b60d58592` | 55,437 | SUCCESS |
| SWAP (USDC->WETH) | `a03e2839967a1e409acbd7e94e0a6211fdcd003f0bf6518d22c7129f473e47f5` | 141,672 | SUCCESS |

Total gas used: **197,109**

## Result

**PASS** — Strategy produced 2 on-chain transactions (approve + swap) on the Anvil fork,
swapping 10 USDC for ~0.0051 WETH via Uniswap V3 on Arbitrum. The RSI signal logic, balance
checks, trade sizing (percentage-based, within $50 budget), and execution pipeline all worked
correctly end-to-end.

## Notes

- The strategy does not support a `force_action` parameter; the RSI oversold threshold was
  temporarily raised to 99 to guarantee a buy signal on the first iteration.
- The managed gateway's `anvil_funding` config key is the correct way to fund the wallet for
  `--network anvil` runs (not pre-funding a separate Anvil process).
- First run attempt exposed a DX gap: without `anvil_funding`, the wallet has $0 balance and
  the strategy terminates immediately with "quote balance below dust threshold".
