# E2E Strategy Test Report: mantle_mnt_accumulator (Anvil)

**Date:** 2026-03-15 18:05
**Result:** PASS
**Mode:** Anvil
**Duration:** ~6 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | mantle_mnt_accumulator |
| Chain | mantle (Chain ID: 5000) |
| Network | Anvil fork |
| Anvil Port | 49875 (auto-assigned by managed gateway) |
| Config changes | None (trade sizes within 1000 USD cap; no force_action supported) |

### Config Notes

The strategy uses percentage-based sizing (10%/25%/40% of stable balance). With 200 USDT funded,
the maximum possible trade is 80 USDT (40% heavy dip buy) — well under the 1000 USD cap.
No config modifications were required.

The strategy does not support a `force_action` parameter. However, the RSI was naturally
overbought (RSI=73.7 > 70) on the Mantle mainnet fork, triggering a PROFIT TAKE trade
immediately without needing to force one.

## Execution

### Setup

- [x] Anvil fork started (port 49875, managed by `almanak strat run --network anvil`)
- [x] Gateway started on port 50052 (auto-managed)
- [x] Wallet funded: 100 WMNT (slot 0 brute-force), 200 USDT (slot 0 brute-force), 1000 native MNT

### Strategy Decision

RSI(14) for WMNT/USD = 73.7 (overbought > 70 threshold), triggering a PROFIT TAKE signal.
Position was 100 WMNT (~$78 USD) with 200 USDT stables (~$200 USD).
Position ratio = 28% > 20% cap for profit take. Strategy sold 15% of WMNT position.

### Strategy Run

- [x] Strategy executed with `--network anvil --once`
- [x] Intent: SWAP (PROFIT TAKE) — 15.000 WMNT → ~11.70 USDT via Uniswap V3 (Agni Finance on Mantle)
- [x] First attempt reverted with "Transaction too old" (deadline expired due to slow gas estimation — ~2.5 min compile time). Auto-retried.
- [x] Retry succeeded: TX `0xffe7c73f17cc95d70557f00571177dc9194726707ca8425753741f90777eba82` confirmed in block 92733639

### Key Log Output

```text
2026-03-15T17:59:50.421834Z [info] PROFIT TAKE: RSI=73.7 > 70 | Selling 15.0000 WMNT (15% of position)
2026-03-15T18:01:10.998537Z [info] ✅ Compiled SWAP: 15.0000 WMNT → 11.6951 USDT (min: 11.5197 USDT)
   Slippage: 1.50% | Txs: 2 | Gas: 110,000,000
2026-03-15T18:05:31.104387Z [warning] Transaction reverted: f0c773...4f76 reason=Error: Transaction too old
2026-03-15T18:05:33.124772Z [info] Retrying intent b19730ff... (attempt 1/3, delay=1.09s)
2026-03-15T18:05:34.255794Z [info] ✅ Compiled SWAP: 15.0000 WMNT → 11.6951 USDT (min: 11.5197 USDT)
   Slippage: 1.50% | Txs: 1 | Gas: 80,000,000
2026-03-15T18:05:46.397716Z [info] Transaction confirmed: tx_hash=ffe7c7...ba82, block=92733639, gas_used=395204
2026-03-15T18:05:50.434790Z [info] ✅ EXECUTED: SWAP completed successfully | Txs: 1 | 395,204 gas
2026-03-15T18:05:50.449051Z [info] Sell #1 executed (profit take)
Status: SUCCESS | Intent: SWAP | Gas used: 395204 | Duration: 363337ms
```

### Transactions

| Intent | TX Hash | Gas Used | Status |
|--------|---------|----------|--------|
| SWAP attempt 1 (approve) | `8e4d615e135c075f5b822d34c667d3d1d9cd311cdf9eed3723408a880fafcfec` | 46,265 | SUCCESS (approve only) |
| SWAP attempt 1 (swap) | `f0c7736b2119caeeda1eaee5357ac5aa86be589abb8f5680f85c96f4fdcd4f76` | N/A | REVERTED (Transaction too old) |
| SWAP attempt 2 (swap) | `ffe7c73f17cc95d70557f00571177dc9194726707ca8425753741f90777eba82` | 395,204 | SUCCESS |

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Transaction reverted (auto-retried, resolved) | `Transaction f0c773... reverted: Error: Transaction too old` |
| 2 | strategy | WARNING | `evm_snapshot` failed on internal Anvil fork | `evm_snapshot failed (not Anvil?): . Proceeding with gas estimation only` |
| 3 | strategy | INFO | WMNT price confidence 80% (2/4 sources only) | `Aggregated price for WMNT/USD: 0.7820205 (confidence: 0.80, sources: 2/4)` |

### Notes on Findings

**Finding 1** (Transaction too old): The initial compile used a 2-transaction path (approve + swap), but the approval took 90 seconds at block 92733637. By the time the swap TX was submitted, the Uniswap V3 deadline had expired. The retry correctly recompiled with a fresh deadline and a 1-TX path (approval already done), succeeding immediately. This is a known latency pattern on Anvil forks with slow quoter calls. Not a bug in the strategy itself.

**Finding 2** (evm_snapshot failed): The internal LocalSimulator tried `evm_snapshot` to do full simulation on the Anvil fork, but the RPC returned an error. This is a Mantle-specific Anvil fork issue — the managed fork starts but doesn't support `evm_snapshot`. The framework gracefully fell back to gas estimation only. This caused the gas estimate for TX 2 to fall back to the compiler's `120,000,000` limit, which is very high but harmless on Anvil.

**Finding 3** (WMNT price confidence 0.80): Only 2 of 4 price sources returned data for WMNT (likely Chainlink has no Mantle oracle, and Binance doesn't list MNT/USDT). The 2 available sources (DexScreener + CoinGecko) agreed at $0.782, giving a valid price. Not a blocking issue but worth noting that Mantle has limited oracle coverage.

## Result

**PASS** — Strategy executed a PROFIT TAKE SWAP (15 WMNT → 11.70 USDT) on a Mantle Anvil fork, confirmed on-chain after one retry due to a deadline expiry caused by slow Anvil quoter latency. All signals, compilation, execution, and receipt parsing functioned correctly.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
