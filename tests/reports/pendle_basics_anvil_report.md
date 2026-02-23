# E2E Strategy Test Report: pendle_basics (Anvil)

**Date:** 2026-02-23 07:03
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pendle_basics |
| Chain | plasma |
| Network | Anvil fork (port 8554, chain ID 9745) |
| Anvil Port | 8554 |
| Market | PT-fUSDT0-26FEB2026 (`0x0cb289e9df2d0dcfe13732638c89655fb80c2be2`) |
| Trade Size | 1 FUSDT0 (~$1.00) |
| Budget Cap Check | OK — 1 FUSDT0 = ~$1.00, well under $100 cap |
| Config Changes | None required |

## Execution

### Setup
- [x] Anvil started on port 8554 (Plasma chain ID 9745 confirmed)
- [x] Gateway started (managed, auto-started on port 50052)
- [x] Wallet funded: 100 XPL (native), 10,000 FUSDT0 (storage slot 0 — brute-force found by managed gateway)
- [x] Note: FUSDT0 uses storage slot 0, not the standard slot 9

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Price: FUSDT0 priced at $1.00 via stablecoin fallback (not in CoinGecko)
- [x] Balance check passed: 10,000 FUSDT0 available vs 1 FUSDT0 required
- [x] Decision: SWAP intent generated — `1 FUSDT0 -> PT-fUSDT0 (slippage: 1.00%) via pendle`
- [x] 2 transactions submitted and confirmed
  - TX 1 (approve): `c040a24e9cade74a1584159072e351eb472c62b16e028827522a186b9686921b` — block 14862435, gas 46,216
  - TX 2 (swap): `e9ffedd4177b8c03ab00494ff7bd3df876a63d29a8fa29408b161c1185913974` — block 14862436, gas 314,361
- [x] Total gas used: 360,577
- [x] Final status: `SUCCESS | Intent: SWAP | Gas used: 360577 | Duration: 26084ms`

### Key Log Output
```text
PendleSDK initialized for chain=plasma, router=0x888888888889758F76e7103c6CbF23ABbF58F946
PendleAdapter initialized: chain=plasma
Compiling Pendle SWAP: FUSDT0 -> PT-fUSDT0, amount=1000000, market=0x0cb289e9...
Compiled Pendle SWAP intent: FUSDT0 -> PT-fUSDT0, 2 txs, 480000 gas
Token FUSDT0 not on CoinGecko, using stablecoin fallback ($1.00)
Entering Pendle position: Swapping 1 FUSDT0 for PT-fUSDT0
Transaction confirmed: tx_hash=c040a24e..., block=14862435, gas_used=46216
Transaction confirmed: tx_hash=e9ffedd4..., block=14862436, gas_used=314361
EXECUTED: SWAP completed successfully
   Txs: 2 (c040a2...921b, e9ffed...3974) | 360,577 gas
Status: SUCCESS | Intent: SWAP | Gas used: 360577 | Duration: 26084ms
Iteration completed successfully.
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | WARNING | CoinGecko free tier | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 3 | strategy | INFO | Stablecoin price fallback | `Token FUSDT0 not on CoinGecko, using stablecoin fallback ($1.00)` |
| 4 | strategy | WARNING | Gas limit override | `Gas estimate tx[0]: raw=46,216 buffered=50,837 (x1.1) < compiler=88,000, using compiler limit` |
| 5 | strategy | WARNING | Amount chaining gap | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 6 | strategy | WARNING | Port not freed | `Port 62905 not freed after 5.0s` |

### Finding Notes

**Finding 3 (Stablecoin fallback):** FUSDT0 is a niche Plasma chain stablecoin not indexed by CoinGecko. The gateway correctly applied the $1.00 stablecoin fallback. Expected and benign.

**Finding 4 (Gas limit override):** The Pendle adapter's compiler-specified gas limit (88,000) for the approve transaction is higher than the Anvil simulation estimate (50,837 buffered). The orchestrator used the compiler's limit. Normal Anvil fork behaviour — no action required.

**Finding 5 (Amount chaining gap):** The receipt parser did not extract an output amount from step 1 (the approve transaction). This means if a strategy ever uses `amount="all"` referencing step 1's output, it would silently fail. For this strategy it is benign since only the swap step follows. However, this indicates a gap in the Pendle receipt parser's `extract_swap_amounts()` method that could affect multi-step Pendle strategies relying on amount chaining.

**Finding 6 (Port cleanup):** The managed gateway's Anvil subprocess on port 62905 was not fully released within 5 seconds of shutdown. This is a cosmetic cleanup timing issue and does not affect strategy correctness.

## Result

**PASS** - The `pendle_basics` strategy on Plasma successfully compiled a Pendle SWAP intent (FUSDT0 -> PT-fUSDT0) and executed 2 on-chain transactions (approve + swap) using 360,577 gas total. No errors. One actionable finding: the Pendle receipt parser does not extract output amounts from the approve step, which could break amount chaining in multi-step Pendle strategies.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 6
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
