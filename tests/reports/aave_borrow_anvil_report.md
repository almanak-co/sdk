# E2E Strategy Test Report: aave_borrow (Anvil)

**Date:** 2026-02-23 03:33
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_aave_borrow |
| Chain | arbitrum |
| Network | Anvil fork (Arbitrum mainnet) |
| Anvil Port | 57609 (managed gateway auto-fork) |
| Collateral Token | WETH |
| Collateral Amount | 0.002 WETH (~$3.89 at $1,942 ETH) |
| Borrow Token | USDC |
| LTV Target | 50% |
| Min Health Factor | 2.0 |

**Budget check:** 0.002 WETH at ~$1,942/ETH = ~$3.89. Well within the $100 cap. No config amount changes needed.

**Config changes made:**
- Added `"force_action": "supply"` to trigger an immediate SUPPLY intent (restored after test)

---

## Execution

### Setup
- [x] Anvil fork started (managed gateway auto-starts its own fork on port 57609)
- [x] Gateway started on port 50052 (managed gateway, auto-started by `almanak strat run`)
- [x] Wallet auto-funded by managed gateway: 100 ETH, 1 WETH, 10,000 USDC

### Strategy Run

The strategy loaded persisted state from a previous run (`loop_state: supplied`, `supplied_amount: 0.002`). Because `force_action: "supply"` was set, it bypassed the state machine and immediately issued a SUPPLY intent.

- [x] Strategy executed with `--network anvil --once`
- [x] Intent executed: **SUPPLY 0.002 WETH to Aave V3**
- [x] 3 transactions submitted and confirmed on Anvil fork:

| Tx | Hash | Gas Used | Status |
|----|------|----------|--------|
| 1 (approve/setup) | `44a0857c9d083954fb562a6190630efef35f02e2b418565c2c9c4fe217591d9b` | 53,440 | SUCCESS |
| 2 (supply to Aave) | `2cf8a4fb0144310f6774d44ea35e97dc1139a397de333badf070d8b2b8743cdb` | 205,598 | SUCCESS |
| 3 (collateral flag) | `eb5dbfde421b7d160ee83c4f6d8e121924b9372180b02b9f745dd15d48b72e90` | 45,572 | SUCCESS |

**Total gas used:** 304,610

**Final status line:**
```text
Status: SUCCESS | Intent: SUPPLY | Gas used: 304610 | Duration: 27681ms
Iteration completed successfully.
```

### Key Log Output

```text
[info] Aggregated price for WETH/USD: 1942.62 (confidence: 1.00, sources: 1/1, outliers: 0)
[info] Aggregated price for USDC/USD: 0.999897 (confidence: 1.00, sources: 1/1, outliers: 0)
[info] Forced action: SUPPLY collateral
[info] SUPPLY intent: 0.0020 WETH to Aave V3
[info] Compiled SUPPLY: 0.0020 WETH to aave_v3 (as collateral) | Txs: 3 | Gas: 530,000
[info] Transaction confirmed: tx_hash=44a085..., block=434896070, gas_used=53440
[info] Transaction confirmed: tx_hash=2cf8a4..., block=434896071, gas_used=205598
[info] Transaction confirmed: tx_hash=eb5dbf..., block=434896072, gas_used=45572
[info] EXECUTED: SUPPLY completed successfully | Txs: 3 | 304,610 gas
[info] Parsed Aave V3: SUPPLY 2,000,000,000,000,000 to 0x82af...bab1
[info] Enriched SUPPLY result with: supply_amount, a_token_received (protocol=aave_v3, chain=arbitrum)
[info] Supply successful - state: supplied
```

---

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | No CoinGecko API key | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 req/min limit)` |
| 2 | strategy | WARNING | Gas estimate below compiler limit | `Gas estimate tx[0]: raw=53,788 buffered=80,682 (x1.5) < compiler=120,000, using compiler limit` |
| 3 | strategy | WARNING | Gas estimation failed for tx 3 | `Gas estimation failed for tx 3/3: ('0x5fe10377', '0x5fe10377'). Using compiler-provided gas limit.` |
| 4 | strategy | WARNING | Amount chaining extraction gap | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 5 | strategy | WARNING | Port not freed immediately | `Port 57609 not freed after 5.0s` (cosmetic, fork stopped successfully) |

**Analysis:**
- Finding 1 (CoinGecko free tier): Operational note. Prices were fetched successfully (WETH=$1,942.62, USDC=$0.9999). Not a problem for this run.
- Finding 2 (Gas estimate below compiler limit): The actual gas used (53,440) was lower than the buffered estimate (80,682) and the compiler limit (120,000). The compiler limit was used as a safety margin. Transaction confirmed. No functional impact.
- Finding 3 (Gas estimation failed for tx 3): The `setUserUseReserveAsCollateral` call (0x5fe10377) did not simulate cleanly, so the compiler-provided limit was used. The tx confirmed with 45,572 gas. Low risk -- fallback worked correctly.
- Finding 4 (Amount chaining): The SUPPLY receipt parser does not expose an output amount compatible with `amount='all'` chaining in multi-step IntentSequences. Not relevant for this single-intent run but worth tracking as a potential issue for composed workflows.
- Finding 5 (Port not freed): Cosmetic timing warning on Anvil shutdown. Fork was stopped successfully.

**No zero prices, no token resolution failures, no on-chain reverts, no API fetch failures.**

---

## Result

**PASS** - The aave_borrow strategy executed a SUPPLY intent successfully on an Arbitrum Anvil fork. Three transactions were confirmed on-chain with 304,610 total gas used. Prices were fetched live from CoinGecko (WETH=$1,942.62, USDC=$0.9999). Five warnings were detected; none indicate functional failures. The gas estimation fallback for tx 3 and the amount chaining gap (finding 4) are the most notable items to track.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 5
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
