# E2E Strategy Test Report: spark_lender (Anvil)

**Date:** 2026-03-16 01:40 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_spark_lender |
| Chain | ethereum |
| Network | Anvil fork (eth-mainnet via Alchemy) |
| Anvil Port | 57396 (assigned dynamically by managed gateway) |
| Fork Block | 24664741 |
| Supply Token | DAI |
| Min Supply Amount | 5 DAI |
| Force Action | supply (pre-configured) |

## Config Changes Made

None. `min_supply_amount: "5"` (5 DAI ~$5) is well within the $1000 budget cap.
`force_action: "supply"` was already set. `anvil_funding.DAI: 10` handled by managed gateway.

## Execution

### Setup
- [x] Managed gateway auto-started on 127.0.0.1:50052 (network=anvil)
- [x] Anvil fork started on port 57396 (ethereum mainnet, block 24664741)
- [x] Wallet funded: 100 ETH, 10 DAI (slot 2), 1 WETH (slot 3) via managed gateway anvil_funding

### Strategy Run

`force_action: "supply"` triggered an immediate SUPPLY intent for 5 DAI.

**Execution sequence:**

- 2-transaction bundle compiled (approve + supply, 330,000 gas estimate)
- TX 1 (approve DAI): `89f5ce7c0b98ac5b04b40c21e930749d76414966cec65f860ab65cb444a1efc4` -- CONFIRMED (block 24664744, gas 46,146)
- TX 2 (supply): `a7c0603b1d8a889d601c7ee572f63eb2a40ed2cb1f78eaae5cf8c8a884c104de` -- CONFIRMED (block 24664745, gas 200,539)
- No retries needed

**Net outcome:** 5 DAI supplied to Spark. Receipt parsed: 1 supply event, 0 withdraws, 0 borrows, 0 repays. Result enriched with `supply_amount` and `a_token_received` (spDAI).

### Key Log Output

```text
[info] Anvil fork started: chain=ethereum, port=57396, fork_block=latest, block=24664741
[info] Funded 0xf39Fd6e5... with 100 ETH
[info] Funded 0xf39Fd6e5... with DAI via known slot 2
[info] Funded 0xf39Fd6e5... with WETH via known slot 3
[info] Forced action: SUPPLY DAI
[info] SUPPLY intent: 5.0000 DAI -> Spark
[info] Compiled SUPPLY: 5.0000 DAI to Spark (as collateral) | Txs: 2 | Gas: 330,000
[info] Simulation successful: 2 transaction(s), total gas: 321146
[info] Sequential submit: TX 1/2 confirmed (block=24664744, gas=46146)
[info] Sequential submit: TX 2/2 confirmed (block=24664745, gas=200539)
[info] EXECUTED: SUPPLY completed successfully
[info] Txs: 2 (89f5ce...efc4, a7c060...04de) | 246,685 gas
[info] Parsed Spark receipt: supplies=1, withdraws=0, borrows=0, repays=0
[info] Enriched SUPPLY result with: supply_amount, a_token_received (protocol=spark, chain=ethereum)
[info] Supply successful: 5 DAI -> Spark
Status: SUCCESS | Intent: SUPPLY | Gas used: 246685 | Duration: 25860ms
```

## Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| Approve DAI | `89f5ce7c0b...efc4` | 24664744 | 46,146 | SUCCESS |
| Supply DAI | `a7c0603b1d...04de` | 24664745 | 200,539 | SUCCESS |

(All transactions on local Anvil fork -- no block explorer links applicable)

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| - | - | - | None detected | - |

All grep matches were benign initialization INFO lines:
- `timeout=300.0s` — receipt wait timeout parameter (normal)
- `rate_limit=30/min` — GeckoTerminal provider config (normal)
- `using...as fallback` — no CoinGecko API key, uses Chainlink as primary (normal)
- `cache_ttl=30s` / `Initialized Binance` — provider initialization (normal)

No zero prices, revert errors, token resolution failures, or warnings found.

## Result

**PASS** - The `spark_lender` strategy on Ethereum (Anvil fork, block 24664741) successfully compiled and executed a 2-transaction SUPPLY of 5 DAI into Spark. Both transactions confirmed without retries, the Spark receipt was correctly parsed (1 supply event), and ResultEnricher enriched the result with `supply_amount` and `a_token_received`.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 0
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
