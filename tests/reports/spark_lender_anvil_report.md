# E2E Strategy Test Report: spark_lender (Anvil)

**Date:** 2026-03-06 05:44 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_spark_lender |
| Chain | ethereum |
| Network | Anvil fork (ethereum-rpc.publicnode.com -- no ALCHEMY_API_KEY set) |
| Anvil Port | 63215 (assigned dynamically by managed gateway) |
| Fork Block | 24594328 |
| Supply Token | DAI |
| Min Supply Amount | 5 DAI |
| Force Action | supply (pre-configured) |

## Config Changes Made

None. `min_supply_amount: "5"` (5 DAI ~$5) is well within the $50 budget cap.
`force_action: "supply"` was already set. `anvil_funding.DAI: 10` handled by managed gateway.

## Execution

### Setup
- [x] Managed gateway auto-started on 127.0.0.1:50053 (network=anvil)
- [x] Anvil fork started on port 63215 (ethereum mainnet, block 24594328)
- [x] Wallet funded: 100 ETH, 10 DAI (slot 2), 1 WETH (slot 3) via managed gateway anvil_funding

### Strategy Run

`force_action: "supply"` triggered an immediate SUPPLY intent for 5 DAI.

**Execution sequence:**

1. First attempt: 2-transaction bundle compiled (approve + supply, 230,000 gas estimate)
   - TX 1 (approve DAI): `6fb00e7325cbb96af3e8c47e3b045df18c5c4984836d5a2ba73ec11f6641505e` -- CONFIRMED (block 24594331, gas 46,146)
   - TX 2 (supply): `0dca47b11df2bb96930a28376e44099a71f945f3bb1a56da83f9cdb426d05c25` -- REVERTED ("Invalid revert data (too short): 0x")
   - Framework triggered auto-retry (attempt 1/3)

2. Retry: 1-transaction bundle (supply only, approval already granted)
   - TX: `7677afda16d0d0450e66dba7832b3e3462d354848ce1ff9f4e149bf06200119f` -- CONFIRMED (block 24594333, gas 200,539)
   - SUPPLY completed successfully

**Net outcome:** 5 DAI supplied to Spark. Receipt parsed: 1 supply event, 0 withdraws, 0 borrows, 0 repays. Result enriched with `supply_amount` and `a_token_received` (spDAI).

### Key Log Output

```text
[info] Anvil fork started: chain=ethereum, port=63215, fork_block=latest
[info] Funded 0xf39Fd6e5... with 100 ETH
[info] Funded 0xf39Fd6e5... with DAI via known slot 2
[info] Funded 0xf39Fd6e5... with WETH via known slot 3
[info] Forced action: SUPPLY DAI
[info] SUPPLY intent: 5.0000 DAI -> Spark
[info] Compiled SUPPLY: 5.0000 DAI to Spark (as collateral) | Txs: 2 | Gas: 230,000
[info] Transaction 2/2: skipping estimation (multi-TX dependent), using compiler gas_limit=165000
[info] Sequential submit: TX 1/2 confirmed (block=24594331, gas=46146)
[warn] Transaction reverted: tx_hash=0dca47b1..., reason=Invalid revert data (too short): 0x
[error] FAILED: SUPPLY - Transaction reverted at 0dca47...5c25
[info] Retrying intent (attempt 1/3, delay=1.05s)
[info] Compiled SUPPLY: 5.0000 DAI to Spark (as collateral) | Txs: 1 | Gas: 150,000
[info] Simulation successful: 1 transaction(s), total gas: 205938
[info] Transaction confirmed: 7677afda..., block=24594333, gas_used=200,539
[info] EXECUTED: SUPPLY completed successfully
[info] Txs: 1 (7677af...119f) | 200,539 gas
[info] Parsed Spark receipt: supplies=1, withdraws=0, borrows=0, repays=0
[info] Enriched SUPPLY result with: supply_amount, a_token_received (protocol=spark, chain=ethereum)
[info] Supply successful: 5 DAI -> Spark
[info] Intent succeeded after 1 retries
Status: SUCCESS | Intent: SUPPLY | Gas used: 200539 | Duration: 37982ms
```

## Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| Approve DAI (attempt 1) | `6fb00e7325...1505e` | 24594331 | 46,146 | SUCCESS |
| Supply DAI (attempt 1) | `0dca47b11d...5c25` | - | - | REVERTED |
| Supply DAI (retry 1) | `7677afda16...119f` | 24594333 | 200,539 | SUCCESS |

(All transactions on local Anvil fork -- no block explorer links applicable)

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | strategy | ERROR | First supply TX reverted | `FAILED: SUPPLY - Transaction reverted at 0dca47...5c25; Reason: Invalid revert data (too short): 0x` |
| 3 | gateway | INFO | Public RPC, rate limits possible | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 4 | strategy | INFO | Circular import in incubating strategy | `Failed to import strategy pendle_pt_swap_arbitrum: cannot import name 'IntentStrategy' from partially initialized module` |

**Notes on findings:**

- **Finding #1 (Placeholder prices)**: Expected in Anvil mode without a price feed. The Spark SUPPLY intent does not use slippage calculations, so this has no practical impact. Informational only.
- **Finding #2 (TX revert on first attempt)**: The 2-TX bundle (approve + supply) caused the supply TX to revert. The log shows `Transaction 2/2: skipping estimation (multi-TX dependent), using compiler gas_limit=165000`. The actual gas needed was 200,539 -- exceeding the static cap by ~21%. The retry correctly re-compiles as a 1-TX bundle (approval already granted), runs full simulation (205,938 gas), and succeeds. This is a recurring Spark-on-Anvil pattern. The static gas cap for the second TX in multi-TX bundles (165,000) is too low for Spark `supply()`. Recommend raising to at least 220,000.
- **Finding #3 (Public RPC)**: Expected. No Alchemy API key configured in .env.
- **Finding #4 (Circular import)**: Pre-existing issue in `pendle_pt_swap_arbitrum` incubating strategy, unrelated to spark_lender.

## Result

**PASS** - The strategy successfully supplied 5 DAI to the Spark protocol on an Ethereum Anvil fork after one auto-retry. The first deposit attempt reverted because the static gas limit for the second TX in the multi-TX bundle (165,000) was insufficient for the actual Spark `supply()` execution (200,539 gas). The retry mechanism recovered correctly and the SUPPLY completed successfully (tx: 7677afda16d0d0450e66dba7832b3e3462d354848ce1ff9f4e149bf06200119f, gas: 200,539).

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
