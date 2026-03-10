# E2E Strategy Test Report: aave_borrow (Anvil)

**Date:** 2026-03-05 21:37 (kitchen-iter-52)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_aave_borrow |
| Chain | arbitrum |
| Network | Anvil fork (managed, public RPC via arbitrum-one-rpc.publicnode.com) |
| Anvil Port | Managed (auto-selected 63867) |
| Collateral | 0.002 WETH (~$4.17 at $2084/ETH) |
| Borrow Token | USDC |
| LTV Target | 50% |
| Min Health Factor | 2.0 |

## Config Changes Made

| Field | Before | After (test) | Restored |
|-------|--------|--------------|---------|
| force_action | (absent) | "supply" | removed (restored) |

`force_action` was added temporarily to trigger an immediate SUPPLY on the first `--once` run.
Collateral amount: 0.002 WETH (~$4.17 at $2084/ETH). Well under the $50 budget cap.

## Execution

### Setup
- Strategy runner auto-started managed gateway on 127.0.0.1:50053
- Managed Anvil fork started on auto-assigned port 63867, forked from Arbitrum mainnet block 438722868 via arbitrum-one-rpc.publicnode.com
- Wallet auto-funded by managed gateway from `anvil_funding` config: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)

Note: ALCHEMY_API_KEY is empty in .env. The SDK fell back to the public publicnode.com RPC endpoint. Execution was fully successful with the public fallback.

### Strategy Run
- `force_action = "supply"` triggered immediate SUPPLY intent
- Intent compiled to 3 transactions (WETH approve + Aave V3 supply + setUserUseReserveAsCollateral)
- All 3 transactions confirmed on Anvil fork

### Transactions

| TX # | Hash | Block | Gas Used | Status |
|------|------|-------|----------|--------|
| 1 (WETH approve) | `f6cf31636d2d3f94ddf9d26c96bc92e6ccca0ca8404181a26e50cb22a5eb60d8` | 438722871 | 53,440 | SUCCESS |
| 2 (Aave V3 supply) | `f577687f9a26ee98e05cb0605e5c60f439f01f88ddf7df5e8728888a9dabf4f0` | 438722872 | 205,598 | SUCCESS |
| 3 (set as collateral) | `ca6a64a45e40a446f8e5783a84b2a4009210b56ec12211434011f9356d86eb1f` | 438722873 | 45,572 | SUCCESS |

Total gas used: 304,610

### Key Log Output

```text
Aggregated price for WETH/USD: 2084.01 (confidence: 0.90, sources: 1/2, outliers: 0)
Aggregated price for USDC/USD: 1.00 (confidence: 0.90, sources: 1/2, outliers: 0)
Forced action: SUPPLY collateral
SUPPLY intent: 0.0020 WETH to Aave V3
Compiled SUPPLY: 0.0020 WETH to aave_v3 (as collateral) | Txs: 3 | Gas: 530,000
Simulating 3 transaction(s) via eth_estimateGas
Simulation successful: 3 transaction(s), total gas: 728788
Sequential submit: TX 1/3 confirmed (block=438722871, gas=53440)
Sequential submit: TX 2/3 confirmed (block=438722872, gas=205598)
Sequential submit: TX 3/3 confirmed (block=438722873, gas=45572)
EXECUTED: SUPPLY completed successfully
Parsed Aave V3: SUPPLY 2,000,000,000,000,000 to 0x82af...bab1, tx=0xf577...f4f0, 205,598 gas
Enriched SUPPLY result with: supply_amount, a_token_received, supply_rate (protocol=aave_v3, chain=arbitrum)
Supply successful - state: supplied

Status: SUCCESS | Intent: SUPPLY | Gas used: 304610 | Duration: 47089ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Circular import in incubating strategy | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy: cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |
| 2 | strategy | WARNING | CoinGecko rate limit on WETH price | `Rate limited by CoinGecko for WETH/USD, backoff: 1.00s` |
| 3 | strategy | WARNING | CoinGecko rate limit on USDC price | `Rate limited by CoinGecko for USDC/USD, backoff: 2.00s` |

**Finding #1 (WARNING)**: Pre-existing bug in `pendle_pt_swap_arbitrum` incubating strategy -- a circular import causes a hard import failure on every strategy discovery scan regardless of which strategy is being run. Non-blocking for other strategies but is a latent regression risk for pendle_pt_swap_arbitrum itself.

**Findings #2, #3 (WARNING)**: CoinGecko free tier rate-limiting is expected without an API key. The system recovered correctly via backoff and the price aggregator returned valid prices from the on-chain Chainlink oracle as primary source. No zero prices or failed price resolution.

No zero prices, no API fetch failures (after backoff), no reverts, no token resolution errors, no timeouts, no missing data.

## Result

**PASS** - The aave_borrow strategy successfully compiled and executed a 3-transaction SUPPLY sequence on an Anvil fork of Arbitrum, supplying 0.002 WETH to Aave V3 as collateral. Receipt parsing correctly identified the SUPPLY event, and result enrichment extracted supply_amount, a_token_received, and supply_rate. Strategy transitioned to "supplied" state cleanly.

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
