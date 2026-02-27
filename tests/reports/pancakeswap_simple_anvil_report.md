# E2E Strategy Test Report: pancakeswap_simple (Anvil)

**Date:** 2026-02-27 16:18
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pancakeswap_simple |
| Chain | arbitrum |
| Network | Anvil fork (managed, public RPC fallback) |
| Anvil Port | 53042 (auto-assigned by managed gateway) |
| swap_amount_usd | $10 |
| from_token | WETH |
| to_token | USDC |
| max_slippage | 1.00% |

**Config changes made:** None. `swap_amount_usd` was already $10, well within the $500 budget cap. The strategy unconditionally executes a swap on every call (no `force_action` field needed).

## Execution

### Setup
- [x] Anvil fork auto-started by managed gateway (port 53042, chain_id=42161, block 436466229, public RPC)
- [x] Gateway started on port 50052 (insecure mode, anvil network, managed/embedded)
- [x] Wallet funded by managed gateway: 100 ETH, 10 WETH (storage slot 51), 10,000 USDC (storage slot 9)
- [x] Pricing: on-chain Chainlink (primary) + free CoinGecko (fallback), no API key required

### Strategy Run
- [x] Strategy executed with `uv run almanak strat run -d strategies/demo/pancakeswap_simple --network anvil --once`
- [x] Prices fetched: WETH=$2,035.38, USDC=$0.999958 (aggregated, confidence 1.00, sources 2/2)
- [x] Balance confirmed: 10 WETH ($20,353.83)
- [x] SWAP intent returned: $10.00 WETH -> USDC via pancakeswap_v3 (1.00% slippage)
- [x] Intent compiled: 0.0049 WETH -> 9.9704 USDC (min: 9.8707 USDC), 2 TXs, gas estimate 280,000
- [x] Simulation: successful via eth_estimateGas (total gas 353,800)
- [x] Receipt parsed: PancakeSwap V3 swap detected (swaps=1)
- [x] Result enriched with swap_amounts

### Transactions

| TX # | Hash | Block | Gas Used | Status |
|------|------|-------|----------|--------|
| 1 (Approve) | `c8b36cfc28bb6d9aa189a6b88b9ee56341830dd77063ad91b55fca32eee5b5d0` | 436466232 | 53,452 | SUCCESS |
| 2 (Swap) | `aadb40468a10e132c86141d2317583f125e995993cc7afe30e39d6179266c5af` | 436466233 | 173,264 | SUCCESS |

**Total gas used:** 226,716

### Key Log Output

```text
Aggregated price for WETH/USD: 2035.3829195 (confidence: 1.00, sources: 2/2, outliers: 0)
Aggregated price for USDC/USD: 0.9999575 (confidence: 1.00, sources: 2/2, outliers: 0)
Prices: WETH=$2035.38, USDC=$0.999958
Balance: 10 WETH ($20353.83)
Swapping $10 WETH -> USDC via PancakeSwap V3
intent: SWAP: $10.00 WETH -> USDC (slippage: 1.00%) via pancakeswap_v3
Compiled SWAP: 0.0049 WETH -> 9.9704 USDC (min: 9.8707 USDC) | Slippage: 1.00% | Txs: 2 | Gas: 280,000
Simulation successful: 2 transaction(s), total gas: 353800
TX c8b36c... confirmed block=436466232, gas=53452
TX aadb40... confirmed block=436466233, gas=173264
EXECUTED: SWAP completed successfully | Txs: 2 (c8b36c...b5d0, aadb40...c5af) | 226,716 gas
Parsed PancakeSwap V3 receipt: tx=..., swaps=1
Enriched SWAP result with: swap_amounts (protocol=pancakeswap_v3, chain=arbitrum)
Status: SUCCESS | Intent: SWAP | Gas used: 226716 | Duration: 26643ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Token resolution failure: BTC | `token_resolution_error token=BTC chain=arbitrum error_type=TokenNotFoundError detail=Cannot resolve token 'BTC' on arbitrum: Symbol 'BTC' not found in registry. Suggestions: Did you mean 'WBTC'?` |
| 2 | strategy | WARNING | Token resolution failure: STETH | `token_resolution_error token=STETH chain=arbitrum error_type=TokenNotFoundError detail=Cannot resolve token 'STETH' on arbitrum. Suggestions: Did you mean 'WSTETH'?` |
| 3 | strategy | WARNING | Token resolution failure: RDNT | `token_resolution_error token=RDNT chain=arbitrum error_type=TokenNotFoundError detail=Cannot resolve token 'RDNT' on arbitrum` |
| 4 | strategy | WARNING | Token resolution failure: MAGIC | `token_resolution_error token=MAGIC chain=arbitrum error_type=TokenNotFoundError detail=Cannot resolve token 'MAGIC' on arbitrum` |
| 5 | strategy | WARNING | Token resolution failure: WOO | `token_resolution_error token=WOO chain=arbitrum error_type=TokenNotFoundError detail=Cannot resolve token 'WOO' on arbitrum` |
| 6 | strategy | INFO | Public RPC in use (no Alchemy key) | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 7 | strategy | INFO | Anvil port cleanup timing | `Port 53042 not freed after 5.0s` |

**Notes on findings:**

- Findings 1-5 (BTC, STETH, RDNT, MAGIC, WOO resolution warnings): These appear during CoinGecko price source initialization at gateway startup. The gateway iterates a pre-warm token list that includes symbols not registered in the Arbitrum static registry. This does not affect strategy execution -- WETH and USDC both resolved correctly. The pre-warm list should be trimmed to symbols registered per chain.
- Finding 6 is informational -- the public RPC worked for this test. Risk exists at high request rates.
- Finding 7 is a cosmetic cleanup race condition at teardown. Anvil was stopped successfully.
- No zero prices, no failed API fetches, no reverts, no NaN/None in numeric contexts.

## Result

**PASS** - The `pancakeswap_simple` strategy executed a WETH->USDC swap on PancakeSwap V3 on an Arbitrum Anvil fork. Both transactions (approve + swap) confirmed on-chain. 0.0049 WETH swapped for ~9.97 USDC. Total gas: 226,716. Receipt parsing and result enrichment succeeded cleanly.

SUSPICIOUS_BEHAVIOUR_COUNT: 7
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
