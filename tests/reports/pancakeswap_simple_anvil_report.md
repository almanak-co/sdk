# E2E Strategy Test Report: pancakeswap_simple (Anvil)

**Date:** 2026-03-16 01:30
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_pancakeswap_simple |
| Chain | arbitrum |
| Network | Anvil fork (managed, Alchemy RPC) |
| Anvil Port | 55766 (auto-assigned by managed gateway) |
| swap_amount_usd | $10 (within $1000 budget cap) |
| from_token | WETH |
| to_token | USDC |
| max_slippage | 1.00% |

**Config changes made:** None. `swap_amount_usd` was already $10, well within the $1000 budget cap.
The strategy always executes a swap unconditionally (no `force_action` field supported or needed).

## Execution

### Setup
- [x] Anvil fork auto-started by managed gateway (port 55766, chain_id=42161, block 442133488, Alchemy RPC)
- [x] Gateway started on port 50052 (insecure mode, anvil network, managed/embedded)
- [x] Wallet auto-funded by managed gateway via `anvil_funding` config: 100 ETH, 10 WETH (slot 51), 10,000 USDC (slot 9)
- [x] Pricing: 4-source aggregation (Chainlink + Binance + DexScreener + CoinGecko), all 4 sources active

### Strategy Run
- [x] Strategy executed with `uv run almanak strat run -d strategies/demo/pancakeswap_simple --network anvil --once`
- [x] Prices fetched: WETH=$2108.52, USDC=$0.999983 (confidence: 1.00, sources: 4/4, outliers: 0)
- [x] Balance confirmed: 10 WETH ($21,085.20)
- [x] SWAP intent returned: $10.00 WETH -> USDC via pancakeswap_v3 (1.00% slippage)
- [x] Intent compiled: 0.0047 WETH -> 9.9702 USDC (min: 9.8705 USDC), 2 TXs, gas estimate 280,000
- [x] Simulation: successful via LocalSimulator / eth_estimateGas (total gas 353,800)
- [x] Receipt parsed: PancakeSwap V3 swap detected (swaps=1)
- [x] Result enriched with swap_amounts

### Transactions

| TX # | Hash | Block | Gas Used | Status |
|------|------|-------|----------|--------|
| 1 (Approve) | `0xc84bf0fe47b9c3adbac6ba254c0e78691f1809663ce8468c6af7f5dde1b39f61` | 442133491 | 53,452 | SUCCESS |
| 2 (Swap) | `0xc53715ab5280f544b7732c20db7f4690997eaa1c99732f87d983b4140d466461` | 442133492 | 173,207 | SUCCESS |

**Total gas used:** 226,659 | Duration: 28,479ms

### Key Log Output

```text
Aggregated price for WETH/USD: 2108.52 (confidence: 1.00, sources: 4/4, outliers: 0)
Aggregated price for USDC/USD: 0.9999825 (confidence: 1.00, sources: 4/4, outliers: 0)
Prices: WETH=$2108.52, USDC=$0.999982
Balance: 10 WETH ($21085.20)
Swapping $10 WETH -> USDC via PancakeSwap V3
intent: SWAP: $10.00 WETH -> USDC (slippage: 1.00%) via pancakeswap_v3
Compiled SWAP: 0.0047 WETH -> 9.9702 USDC (min: 9.8705 USDC) | Slippage: 1.00% | Txs: 2 | Gas: 280,000
Simulation successful: 2 transaction(s), total gas: 353800
TX c84bf0...9f61 confirmed block=442133491, gas=53452
TX c53715...6461 confirmed block=442133492, gas=173207
EXECUTED: SWAP completed successfully | Txs: 2 | 226,659 gas
Parsed PancakeSwap V3 receipt: tx=0xc84bf0fe..., swaps=0
Parsed PancakeSwap V3 receipt: tx=0xc53715ab..., swaps=1
Enriched SWAP result with: swap_amounts (protocol=pancakeswap_v3, chain=arbitrum)
Status: SUCCESS | Intent: SWAP | Gas used: 226659 | Duration: 28479ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Insecure mode — expected for Anvil local dev | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | INFO | No CoinGecko API key — Chainlink primary, CoinGecko fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

**Notes on findings:**

- Item 1 (Insecure mode): Fully expected for Anvil local dev. The gateway correctly identifies this as
  acceptable and logs a warning. Non-blocking.
- Item 2 (No CoinGecko key): CoinGecko is used as fallback only; primary pricing is Chainlink on-chain.
  All 4 price sources (Chainlink + Binance + DexScreener + CoinGecko) aggregated with confidence 1.00
  in this run, so the missing dedicated API key had no impact.
- No zero prices, no failed API fetches, no reverts, no token resolution failures, no rate limiting,
  no NaN/None in numeric contexts.

## Result

**PASS** - The `pancakeswap_simple` strategy executed a WETH->USDC swap on PancakeSwap V3 on an
Arbitrum Anvil fork. Both transactions (approve + swap) confirmed on-chain. ~0.0047 WETH swapped
for ~9.97 USDC. Total gas: 226,659. Receipt parsing and result enrichment succeeded cleanly.
Pricing aggregation improved vs previous run: 4/4 sources active (vs 2/2 previously), no rate
limiting observed.

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
