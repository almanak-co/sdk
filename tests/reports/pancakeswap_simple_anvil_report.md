# E2E Strategy Test Report: pancakeswap_simple (Anvil)

**Date:** 2026-03-05 22:33
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_pancakeswap_simple |
| Chain | arbitrum |
| Network | Anvil fork (managed, public RPC: publicnode.com) |
| Anvil Port | 60741 (auto-assigned by managed gateway) |
| swap_amount_usd | $10 (within $50 budget cap) |
| from_token | WETH |
| to_token | USDC |
| max_slippage | 1.00% |

**Config changes made:** None. `swap_amount_usd` was $10, within the $50 budget cap. The strategy
always executes a swap unconditionally (no `force_action` field supported or needed).

**Note on Alchemy key:** `ALCHEMY_API_KEY` was empty in `.env`. The CLI auto-selected a free public
Arbitrum RPC. Pricing used on-chain Chainlink oracles as primary source with free CoinGecko as fallback.

## Execution

### Setup
- [x] Anvil fork auto-started by managed gateway (port 60741, chain_id=42161, block 438736486, public RPC)
- [x] Gateway started on port 50053 (insecure mode, anvil network, managed/embedded)
- [x] Wallet auto-funded by managed gateway via `anvil_funding` config: 100 ETH, 10 WETH (slot 51), 10,000 USDC (slot 9)
- [x] Pricing: on-chain Chainlink (primary) + free CoinGecko (fallback)

### Strategy Run
- [x] Strategy executed with `uv run almanak strat run -d strategies/demo/pancakeswap_simple --network anvil --once`
- [x] Prices fetched: WETH=$2080.15, USDC=$1.00 (on-chain Chainlink; CoinGecko rate-limited for USDC, fallback used)
- [x] Balance confirmed: 10 WETH ($20,801.50)
- [x] SWAP intent returned: $10.00 WETH -> USDC via pancakeswap_v3 (1.00% slippage)
- [x] Intent compiled: 0.0048 WETH -> 9.9700 USDC (min: 9.8703 USDC), 2 TXs, gas estimate 280,000
- [x] Simulation: successful via LocalSimulator / eth_estimateGas (total gas 353,800)
- [x] Receipt parsed: PancakeSwap V3 swap detected (swaps=1)
- [x] Result enriched with swap_amounts

### Transactions

| TX # | Hash | Block | Gas Used | Status |
|------|------|-------|----------|--------|
| 1 (Approve) | `0x51ee32a3969c757f148dd4305b4dc0eb5efffdd21a3ae068cd48528d1d91f8fd` | 438736489 | 53,452 | SUCCESS |
| 2 (Swap) | `0x45b0709813da7e316658d7773f65c324b983419b9638466efdda2797903f5976` | 438736490 | 173,227 | SUCCESS |

**Total gas used:** 226,679 | Duration: 27,776ms

### Key Log Output

```text
Aggregated price for WETH/USD: 2080.149799625 (confidence: 1.00, sources: 2/2, outliers: 0)
Rate limited by CoinGecko for USDC/USD, backoff: 1.00s
Aggregated price for USDC/USD: 1.00 (confidence: 0.90, sources: 1/2, outliers: 0)
Prices: WETH=$2080.15, USDC=$1.000000
Balance: 10 WETH ($20801.50)
Swapping $10 WETH -> USDC via PancakeSwap V3
intent: SWAP: $10.00 WETH -> USDC (slippage: 1.00%) via pancakeswap_v3
Compiled SWAP: 0.0048 WETH -> 9.9700 USDC (min: 9.8703 USDC) | Slippage: 1.00% | Txs: 2 | Gas: 280,000
Simulation successful: 2 transaction(s), total gas: 353800
TX 51ee32...f8fd confirmed block=438736489, gas=53452
TX 45b070...5976 confirmed block=438736490, gas=173227
EXECUTED: SWAP completed successfully | Txs: 2 | 226,679 gas
Parsed PancakeSwap V3 receipt: tx=0x51ee32a3..., swaps=0
Parsed PancakeSwap V3 receipt: tx=0x45b07098..., swaps=1
Enriched SWAP result with: swap_amounts (protocol=pancakeswap_v3, chain=arbitrum)
Status: SUCCESS | Intent: SWAP | Gas used: 226679 | Duration: 27776ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | CoinGecko rate limit on USDC price (free tier) | `Rate limited by CoinGecko for USDC/USD, backoff: 1.00s` |
| 2 | strategy | ERROR | Circular import in incubating strategy scan | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy: cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |

**Notes on findings:**

- Item 1 (CoinGecko rate limit): Expected on the free tier with no API key. The aggregator correctly
  fell back to on-chain pricing for USDC (confidence 0.90 with 1/2 sources). Price resolved correctly as $1.00.
  Non-blocking and non-impactful for this test.
- Item 2 (circular import): Pre-existing issue in `strategies/incubating/pendle_pt_swap_arbitrum/strategy.py`.
  A circular import in `almanak/__init__.py` prevents this incubating strategy from being discovered.
  This does not affect `pancakeswap_simple` execution but is a real ERROR-severity defect that should
  be tracked separately.
- No zero prices, no failed API fetches (only rate-limited with fallback), no token resolution failures,
  no on-chain reverts, no NaN/None in numeric contexts.

## Result

**PASS** - The `pancakeswap_simple` strategy executed a WETH->USDC swap on PancakeSwap V3 on an Arbitrum Anvil
fork. Both transactions (approve + swap) confirmed on-chain. ~0.0048 WETH swapped for ~9.97 USDC. Total gas:
226,679. Receipt parsing and result enrichment succeeded cleanly. One pre-existing incubating strategy import
error detected (pendle_pt_swap_arbitrum circular import).

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
