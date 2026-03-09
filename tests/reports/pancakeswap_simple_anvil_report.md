# E2E Strategy Test Report: pancakeswap_simple (Anvil)

**Date:** 2026-03-03 19:06 (re-run)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_pancakeswap_simple |
| Chain | arbitrum |
| Network | Anvil fork (managed, public RPC: arbitrum-one-rpc.publicnode.com) |
| Anvil Port | 53960 (auto-assigned by managed gateway) |
| swap_amount_usd | $10 (within $500 budget cap) |
| from_token | WETH |
| to_token | USDC |
| max_slippage | 1.00% |

**Config changes made:** None. `swap_amount_usd` was $10, within the $500 budget cap. The strategy always executes a swap (no `force_action` field supported or needed).

**Note on Alchemy key:** `ALCHEMY_API_KEY` was empty in `.env`. The CLI auto-selected a free public Arbitrum RPC (`arbitrum-one-rpc.publicnode.com`). Pricing used on-chain Chainlink oracles as primary source with free CoinGecko as fallback.

## Execution

### Setup
- [x] Anvil fork auto-started by managed gateway (port 53960, chain_id=42161, block 437892416, public RPC: publicnode.com)
- [x] Gateway started on port 50052 (insecure mode, anvil network, managed/embedded)
- [x] Wallet funded by managed gateway via `anvil_funding` config: 100 ETH, 10 WETH (storage slot 51), 10,000 USDC (storage slot 9)
- [x] Pricing: on-chain Chainlink (primary) + free CoinGecko (fallback)

### Strategy Run
- [x] Strategy executed with `uv run almanak strat run -d strategies/demo/pancakeswap_simple --network anvil --once`
- [x] Prices fetched: WETH=$1,964.90, USDC=$1.000000 (aggregated, confidence 1.00, sources 2/2)
- [x] Balance confirmed: 10 WETH ($19,649.02)
- [x] SWAP intent returned: $10.00 WETH -> USDC via pancakeswap_v3 (1.00% slippage)
- [x] Intent compiled: 0.0051 WETH -> 9.9700 USDC (min: 9.8703 USDC), 2 TXs, gas estimate 280,000
- [x] Simulation: successful via eth_estimateGas (total gas 353,800)
- [x] Receipt parsed: PancakeSwap V3 swap detected (swaps=1)
- [x] Result enriched with swap_amounts

### Transactions

| TX # | Hash | Block | Gas Used | Status |
|------|------|-------|----------|--------|
| 1 (Approve) | `4293e055bbe51bedbee2cddda42c000465fb95a9b1d2685795a5ccadeba05c74` | 437892419 | 53,452 | SUCCESS |
| 2 (Swap)    | `50b4ed1925f21695c20f9f5e0cb68e11855d5c3e887471fc9c7989433a42d958` | 437892420 | 173,252 | SUCCESS |

**Total gas used:** 226,704

### Key Log Output

```text
Aggregated price for WETH/USD: 1964.90194802 (confidence: 1.00, sources: 2/2, outliers: 0)
Aggregated price for USDC/USD: 1.0 (confidence: 1.00, sources: 2/2, outliers: 0)
Prices: WETH=$1964.90, USDC=$1.000000
Balance: 10 WETH ($19649.02)
Swapping $10 WETH -> USDC via PancakeSwap V3
intent: SWAP: $10.00 WETH -> USDC (slippage: 1.00%) via pancakeswap_v3
Compiled SWAP: 0.0051 WETH -> 9.9700 USDC (min: 9.8703 USDC) | Slippage: 1.00% | Txs: 2 | Gas: 280,000
Simulation successful: 2 transaction(s), total gas: 353800
TX 4293e0... confirmed block=437892419, gas=53452
TX 50b4ed... confirmed block=437892420, gas=173252
EXECUTED: SWAP completed successfully | Txs: 2 (4293e0...5c74, 50b4ed...d958) | 226,704 gas
Parsed PancakeSwap V3 receipt: tx=0x50b4ed19..., swaps=1
Enriched SWAP result with: swap_amounts (protocol=pancakeswap_v3, chain=arbitrum)
Status: SUCCESS | Intent: SWAP | Gas used: 226704 | Duration: 27788ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | No CoinGecko API key / fallback mode | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | gateway | INFO | Free public RPC (rate limits apply) | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 3 | gateway | WARNING | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 4 | strategy | WARNING | Port not freed after 5.0s (cosmetic) | `Port 53960 not freed after 5.0s` |

**Notes on findings:**

- Items 1 and 2 are informational: no API keys configured in this environment. On-chain Chainlink pricing worked correctly (WETH=$1964.90, USDC=$1.00, confidence 1.00 from 2 sources).
- Item 3 (insecure mode) is expected and explicitly noted as acceptable for Anvil in the gateway logs.
- Item 4 is a cosmetic cleanup race condition at teardown; Anvil stopped successfully immediately after.
- No zero prices, no failed API fetches, no token resolution failures, no reverts, no NaN/None in numeric contexts.

## Result

**PASS** - The `pancakeswap_simple` strategy executed a WETH->USDC swap on PancakeSwap V3 on an Arbitrum Anvil fork. Both transactions (approve + swap) confirmed on-chain. 0.0051 WETH swapped for ~9.97 USDC. Total gas: 226,704. Receipt parsing and result enrichment succeeded cleanly. No suspicious data layer behaviour detected.

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
