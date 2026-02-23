# E2E Strategy Test Report: pancakeswap_simple (Anvil)

**Date:** 2026-02-23 03:58
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pancakeswap_simple |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 8545 (manual pre-fund), 62149 (managed by runner) |
| swap_amount_usd | $10 |
| from_token | WETH |
| to_token | USDC |
| max_slippage | 1.00% |

**Config changes made:** None. `swap_amount_usd` was already $10 (well within $100 cap).

## Execution

### Setup
- [x] Anvil started on port 8545 (Arbitrum fork, chain ID 42161)
- [x] Gateway started on port 50051
- [x] Wallet pre-funded: 100 ETH, 10 WETH, 10,000 USDC (Anvil default wallet `0xf39Fd6e5...`)
- [x] Runner also auto-managed its own Anvil fork (port 62149) + gateway (port 50052) with anvil_funding from config

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Prices fetched: WETH = $1,940.06, USDC = $0.999896
- [x] Balance confirmed: 10 WETH ($19,400.60)
- [x] Intent produced: SWAP $10 WETH -> USDC via pancakeswap_v3
- [x] Intent compiled: 0.0052 WETH -> 9.9710 USDC (min: 9.8713 USDC)
- [x] 2 transactions submitted and confirmed

### Transactions

| # | TX Hash | Gas Used | Status |
|---|---------|----------|--------|
| 1 (approve) | `67cda978c109ed015eb8aa501d322887a4ac229a874591bd081059033803f448` | 53,452 | SUCCESS |
| 2 (swap) | `817e8d011267f940e6edcaf59f67e80519e901b7f058c028552198652fbc4f13` | 173,292 | SUCCESS |

**Total gas used:** 226,744

### Key Log Output

```text
Aggregated price for WETH/USD: 1940.06 (confidence: 1.00, sources: 1/1, outliers: 0)
Aggregated price for USDC/USD: 0.999896 (confidence: 1.00, sources: 1/1, outliers: 0)
Balance: 10 WETH ($19400.60)
Swapping $10 WETH -> USDC via PancakeSwap V3
Compiled SWAP: 0.0052 WETH -> 9.9710 USDC (min: 9.8713 USDC) | Slippage: 1.00% | Txs: 2 | Gas: 280,000
Transaction confirmed: tx_hash=67cda978..., block=434902203, gas_used=53452
Transaction confirmed: tx_hash=817e8d01..., block=434902204, gas_used=173292
EXECUTED: SWAP completed successfully | Txs: 2 | 226,744 gas
Status: SUCCESS | Intent: SWAP | Gas used: 226744 | Duration: 21816ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | WARNING | CoinGecko free tier rate limit risk | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 3 | strategy | WARNING | Gas estimate below compiler limit | `Gas estimate tx[0]: raw=53,800 buffered=80,700 (x1.5) < compiler=120,000, using compiler limit` |
| 4 | strategy | WARNING | PancakeSwapV3ReceiptParser missing swap_amounts | `Parser PancakeSwapV3ReceiptParser does not declare support for 'swap_amounts' (expected by SWAP)` |
| 5 | strategy | WARNING | Amount chaining broken due to missing swap_amounts | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 6 | strategy | INFO | Anvil fork port not freed cleanly on shutdown | `Port 62149 not freed after 5.0s` |

**Notes on findings:**

- Findings #1 and #2 are normal/expected for local Anvil testing with no production API keys. No functional impact on this run.
- Finding #3 (gas estimate below compiler limit) is benign -- the compiler limit was used as the floor and both transactions confirmed successfully. Actual gas (53,452) was well within the limit.
- **Findings #4 and #5 are a real bug:** `PancakeSwapV3ReceiptParser` does not implement the `extract_swap_amounts()` method required by the `ResultEnricher` framework contract. This means:
  - The strategy cannot chain swap output amounts (e.g., `amount='all'` for a subsequent step would fail silently).
  - Post-execution enrichment data (actual in/out amounts) is not surfaced to the strategy author's `on_intent_executed` callback.
  - This is a gap compared to Uniswap V3 and Enso receipt parsers which both implement `extract_swap_amounts()`.
- Finding #6 is a minor Anvil cleanup race condition on teardown; not functionally impactful.

## Result

**PASS** - The pancakeswap_simple strategy executed a $10 WETH->USDC swap via PancakeSwap V3 on Arbitrum Anvil fork. 2 transactions confirmed (approve + swap), total 226,744 gas. One notable bug detected: `PancakeSwapV3ReceiptParser` does not implement `swap_amounts` extraction, breaking amount chaining for multi-step intents that depend on prior swap outputs.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 6
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
