# E2E Strategy Test Report: almanak_rsi (Anvil)

**Date:** 2026-02-23 03:41
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | almanak_rsi |
| Chain | base (Chain ID 8453) |
| Network | Anvil fork |
| Anvil Port | 8547 (pre-start) / 58965 (managed gateway auto-spawned) |
| Protocol | uniswap_v3 |
| Trading Pair | ALMANAK/USDC |
| Pool | 0xbDbC38652D78AF0383322bBc823E06FA108d0874 |
| RSI Period | 14 |
| RSI Oversold / Overbought | 30 / 70 |
| Cooldown | 1 hour |
| initial_capital_usdc | 20 (init swap = $10, well under $100 budget cap) |

## Config Changes Made

None. The `initial_capital_usdc` of 20 results in an initialization swap of $10 (half of capital), which is well within the $100 budget cap. The strategy has no `force_action` field; the initialization phase guarantees a trade on the first run when no prior state exists.

## Execution

### Setup
- Anvil fork of Base mainnet started on port 8547 (Chain ID 8453 confirmed)
- Anvil default wallet funded: ~99 ETH, 10,000 USDC (slot 9), 1 WETH
- Strategy runner launched its own managed gateway on port 50052 with internal Anvil fork on port 58965
- Managed gateway auto-funded wallet per `anvil_funding` config (ETH, USDC, WETH)
- No existing `almanak_rsi` strategy state in `almanak_state.db` -- fresh start

### Strategy Run
- Strategy executed with `--network anvil --once`
- Mode: FRESH START (no existing state)
- Initialization phase triggered (`_initialized = False` on first run)
- ALMANAK price fetched: $0.00183198 (CoinGecko free tier)
- USDC price fetched: $0.999925
- Initial buy: 10.000000 USDC -> ALMANAK
- Compiled: 10.0000 USDC -> 5441.7910 ALMANAK (min: 5387.3731 ALMANAK, 1% slippage)
- 2 transactions submitted and confirmed (blocks 42501773, 42501774)

### Transactions

| Intent | TX Hash | Block | Gas Used | Status |
|--------|---------|-------|----------|--------|
| APPROVE (USDC) | `f8b7ba3f49f370875ccba015351cc6403d05a861aec60a21e27c8caa4463baa9` | 42501773 | 55,437 | SUCCESS |
| SWAP (USDC->ALMANAK) | `433e879e61e90aff2c009c8578034a97bba8391fa231fa498246006dd519cf89` | 42501774 | 145,344 | SUCCESS |
| **Total** | | | **200,781** | |

### Key Log Output

```text
[info] Aggregated price for USDC/USD: 0.999925 (confidence: 1.00, sources: 1/1)
[info] Aggregated price for ALMANAK/USD: 0.00183198 (confidence: 1.00, sources: 1/1)
[info] INITIALIZATION: First run - buying ALMANAK for $10.00 (half of initial capital)
[info] almanak_rsi intent: SWAP: 10.000000 USDC -> ALMANAK (slippage: 1.00%) via uniswap_v3
[info] Compiled SWAP: 10.0000 USDC -> 5441.7910 ALMANAK (min: 5387.3731 ALMANAK)
[info]    Slippage: 1.00% | Txs: 2 | Gas: 280,000
[info] Transaction confirmed: tx=f8b7ba...baa9, block=42501773, gas_used=55437
[info] Transaction confirmed: tx=433e87...cf89, block=42501774, gas_used=145344
[info] EXECUTED: SWAP completed successfully | Txs: 2 | 200,781 gas
[info] Parsed Uniswap V3 swap: 0.0000 token0 -> 5436.4807 token1
[info] Initialization swap succeeded - strategy is now initialized
[info] Trade executed successfully (total trades: 1)
Status: SUCCESS | Intent: SWAP | Gas used: 200781 | Duration: 20436ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Token resolution failure: `USDC.e` not found on Base | `token_resolution_error token=USDC.e chain=base error_type=TokenNotFoundError ... Suggestions: Did you mean 'USDC'?` |
| 2 | strategy | WARNING | Token resolution failure: `USDC_BRIDGED` not found on Base | `token_resolution_error token=USDC_BRIDGED chain=base error_type=TokenNotFoundError ... Suggestions: Did you mean 'USDC'?` |
| 3 | gateway | WARNING | CoinGecko free tier in use (rate limit 30 req/min) | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 4 | strategy | WARNING | Gas estimate below compiler floor -- compiler limit used | `Gas estimate tx[0]: raw=55,819 buffered=83,728 (x1.5) < compiler=120,000, using compiler limit` |

**Notes on findings:**

- **Findings 1 & 2** (`USDC.e` / `USDC_BRIDGED`): The intent compiler probes bridged USDC aliases during compilation to find the best route. These lookups fail gracefully -- `USDC.e` does not exist on Base (it is Arbitrum-specific) and `USDC_BRIDGED` is not in the registry. The swap proceeded correctly using native Base USDC by address. Non-fatal but noisy; the probes add a small latency overhead.
- **Finding 3**: CoinGecko free tier is expected in a dev/test environment. ALMANAK price was fetched successfully ($0.00183198). Not a blocking issue but poses rate-limit risk under heavy parallel testing.
- **Finding 4**: The gas estimator returned a lower bound than the compiler's conservative reservation (120k for approvals). The orchestrator correctly defers to the compiler limit. Expected behaviour -- not a bug.

## Result

**PASS** - The `almanak_rsi` strategy on Base (Anvil fork) successfully executed the initialization swap, purchasing ~5,436 ALMANAK for $10.00 USDC via Uniswap V3. Both the USDC approval and swap transactions were confirmed on-chain (200,781 gas total). Strategy marked as initialized and state persisted. No ERROR-severity findings. Four WARNING-level findings noted: two are benign token alias probes (compiler artefact), one is an API advisory, one is a normal gas-floor deferral.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
