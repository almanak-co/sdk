# E2E Strategy Test Report: almanak_rsi (Anvil)

**Date:** 2026-03-03 11:37 (kitchen iter-29 re-run)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~25 seconds (strategy run)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | almanak_rsi |
| Chain | base (Chain ID 8453) |
| Network | Anvil fork (Base mainnet, public RPC fallback) |
| Anvil Port | 65018/65150 (managed, auto-selected by CLI) |
| Protocol | uniswap_v3 |
| Trading Pair | ALMANAK/USDC |
| Pool | 0xbDbC38652D78AF0383322bBc823E06FA108d0874 |
| RSI Period | 14 |
| RSI Oversold / Overbought | 30 / 70 |
| Cooldown | 1 hour |
| initial_capital_usdc | $20 (init swap = $10, well within $500 budget cap) |

## Config Changes Made

None. The `initial_capital_usdc` of $20 results in an initialization swap of $10 (half of
capital), which is well within the $500 budget cap. The strategy has no `force_action` field;
the initialization phase guarantees a trade on the first run when no prior state exists.

## Execution

### Setup
- Managed gateway auto-started on port 50052 (network=anvil)
- Base mainnet fork created at block 42874218 (chain ID 8453)
- Wallet auto-funded per `anvil_funding` config: 100 ETH, 10,000 USDC (slot 9), 1 WETH (slot 3)
- No existing `almanak_rsi` strategy state found (fresh start)
- Public RPC used: `https://base-rpc.publicnode.com` (no ALCHEMY_API_KEY configured in .env)

### Strategy Run
- Strategy executed with `--network anvil --once`
- Mode: FRESH START (no existing state)
- Initialization phase triggered (`_initialized = False` on first run)
- ALMANAK price fetched: $0.00207676 (GeckoTerminal, confidence 0.90, 1/2 sources)
- USDC price fetched: $0.9999990 (confidence 1.00, 2/2 sources)
- Initial buy: 10.000000 USDC -> ALMANAK (half of $20 initial capital)
- Compiled: 10.0000 USDC -> 4800.7425 ALMANAK (min: 4752.7351 ALMANAK, 1% slippage)
- 2 transactions submitted and confirmed on-chain
- Strategy state saved: `initialized=True`, `trade_count=1`

### Transactions

| Intent | TX Hash | Block | Gas Used | Status |
|--------|---------|-------|----------|--------|
| APPROVE (USDC) | `0x2afcac272eb1bb20426e346dd7c6744706a01f6527f6231bb60c7825e260b719` | 42874258 | 55,437 | SUCCESS |
| SWAP (USDC->ALMANAK) | `0x87d4775f30806e0dd29e1f8af446621c3317bcb0002c74de7d3fdc7240e4505d` | 42874259 | 145,336 | SUCCESS |
| **Total** | | | **200,773** | |

*Note: These are Anvil local fork transactions, not mainnet.*

### Key Log Output

```text
[info] Aggregated price for USDC/USD: 0.9999990000000001 (confidence: 1.00, sources: 2/2, outliers: 0)
[info] Aggregated price for ALMANAK/USD: 0.00207676 (confidence: 0.90, sources: 1/2, outliers: 0)
[info] INITIALIZATION: First run - buying ALMANAK for $10.00 (half of initial capital)
[info] almanak_rsi intent: SWAP: 10.000000 0x833589fcd6...02913 -> 0xdefa1d21c5...cc3a3 (slippage: 1.00%) via uniswap_v3
[info] Compiled SWAP: 10.0000 USDC -> 4800.7425 ALMANAK (min: 4752.7351 ALMANAK)
[info]    Slippage: 1.00% | Txs: 2 | Gas: 280,000
[info] Simulation successful: 2 transaction(s), total gas: 355,819
[info] TX 1 confirmed: block=42874258, gas=55437
[info] TX 2 confirmed: block=42874259, gas=145336
[info] EXECUTED: SWAP completed successfully
[info]    Txs: 2 (2afcac...b719, 87d477...505d) | 200,773 gas
[info] Parsed Uniswap V3 swap: 0.0000 token0 -> 4829.7565 token1, slippage=N/A
[info] Initialization swap succeeded - strategy is now initialized
[info] Trade executed successfully (total trades: 1)
Status: SUCCESS | Intent: SWAP | Gas used: 200773 | Duration: 24925ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | ALMANAK price: only 1 of 2 sources available (no Chainlink oracle) | `ALMANAK/USD: 0.00207019 (confidence: 0.90, sources: 1/2, outliers: 0)` |
| 2 | gateway | INFO | No CoinGecko API key, using free tier as fallback | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |
| 3 | gateway | INFO | No Alchemy API key, using public RPC (rate limits apply) | `No API key configured -- using free public RPC for base (rate limits may apply)` |
| 4 | gateway | WARNING | Port 58000 not freed after 5.0s (cleanup timing) | `Port 58000 not freed after 5.0s` |
| 5 | gateway | WARNING | Insecure mode active (expected for anvil) | `INSECURE MODE: Auth interceptor disabled -- acceptable for local development on 'anvil'` |

**Notes on findings:**

- **Finding 1 (Price confidence 0.90):** ALMANAK has only 1/2 price sources; no Chainlink oracle
  exists for this small-cap token. Price sourced solely via GeckoTerminal (CoinGecko free tier).
  Non-blocking; strategy handled it correctly. Worth noting for production risk assessment.
- **Findings 2-3 (Public RPC / CoinGecko fallback):** Expected in a dev/test environment
  without `ALCHEMY_API_KEY` or `ALMANAK_GATEWAY_COINGECKO_API_KEY`. Both fallbacks worked correctly.
- **Finding 4 (Port not freed):** Minor cleanup timing issue in Anvil fork manager after strategy
  exit. Does not affect test correctness.
- **Finding 5 (Insecure mode):** Expected and correct for local Anvil development mode.

**No ERROR-severity findings. No zero prices. No transaction reverts. No token resolution failures.**

## Result

**PASS** - The `almanak_rsi` strategy on Base (Anvil fork) successfully executed its
initialization swap, buying 4829.76 ALMANAK for $10.00 USDC via Uniswap V3. Both the USDC approval
and swap transactions were confirmed on-chain (200,773 gas total). Strategy correctly transitioned
from uninitialized to initialized state and persisted state to SQLite.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
