# E2E Strategy Test Report: almanak_rsi (Anvil)

**Date:** 2026-02-27 22:43
**Result:** PASS
**Mode:** Anvil
**Duration:** ~35 seconds (strategy iteration)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | almanak_rsi |
| Chain | base (Chain ID 8453) |
| Network | Anvil fork (Base mainnet, public RPC fallback) |
| Anvil Port | 55137 (managed, auto-selected) |
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
- Base mainnet fork created at block 42708804 (chain ID 8453)
- Wallet auto-funded per `anvil_funding` config: 100 ETH, 10,000 USDC (slot 9), 1 WETH (slot 3)
- No existing `almanak_rsi` strategy state found (fresh start)
- Public RPC used: `https://base-rpc.publicnode.com` (no ALCHEMY_API_KEY configured)

### Strategy Run
- Strategy executed with `--network anvil --once`
- Mode: FRESH START (no existing state)
- Initialization phase triggered (`_initialized = False` on first run)
- ALMANAK price fetched: $0.00201819 (GeckoTerminal, confidence 0.90, 1/2 sources)
- USDC price fetched: $0.999994 (confidence 1.00, 2/2 sources)
- Initial buy: 10.000000 USDC -> ALMANAK (half of $20 initial capital)
- Compiled: 10.0000 USDC -> 4940.0404 ALMANAK (min: 4890.6400 ALMANAK, 1% slippage)
- 2 transactions submitted and confirmed on-chain
- Strategy state saved: `initialized=True`, `trade_count=1`

### Transactions

| Intent | TX Hash | Block | Gas Used | Status |
|--------|---------|-------|----------|--------|
| APPROVE (USDC) | `0x20064ba2b9004bce98158ca8f454edff0a1581a890a1bff2430065ba8a2fc17b` | 42708807 | 55,437 | SUCCESS |
| SWAP (USDC->ALMANAK) | `0x354d77f44a0cfc9f49830b5ee8dfd1fe786647b99717f41f5d5197a68e50d1c7` | 42708808 | 136,678 | SUCCESS |
| **Total** | | | **192,115** | |

*Note: These are Anvil local fork transactions, not mainnet.*

### Key Log Output

```text
[info] Aggregated price for USDC/USD: 0.999994 (confidence: 1.00, sources: 2/2)
[info] Aggregated price for ALMANAK/USD: 0.00201819 (confidence: 0.90, sources: 1/2)
[info] INITIALIZATION: First run - buying ALMANAK for $10.00 (half of initial capital)
[info] almanak_rsi intent: SWAP: 10.000000 0x833589fcd6...02913 -> 0xdefa1d21c5...cc3a3 (slippage: 1.00%) via uniswap_v3
[info] Compiled SWAP: 10.0000 USDC -> 4940.0404 ALMANAK (min: 4890.6400 ALMANAK)
[info]    Slippage: 1.00% | Txs: 2 | Gas: 280,000
[info] Simulation successful: 2 transaction(s), total gas: 355819
[info] TX 1 confirmed: block=42708807, gas=55437
[info] TX 2 confirmed: block=42708808, gas=136678
[info] EXECUTED: SWAP completed successfully
[info]    Txs: 2 (20064b...c17b, 354d77...d1c7) | 192,115 gas
[info] Parsed Uniswap V3 swap: 0.0000 token0 -> 4940.0603 token1, slippage=N/A
[info] Initialization swap succeeded - strategy is now initialized
[info] Trade executed successfully (total trades: 1)
Status: SUCCESS | Intent: SWAP | Gas used: 192115 | Duration: 34476ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | WARNING | Token resolution failure: BTC not in Base registry | `token_resolution_error token=BTC chain=base error_type=TokenNotFoundError ... Symbol 'BTC' not found in registry for base` |
| 2 | gateway | WARNING | Token resolution failure: WBTC not in Base registry | `token_resolution_error token=WBTC chain=base error_type=TokenNotFoundError` |
| 3 | gateway | WARNING | Token resolution failure: STETH not in Base registry | `token_resolution_error token=STETH chain=base ... Did you mean 'WSTETH'?` |
| 4 | gateway | WARNING | Token resolution failure: CBETH not in Base registry | `token_resolution_error token=CBETH chain=base error_type=TokenNotFoundError` |
| 5 | gateway | INFO | Public RPC fallback for Base (no Alchemy key) | `No API key configured -- using free public RPC for base (rate limits may apply)` |
| 6 | gateway | INFO | CoinGecko fallback mode active | `No CoinGecko API key -- using on-chain pricing with free CoinGecko as fallback` |
| 7 | gateway | INFO | ALMANAK price confidence 0.90 (1 of 2 sources) | `ALMANAK/USD: 0.00201755 (confidence: 0.90, sources: 1/2, outliers: 0)` |
| 8 | gateway | WARNING | Anvil port not freed within 5s after strategy exit | `Port 61479 not freed after 5.0s` |

**Notes on findings:**

- **Findings 1-4 (Token resolution warnings):** Emitted during gateway market service
  initialization when the price aggregator pre-warms its source list. BTC, WBTC, STETH, and
  CBETH are not tokens this strategy uses -- noise from a generic startup probe. Indicates
  these tokens are missing from the Base token registry (registry gap), but non-fatal for
  this strategy.
- **Findings 5-6 (Public RPC / CoinGecko fallback):** Expected in a dev/test environment
  without `ALCHEMY_API_KEY` or `ALMANAK_GATEWAY_COINGECKO_API_KEY`. Both fallbacks functioned
  correctly.
- **Finding 7 (Price confidence 0.90):** ALMANAK has only 1/2 price sources available; no
  Chainlink oracle exists for this small-cap token. Price sourced solely via GeckoTerminal.
  Acceptable for testing; worth noting for production risk assessment.
- **Finding 8 (Port not freed):** Minor cleanup timing issue in Anvil fork manager. Does not
  affect test correctness.
- **Finding 9 (slippage=N/A in receipt):** The Uniswap V3 receipt parser shows `slippage=N/A`
  after the ALMANAK swap. This occurs because the parser cannot compute post-execution slippage
  without a reference price for the ALMANAK token (no Chainlink oracle). Non-blocking but a
  data quality gap in the receipt parser for tokens without price oracles.

**No ERROR-severity findings. No zero prices. No transaction reverts.**

## Result

**PASS** - The `almanak_rsi` strategy on Base (Anvil fork) successfully executed its
initialization swap, buying ALMANAK for $10.00 USDC via Uniswap V3. Both the USDC approval
and swap transactions were confirmed on-chain (192,115 gas total). Strategy marked as
initialized and state persisted.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 9
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
