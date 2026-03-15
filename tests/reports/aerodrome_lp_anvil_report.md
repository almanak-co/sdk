# E2E Strategy Test Report: aerodrome_lp (Anvil)

**Date:** 2026-03-16 00:27 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | aerodrome_lp (`demo_aerodrome_lp`) |
| Chain | base |
| Network | Anvil fork (Base mainnet, block 43403132) |
| Anvil Port | 59749 (auto-assigned by managed gateway) |
| Pool | WETH/USDC volatile |
| Amount0 | 0.001 WETH |
| Amount1 | 0.04 USDC |
| Force Action | open (pre-set in config) |

## Config Changes Made

None. Amounts were already within the $1000 budget cap (0.001 WETH ~$2.10 + 0.04 USDC = ~$2.14 total). `force_action` was already set to `"open"`.

## Execution

### Setup
- Managed gateway auto-started with its own Anvil fork (Base at block 43403132, chain ID 8453, port 59749)
- Gateway started on port 50052
- Wallet `0xf39Fd6e5...` funded by managed gateway: 100 ETH, 1 WETH (slot 3), 10,000 USDC (slot 9) (from `anvil_funding` config)

### Strategy Run
- Strategy executed with `--network anvil --once`
- `force_action=open` triggered LP_OPEN intent immediately
- Prices fetched: WETH/USD = 2095.67 (confidence 1.00, 4/4 sources), USDC/USD = 0.9999775 (confidence 1.00, 4/4 sources)
- Intent compiled to 3 transactions: WETH approve + USDC approve + addLiquidity
- All 3 transactions confirmed sequentially on Anvil fork

### Transactions

| TX # | Action | Hash | Block | Gas Used | Status |
|------|--------|------|-------|----------|--------|
| 1/3 | WETH approve | `0xf77d13c74427addc6b98e0a992b261db301bc9bd87ca4b4129d38063a84fe759` | 43403135 | 46,343 | CONFIRMED |
| 2/3 | USDC approve | `0x7a9aeae5f885c39c01c5b0940f3f40eef465baa2e0372e38515e5f6b5a5fe2a2` | 43403136 | 55,785 | CONFIRMED |
| 3/3 | addLiquidity | `0x0327a3369d169cfdeaebd8b6f6fb3f04e706d0a74b5c620dae73a862efbafff2` | 43403137 | 309,691 | CONFIRMED |

**Total gas used:** 411,819

### Key Log Output
```text
Aggregated price for WETH/USD: 2095.67 (confidence: 1.00, sources: 4/4, outliers: 0)
Aggregated price for USDC/USD: 0.9999775 (confidence: 1.00, sources: 4/4, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WETH + 0.0400 USDC, pool_type=volatile
Built add liquidity: WETH/USDC stable=False, transactions=3
Compiled Aerodrome LP_OPEN intent: WETH/USDC, stable=False, 3 txs (approve + approve + add_liquidity), 312000 gas
Simulation successful: 3 transaction(s), total gas: 445343
Sequential submit: TX 1/3 confirmed (block=43403135, gas=46343)
Sequential submit: TX 2/3 confirmed (block=43403136, gas=55785)
Sequential submit: TX 3/3 confirmed (block=43403137, gas=309691)
EXECUTED: LP_OPEN completed successfully
Txs: 3 (f77d13...e759, 7a9aea...e2a2, 0327a3...fff2) | 411,819 gas
Parsed Aerodrome add liquidity: token0/token1, tx=0x0327...fff2, 309,691 gas
Enriched LP_OPEN result with: liquidity (protocol=aerodrome, chain=base)
Aerodrome LP position opened successfully
Status: SUCCESS | Intent: LP_OPEN | Gas used: 411819 | Duration: 28083ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | No CoinGecko API key (expected for Anvil dev) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | gateway | WARNING | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |

**Notes:**
- Finding 1: Expected in local dev environment without a CoinGecko API key. Pricing fell back to Chainlink on-chain oracles and succeeded with 4/4 source confidence -- prices are valid and healthy.
- Finding 2: Expected for Anvil dev. The gateway correctly reports insecure mode is acceptable here.
- No zero prices, no API failures, no reverts, no token resolution failures, no timeouts.

## Result

**PASS** -- The `aerodrome_lp` strategy compiled an `LP_OPEN` intent and executed 3 on-chain transactions (WETH approve, USDC approve, addLiquidity) on an Anvil-forked Base at block 43403132. All 3 transactions confirmed. Prices fetched with full 4/4 source confidence via Chainlink + Binance + DexScreener + CoinGecko. The Aerodrome receipt parser correctly identified the addLiquidity event and the `ResultEnricher` enriched the result with liquidity data.

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
