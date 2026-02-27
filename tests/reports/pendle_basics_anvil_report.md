# E2E Strategy Test Report: pendle_basics (Anvil)

**Date:** 2026-02-27 16:23 (latest run)
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~4 minutes

## Run History

| Run Time | Result | Notes |
|----------|--------|-------|
| 2026-02-27 16:23 | FAIL | Corrupt Chainlink WSTETH oracle (ratio=4,909,525x), WSTETH price unavailable |
| 2026-02-27 09:41 | FAIL | Same root cause (ratio=4,928,224x) |
| 2026-02-27 05:33 | PASS | Aggregator accepted corrupt Chainlink before divergence guard was tightened |

The divergence safety guard (added to fix VIB-297) correctly rejects both sources now.
FAIL is the expected outcome until VIB-297 (corrupt Arbitrum WSTETH Chainlink oracle) is resolved.

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pendle_basics |
| Chain | arbitrum |
| Network | Anvil fork (public RPC: arbitrum-one-rpc.publicnode.com) |
| Anvil Port | 64313 (managed, auto-assigned by gateway) |
| Trade Size | 0.01 WSTETH (token-based; ~$25 at real price) |
| Market | PT-wstETH-25JUN2026 (`0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b`) |

## Config Changes Made

None. The `trade_size_token: 0.01` WSTETH is approximately $25 at current price (~$2,491/WSTETH),
well within the $500 cap. The market PT-wstETH-25JUN2026 is active (not expired). No config edits required.

## Execution

### Setup
- [x] Anvil started on port 8545 (chain ID 42161 confirmed)
- [x] Gateway started on port 50052 (managed by strat run, insecure mode for Anvil)
- [x] Wallet funded: 10 ETH (native), 1 WSTETH (slot 1, auto-detected by managed gateway)
- [x] Managed gateway auto-forked Arbitrum at block 436370988

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [ ] SWAP intent NOT executed -- strategy returned HOLD due to WSTETH price failure

### Key Log Output
```text
PendleBasicsStrategy initialized: market=PT-wstETH-25JUN2026, trade_size=0.01 WSTETH, slippage=100bps
ERROR: Extreme price divergence detected across 2 sources:
  min=2491.7, max=12279655661.460794 (ratio=4928224x exceeds limit of 100x).
  This indicates a feed configuration error (wrong units/decimals), not market volatility.
  Raising AllDataSourcesFailed to prevent corrupted price from being used.
ERROR: GetPrice failed for WSTETH/USD: All data sources failed:
  onchain_chainlink: Magnitude outlier: price=12279655661.46079453
  coingecko: Magnitude outlier: price=2491.7 (min=2492, max=1.228e+10, ratio=4928224x)
ERROR: Error in decide(): Cannot determine price for WSTETH/USD
HOLD: Error: Cannot determine price for WSTETH/USD
Status: HOLD | Intent: HOLD | Duration: 2418ms
```

## Root Cause of Failure

**VIB-297 (known bug)**: The Chainlink aggregator for WSTETH on Arbitrum returns a corrupt price
of `$12,279,655,661` (~$12 billion). The real price from CoinGecko is `$2,491.70`. The divergence
ratio is `4,928,224x`, far exceeding the 100x safety limit in the price aggregator. The aggregator
correctly raises `AllDataSourcesFailed`.

This causes `market.price("WSTETH")` to raise `ValueError`, which the strategy's `decide()` method
catches and returns `HOLD`.

The bug is a wrong Chainlink aggregator address for WSTETH on Arbitrum. The Ethereum fix was merged
(PR #401) but the Arbitrum aggregator address is different and still returns a corrupt value.

No on-chain transaction was produced.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | Extreme price divergence -- corrupt Chainlink WSTETH feed on Arbitrum | `Extreme price divergence detected: min=2491.7, max=12279655661.460794 (ratio=4928224x)` |
| 2 | strategy | ERROR | All data sources failed for WSTETH/USD | `GetPrice failed for WSTETH/USD: onchain_chainlink: Magnitude outlier: price=12279655661.46...` |
| 3 | strategy | ERROR | Strategy decide() exception -- WSTETH price unavailable | `Error in decide(): Cannot determine price for WSTETH/USD` |
| 4 | strategy | WARNING | Token resolution: BTC not found on Arbitrum | `token_resolution_error token=BTC chain=arbitrum error_type=TokenNotFoundError` |
| 5 | strategy | WARNING | Token resolution: STETH not found on Arbitrum | `token_resolution_error token=STETH chain=arbitrum error_type=TokenNotFoundError` |
| 6 | strategy | WARNING | Token resolution: RDNT not found on Arbitrum | `token_resolution_error token=RDNT chain=arbitrum error_type=TokenNotFoundError` |
| 7 | strategy | WARNING | Token resolution: MAGIC not found on Arbitrum | `token_resolution_error token=MAGIC chain=arbitrum error_type=TokenNotFoundError` |
| 8 | strategy | WARNING | Token resolution: WOO not found on Arbitrum | `token_resolution_error token=WOO chain=arbitrum error_type=TokenNotFoundError` |

**Notes:**
- **Findings 1-3 (ERROR)**: The WSTETH Chainlink oracle on Arbitrum returns ~$12.3B vs CoinGecko's $2,491.
  The new divergence guard in the price aggregator correctly rejects this. This is VIB-297, first found in
  iteration 20. The Ethereum-chain Chainlink fix was merged (PR #401) but Arbitrum has a different aggregator
  address that is still broken. This strategy cannot execute until the Arbitrum WSTETH oracle is fixed.
- **Findings 4-8 (WARNING)**: BTC, STETH, RDNT, MAGIC, WOO are not in the Arbitrum token registry. These
  appear during MarketService initialization -- background token resolution for the gateway's price cache.
  Not related to this strategy's tokens. Expected operational noise.

## Result

**FAIL** -- The strategy returned HOLD without executing any on-chain transaction. The WSTETH Chainlink
price feed on Arbitrum returns a corrupt value (~$12.3B), which is now correctly rejected by the
divergence guard in the price aggregator (AllDataSourcesFailed). This is VIB-297. The Arbitrum-specific
Chainlink aggregator address for WSTETH needs to be fixed.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 8
SUSPICIOUS_BEHAVIOUR_ERRORS: 3
