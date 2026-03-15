# E2E Strategy Test Report: aave_borrow (Anvil)

**Date:** 2026-03-16 00:20 (kitchen-iter-81)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_aave_borrow |
| Chain | arbitrum |
| Network | Anvil fork (managed, forked via Alchemy) |
| Anvil Port | Managed (auto-selected 58442) |
| Collateral | 0.002 WETH (~$4.19 at $2095/ETH) |
| Borrow Token | USDC |
| LTV Target | 50% |
| Min Health Factor | 2.0 |

## Config Changes Made

| Field | Before | After (test) | Restored |
|-------|--------|--------------|---------|
| force_action | (absent) | "supply" | removed (restored) |

`force_action` was added temporarily to trigger an immediate SUPPLY on the first `--once` run.
Collateral amount: 0.002 WETH (~$4.19 at $2095/ETH). Well under the $1000 budget cap.

## Execution

### Setup
- Strategy runner auto-started managed gateway on 127.0.0.1:50052
- Managed Anvil fork started on auto-assigned port 58442, forked from Arbitrum mainnet block 442116905 via Alchemy
- Wallet auto-funded by managed gateway from `anvil_funding` config: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)

### Strategy Run
- `force_action = "supply"` triggered immediate SUPPLY intent
- Intent compiled to 3 transactions (WETH approve + Aave V3 supply + setUserUseReserveAsCollateral)
- All 3 transactions confirmed on Anvil fork

### Transactions

| TX # | Hash | Block | Gas Used | Status |
|------|------|-------|----------|--------|
| 1 (WETH approve) | `2ccfd270acc7e9a16f51cbba3b68eea1d1a314759c3a9206053d2e0d7498b47e` | 442116908 | 53,440 | SUCCESS |
| 2 (Aave V3 supply) | `a6208c763c83f83608f0066ddff167fc3c82d7fe494aef7dbe94177c3e96ecd3` | 442116909 | 205,598 | SUCCESS |
| 3 (set as collateral) | `450ac7c6d39ff0c06e1074c1b485f4610f01cc1d5867a9b35bf8d49dce5334ad` | 442116910 | 45,572 | SUCCESS |

Total gas used: 304,610

### Key Log Output

```text
Aggregated price for WETH/USD: 2095.17 (confidence: 1.00, sources: 4/4, outliers: 0)
Aggregated price for USDC/USD: 0.9999910000000001 (confidence: 1.00, sources: 4/4, outliers: 0)
Forced action: SUPPLY collateral
SUPPLY intent: 0.0020 WETH to Aave V3
Compiled SUPPLY: 0.0020 WETH to aave_v3 (as collateral) | Txs: 3 | Gas: 530,000
Simulating 3 transaction(s) via eth_estimateGas
Simulation successful: 3 transaction(s), total gas: 728788
Sequential submit: TX 1/3 confirmed (block=442116908, gas=53440)
Sequential submit: TX 2/3 confirmed (block=442116909, gas=205598)
Sequential submit: TX 3/3 confirmed (block=442116910, gas=45572)
EXECUTED: SUPPLY completed successfully
Parsed Aave V3: SUPPLY 2,000,000,000,000,000 to 0x82af...bab1, tx=0xa620...ecd3, 205,598 gas
Enriched SUPPLY result with: supply_amount, a_token_received, supply_rate (protocol=aave_v3, chain=arbitrum)
Supply successful - state: supplied

Status: SUCCESS | Intent: SUPPLY | Gas used: 304610 | Duration: 36128ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | No CoinGecko API key (expected in local dev) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

Finding #1 is expected in local dev environments and is not a functional issue. All 4 price sources (Chainlink, Binance, DexScreener, CoinGecko) returned successful aggregated prices with confidence 1.00. No zero prices, no failed fetches, no reverts, no token resolution errors, no timeouts.

## Result

**PASS** - The aave_borrow strategy successfully compiled and executed a 3-transaction SUPPLY sequence on an Anvil fork of Arbitrum, supplying 0.002 WETH to Aave V3 as collateral. Receipt parsing correctly identified the SUPPLY event, and result enrichment extracted supply_amount, a_token_received, and supply_rate. Strategy transitioned to "supplied" state cleanly.

SUSPICIOUS_BEHAVIOUR_COUNT: 1
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
