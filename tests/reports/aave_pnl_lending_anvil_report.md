# E2E Strategy Test Report: aave_pnl_lending (Anvil)

**Date:** 2026-03-16 00:24
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | aave_pnl_lending |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 59241 (managed, auto-started by strat run) |

### Config Changes Made

- `supply_amount` reduced from `"0.5"` to `"0.4"` to stay safely under the $1000 budget cap (0.4 WETH @ ~$2095 = ~$838).

## Execution

### Setup
- [x] Anvil fork of Arbitrum started (managed gateway auto-started Anvil on port 59241, block 442117850)
- [x] Gateway started on port 50052 (managed mode)
- [x] Wallet funded by managed gateway: 100 ETH, 2 WETH, 10,000 USDC

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Strategy state: `idle -> supplying -> supplied`
- [x] Intent executed: SUPPLY 0.4 WETH to aave_v3 (as collateral)
- [x] 3 transactions executed sequentially (approve + supply + set-as-collateral)

### Transaction Hashes (Anvil fork, not mainnet)

| TX # | Hash | Gas Used | Status |
|------|------|----------|--------|
| 1 (approve) | `0xbde59ac7b622481e5c93e5332046d00115683e464e3be3941f208e403506d568` | 53,452 | SUCCESS |
| 2 (supply) | `0x25868f8f753cee0648c60713964d2d049364552027b1a3da938a10773646712a` | 205,610 | SUCCESS |
| 3 (set collateral) | `0x73458cf456180fc2a7c89a358a80a3952bb7a2532dfee08dab4dc38aca638457` | 45,572 | SUCCESS |

**Total gas used:** 304,634

### Key Log Output
```text
AavePnLLendingStrategy initialized: supply=0.4 WETH, borrow_token=USDC, LTV target=40.0%, drop_threshold=3.00%, rise_threshold=5.00%
Aggregated price for WETH/USD: 2095.875 (confidence: 1.00, sources: 4/4, outliers: 0)
SUPPLY 0.4 WETH at $2095.88
Compiled SUPPLY: 0.4000 WETH to aave_v3 (as collateral) | Txs: 3 | Gas: 530,000
Simulation successful: 3 transaction(s), total gas: 728800
✅ EXECUTED: SUPPLY completed successfully
   Txs: 3 (bde59a...d568, 25868f...712a, 73458c...8457) | 304,634 gas
Enriched SUPPLY result with: supply_amount, a_token_received, supply_rate (protocol=aave_v3, chain=arbitrum)
Status: SUCCESS | Intent: SUPPLY | Gas used: 304634 | Duration: 34825ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | gateway | INFO | No CoinGecko API key | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | gateway | WARNING | Insecure mode | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |

Both findings are expected/normal for local Anvil testing. No zero prices, no API failures, no reverts, no token resolution errors, and no timeouts were observed. The "no CoinGecko key" message is informational — 4/4 price sources still aggregated successfully (`confidence: 1.00, sources: 4/4`). The insecure mode warning is the standard gateway message for Anvil.

## Result

**PASS** - Strategy supplied 0.4 WETH as collateral on Aave V3 (Arbitrum Anvil fork) in 3 sequential transactions with all receipts parsed and enriched correctly. The PnL lending lifecycle is confirmed functional from the `idle` state through `SUPPLY` completion.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
