# E2E Strategy Test Report: uniswap_lp (Anvil)

**Date:** 2026-02-15 05:54 UTC
**Result:** PASS
**Mode:** Anvil
**Duration:** ~29 seconds

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_lp |
| Chain | arbitrum |
| Network | Anvil fork |
| Anvil Port | 50146 |
| Fork Block | 432258511 |

## Execution

### Setup
- [x] Anvil started on port 50146
- [x] Gateway started on port 50051
- [x] Wallet funded: USDC (slot 9), WETH (slot 51)
- [x] Position ID tracking initialized

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] LP_OPEN intent generated: 0.001 WETH + 3 USDC
- [x] Price range calculated: $1,882.98 - $2,301.42
- [x] Intent compiled: 3 transactions, 660,000 gas estimated

### On-Chain Execution
- [x] 3 transactions submitted and confirmed
  - TX 1: Approval (gas: 53,440)
  - TX 2: Approval (gas: 55,437)
  - TX 3: LP position mint (gas: 417,939)
- [x] Total gas used: 526,816
- [x] Position ID extracted from receipt: 5308773

### Key Log Output
```text
LP_OPEN: 0.0010 WETH + 3.0000 USDC, range [$1,882.98 - $2,301.42]
Compiled LP_OPEN intent: WETH/USDC, range [1882.98-2301.42], 3 txs, 660000 gas
Transaction confirmed: tx_hash=293636e9..., block=432258514, gas_used=53440
Transaction confirmed: tx_hash=e03ff952..., block=432258515, gas_used=55437
Transaction confirmed: tx_hash=bc35e333..., block=432258516, gas_used=417939
✅ EXECUTED: LP_OPEN completed successfully
Txs: 3 (293636...7f9c, e03ff9...e091, bc35e3...2cc2) | 526,816 gas
Extracted LP position ID from receipt: 5308773
Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity
LP position opened successfully: position_id=5308773
```

## Transactions

| Intent | TX Hash (abbrev) | Gas Used | Status |
|--------|------------------|----------|--------|
| LP_OPEN (approve) | 293636e9...7f9c | 53,440 | SUCCESS |
| LP_OPEN (approve) | e03ff952...e091 | 55,437 | SUCCESS |
| LP_OPEN (mint) | bc35e333...2cc2 | 417,939 | SUCCESS |

**Total Gas:** 526,816

## Result Enrichment Verified

The strategy successfully demonstrates result enrichment:
- Position ID automatically extracted: 5308773
- Additional data enriched: tick_lower, tick_upper, liquidity
- Position saved to persistent state for future runs
- Strategy callback `on_intent_executed()` received position ID without manual parsing

## Result

**PASS** - Strategy completed successfully with 3 on-chain transactions. LP position opened on Uniswap V3 (WETH/USDC/500 pool) with position ID 5308773. Result enrichment working correctly.
