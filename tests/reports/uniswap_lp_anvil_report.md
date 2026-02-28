# Anvil Test Report: uniswap_lp Strategy

**Date:** 2026-02-08 15:51
**Result:** PASS
**Duration:** ~15 seconds

---

## Summary

Successfully executed the uniswap_lp demo strategy on a local Anvil fork of Arbitrum mainnet. The strategy opened a Uniswap V3 concentrated liquidity position (NFT ID 5293351) with WETH and USDC tokens. All transactions executed successfully with on-chain verification confirming the position was created.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_lp |
| Chain | Arbitrum |
| Network | Anvil (local fork) |
| Port | 8545 |
| Gateway | localhost:50051 |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |

---

## Strategy Configuration

| Parameter | Value |
|-----------|-------|
| Pool | WETH/USDC/500 |
| Range Width | 20% (±10% from current price) |
| Amount0 (WETH) | 0.0008 |
| Amount1 (USDC) | 1.4 |

---

## Test Phases

### Phase 1: Setup (PASS)
- Killed existing Anvil/Gateway processes
- Started Anvil fork on port 8545 (Arbitrum chain ID: 42161)
- Funded wallet with 100 ETH for gas
- Funded wallet with 10 WETH via wrapping
- Funded wallet with 10,000 USDC via storage slot method
- Started Gateway on port 50051 with Anvil configuration

### Phase 2: Strategy Execution (PASS)
- Strategy initialized successfully
- Detected no existing position (fresh start)
- Created LP_OPEN intent with calculated price range
- Range calculated: $1,898.55 - $2,320.45
- Compiled to 3 transactions (APPROVE x2, LP_OPEN)
- All transactions executed successfully
- Gas used: 557,780 (estimated: 510,000)
- Duration: 7,375ms

### Phase 3: Position Created (PASS)
- Position ID extracted from receipt: 5293351
- Position saved to strategy state
- On-chain verification confirms position exists

### Phase 4: Cleanup (PASS)
- Anvil process killed
- Gateway process killed
- Ports released

---

## Execution Log Highlights

```
Strategy: UniswapLPStrategy
Instance ID: demo_uniswap_lp
Mode: FRESH START (no existing state)
Chain: arbitrum
Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
Execution: Single run

UniswapLPStrategy initialized: pool=WETH/USDC/500, range_width=20.0%, amounts=0.0008 WETH + 1.4 USDC

No position found - opening new LP position
💧 LP_OPEN: 0.0008 WETH + 1.4000 USDC, range [$1,898.55 - $2,320.45]
Compiled LP_OPEN intent: WETH/USDC, range [1898.55-2320.45], 3 txs, 510000 gas
Execution successful for demo_uniswap_lp: gas_used=557780, tx_count=3
Extracted LP position ID from receipt: 5293351
LP position opened successfully: position_id=5293351

Status: SUCCESS | Intent: LP_OPEN | Gas used: 557780 | Duration: 7375ms
```

---

## Transactions

| Action | Type | Gas Used | Status |
|--------|------|----------|--------|
| Approve WETH | APPROVE | ~46,000 | ✅ SUCCESS |
| Approve USDC | APPROVE | ~46,000 | ✅ SUCCESS |
| Open LP Position | LP_OPEN | ~465,780 | ✅ SUCCESS |
| **Total** | | **557,780** | ✅ SUCCESS |

---

## On-Chain Verification

Position Manager: 0xC36442b4a4522E871399CD717aBDD847Ab11FE88
Position ID: 5293351

```
NFT balance of wallet: 5
Position liquidity: 602194695900

Position details:
- token0: 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1 (WETH)
- token1: 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 (USDC)
- fee_tier: 500 (0.05%)
- tick_lower: -200840
- tick_upper: -198830
- liquidity: 602194695900
```

Position successfully created and verified on-chain.

---

## Token Funding Details

| Token | Method | Amount | Status |
|-------|--------|--------|--------|
| ETH | anvil_setBalance | 100 ETH | ✅ |
| WETH | Wrap ETH | 10 WETH | ✅ |
| USDC | Storage slot (slot 9) | 10,000 USDC | ✅ |

---

## Key Learnings

1. **Storage Slot Method**: Successfully used storage slot method for USDC (slot 9 for Arbitrum native USDC)
2. **WETH Wrapping**: Used native ETH wrapping to fund WETH balance
3. **Gateway Integration**: Gateway successfully connected to Anvil fork
4. **Position Extraction**: Receipt parser correctly extracted position ID (5293351)
5. **State Persistence**: Strategy saved position ID to state for future runs
6. **Price Range Calculation**: Correctly calculated 20% range (±10%) around current price

---

## Conclusion

**PASS** - The uniswap_lp strategy executed successfully on Anvil:
- All setup steps completed without errors
- Strategy compiled LP_OPEN intent correctly
- Transactions executed successfully (557,780 gas)
- Position created on-chain with verified liquidity
- State saved for future iterations
- Clean shutdown completed

The strategy is working as designed for opening Uniswap V3 concentrated liquidity positions.
