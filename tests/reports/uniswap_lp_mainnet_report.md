# E2E Strategy Test Report: uniswap_lp (Mainnet)

**Date:** 2026-02-10 00:05:01 UTC
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Arbitrum
**Duration:** ~16 seconds

## Configuration

| Field | Value |
|-------|-------|
| Strategy | uniswap_lp |
| Chain | arbitrum |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Pool | WETH/USDC (0.05% fee) |
| Range Width | 20% |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | gas      | 0.000732   | 0      | existing |
| WETH  | 0.001    | 0.001247   | 0      | existing |
| USDC  | 3.0      | 3.60       | 0      | existing |

Funding TX(s): None (wallet already funded)

## Strategy Execution

- Strategy ran with `--network mainnet --once`
- Strategy detected no active position (stale state had been cleared)
- Strategy created LP_OPEN intent for WETH/USDC pool
- Intents executed: **LP_OPEN** (SUCCESS)

### Key Log Output
```text
[2026-02-09T17:05:01.314937Z] [info] Initialized IntentStrategy on arbitrum with wallet 0x54776446...
[2026-02-09T17:05:01.314990Z] [info] UniswapLPStrategy initialized: pool=WETH/USDC/500, range_width=20.0%, amounts=0.001 WETH + 3 USDC
[2026-02-09T17:05:09.724339Z] [info] No position found - opening new LP position
[2026-02-09T17:05:09.846754Z] [info] 💧 LP_OPEN: 0.0010 WETH + 3.0000 USDC, range [$1,885.47 - $2,304.47]
[2026-02-09T17:05:10.083081Z] [info] Compiled LP_OPEN intent: WETH/USDC, range [1885.47-2304.47], 4 txs, 590000 gas
[2026-02-09T17:05:17.645419Z] [info] Execution successful for demo_uniswap_lp: gas_used=558199, tx_count=4
[2026-02-09T17:05:17.645641Z] [info] Extracted LP position ID from receipt: 5296102
[2026-02-09T17:05:17.645714Z] [info] LP position opened successfully: position_id=5296102
```

### Gateway Log Highlights
```text
2026-02-10 00:04:28,596 - almanak.gateway.server - INFO - Gateway gRPC server started on 127.0.0.1:50051
2026-02-10 00:05:14,175 - almanak.framework.execution.submitter.public - INFO - Transaction submitted: tx_hash=f5aa1e384d6407ee63a1139235ab49327e57290226572f2fd587b90df6f931da, latency=890.0ms
2026-02-10 00:05:14,931 - almanak.framework.execution.submitter.public - INFO - Transaction submitted: tx_hash=b5efa6ee47a4e1c068e538cc5e7e3f690ddf9c5b2dd6b9a52318628b11547865, latency=755.7ms
2026-02-10 00:05:15,675 - almanak.framework.execution.submitter.public - INFO - Transaction submitted: tx_hash=b137af0339dbfa84ebf9450bf44195138c21003671a2aa58f75672c6c2f60f1b, latency=743.3ms
2026-02-10 00:05:16,467 - almanak.framework.execution.submitter.public - INFO - Transaction submitted: tx_hash=96697458d5f3646fc1d5a829d04b0ac42182dbb8d0fbe210565fe06861717dc9, latency=791.8ms
```

## Transactions

| Intent | TX Hash | Explorer Link | Gas Used | Block | Status |
|--------|---------|---------------|----------|-------|--------|
| LP_OPEN (WETH approval) | 0xf5aa1e384d6407ee63a1139235ab49327e57290226572f2fd587b90df6f931da | [Arbiscan](https://arbiscan.io/tx/0xf5aa1e384d6407ee63a1139235ab49327e57290226572f2fd587b90df6f931da) | 41,824 | 430339179 | SUCCESS |
| LP_OPEN (USDC approval) | 0xb5efa6ee47a4e1c068e538cc5e7e3f690ddf9c5b2dd6b9a52318628b11547865 | [Arbiscan](https://arbiscan.io/tx/0xb5efa6ee47a4e1c068e538cc5e7e3f690ddf9c5b2dd6b9a52318628b11547865) | 39,018 | 430339182 | SUCCESS |
| LP_OPEN (Position mint) | 0xb137af0339dbfa84ebf9450bf44195138c21003671a2aa58f75672c6c2f60f1b | [Arbiscan](https://arbiscan.io/tx/0xb137af0339dbfa84ebf9450bf44195138c21003671a2aa58f75672c6c2f60f1b) | 60,976 | 430339185 | SUCCESS |
| LP_OPEN (Add liquidity) | 0x96697458d5f3646fc1d5a829d04b0ac42182dbb8d0fbe210565fe06861717dc9 | [Arbiscan](https://arbiscan.io/tx/0x96697458d5f3646fc1d5a829d04b0ac42182dbb8d0fbe210565fe06861717dc9) | 416,381 | 430339188 | SUCCESS |

**Total Gas Used:** 558,199 gas
**Position ID Created:** 5296102

## Result

**PASS** - Strategy successfully opened a Uniswap V3 LP position on Arbitrum mainnet with real funds.

### Key Achievements
1. Fresh start mode correctly detected no active position (stale state cleared)
2. Strategy compiled LP_OPEN intent with 4 transactions
3. All 4 transactions executed successfully on-chain:
   - WETH approval
   - USDC approval
   - Position NFT mint
   - Liquidity addition
4. Position ID (5296102) extracted from receipt and saved to state
5. Price range: $1,885.47 - $2,304.47 (20% width centered on current price)
6. Total execution time: 16.5 seconds
7. Gas usage: 558,199 gas (vs 590,000 estimated = 95% accuracy)

### Live Position
- **Position ID:** 5296102
- **Pool:** WETH/USDC 0.05%
- **Liquidity:** 0.001 WETH + 3.0 USDC
- **Status:** ACTIVE on Arbitrum mainnet
- **View on Arbiscan:** [Position NFT](https://arbiscan.io/token/0xc36442b4a4522e871399cd717abdd847ab11fe88?a=5296102)

### Notes
- This is a LIVE position with real funds on Arbitrum mainnet
- Position is now collecting fees from WETH/USDC swaps in the 0.05% pool
- Next run of the strategy will detect this position and enter HOLD mode
- Strategy can be run with `--teardown` flag to close the position when desired

---

## Previous Test (2026-02-09 16:59) - FAIL

The previous test failed due to stale state (position 5295534 no longer existed on-chain but was stored in state). After clearing the stale state, the strategy correctly detected no position and opened a new one (5296102).
