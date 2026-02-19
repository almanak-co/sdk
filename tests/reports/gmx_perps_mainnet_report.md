# E2E Strategy Test Report: gmx_perps (Mainnet)

**Date:** 2026-02-10 01:48 UTC
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Arbitrum
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | gmx_perps |
| Chain | arbitrum |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | 0.0015   | 0.002355   | 0      | existing |
| WETH  | 0.0005   | 0.000047   | 0.0005 | wrap |

Funding TX(s):
- Wrap ETH->WETH: [0xac146bdd0e33e8210480b2c22f43e18292b9eb2d7e7e9298a249c95a0b44a30c](https://arbiscan.io/tx/0xac146bdd0e33e8210480b2c22f43e18292b9eb2d7e7e9298a249c95a0b44a30c)

## Strategy Execution

- Strategy ran with `--network mainnet --once`
- Intents executed: PERP_OPEN (SUCCESS)
- Position: LONG ETH/USD, $2.13 size, 2.0x leverage, 0.0005 WETH collateral

### Key Log Output
```text
[2026-02-09T18:48:24.523708Z] [info] No open position - opening new position
[2026-02-09T18:48:24.523915Z] [info] 📈 LONG: 0.0005 WETH ($1.06) → $2.13 position @ 2.0x leverage, slippage=2.0%
[2026-02-09T18:48:24.524503Z] [info] 📈 GMXPerpsStrategy:d4624687d688 intent: 📈 PERP_OPEN:  ETH/USD $2.13 (2.0x) via gmx_v2
[2026-02-09T18:48:24.537723Z] [info] Created MARKET_INCREASE order: market=ETH/USD, size=$2.1299700, is_long=True
[2026-02-09T18:48:24.834679Z] [info] Compiled PERP_OPEN intent: LONG ETH/USD, $2.1299700 size, 1 txs, 3900000 gas
[2026-02-09T18:48:27.860015Z] [info] Execution successful for GMXPerpsStrategy:d4624687d688: gas_used=885973, tx_count=1

Status: SUCCESS | Intent: PERP_OPEN | Gas used: 885973 | Duration: 3828ms
```

### Gateway Log Highlights
```text
2026-02-10 01:48:27,303 - almanak.framework.execution.submitter.public - INFO - Transaction submitted: tx_hash=9eadd13bd0e4673daa74e0498db0665287b50a795473181477da7d0d117cbe94, latency=887.0ms
2026-02-10 01:48:27,678 - almanak.framework.execution.submitter.public - INFO - Transaction confirmed: tx_hash=9eadd13bd0e4673daa74e0498db0665287b50a795473181477da7d0d117cbe94, block=430364102, gas_used=885973
2026-02-10 01:48:27,859 - almanak.framework.execution.orchestrator - INFO - ✅ EXECUTED: PERP_OPEN completed successfully
```

## Transactions

| Intent | TX Hash | Explorer Link | Gas Used | Status |
|--------|---------|---------------|----------|--------|
| PERP_OPEN | 0x9eadd13b... | [Arbiscan](https://arbiscan.io/tx/0x9eadd13bd0e4673daa74e0498db0665287b50a795473181477da7d0d117cbe94) | 885973 | SUCCESS |

## Result

**PASS** - GMX V2 perpetual position opened successfully on Arbitrum mainnet. Strategy compiled PERP_OPEN intent, submitted transaction, and confirmed on-chain in block 430364102.
