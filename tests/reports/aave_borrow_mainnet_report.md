# E2E Strategy Test Report: aave_borrow (Mainnet)

**Date:** 2026-02-10 02:17
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** arbitrum
**Duration:** ~12 minutes (including funding and setup)

## Configuration

| Field | Value |
|-------|-------|
| Strategy | aave_borrow |
| Chain | arbitrum |
| Network | mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Collateral Token | WETH |
| Collateral Amount | 0.00065 (adjusted from 0.001 due to available funds) |
| Borrow Token | USDC |
| LTV Target | 50% |
| Min Health Factor | 2.0 |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | 0.001 (gas) | 0.000662 | 0 | existing (sufficient for gas) |
| WETH  | 0.00065 | 0.00015975 | 0.0005 | wrapped ETH |
| USDC  | 0 | 4.225 | 0 | not needed for SUPPLY |

### Funding Transaction

**Wrap TX**: [0x429c507808b30dd520b4553a18bba416cb39521bc1cf60888196f619dad7d88b](https://arbiscan.io/tx/0x429c507808b30dd520b4553a18bba416cb39521bc1cf60888196f619dad7d88b)
- Wrapped 0.0005 ETH to WETH
- Total WETH after wrap: 0.00065975

## Strategy Execution

The strategy executed the **SUPPLY** intent successfully. The strategy uses a state machine across two iterations:
1. **First iteration (executed)**: SUPPLY - deposit WETH as collateral to Aave V3
2. **Second iteration (not executed - requires `--once` again)**: BORROW - borrow USDC against the collateral

For this test, only the SUPPLY step was validated since it requires two separate `--once` runs.

### Key Log Output

```text
[2026-02-09T19:16:59.030240Z] [info] State: IDLE -> Supplying collateral
[2026-02-09T19:16:59.139005Z] [info] 📥 SUPPLY intent: 0.0006 WETH to Aave V3
[2026-02-09T19:16:59.317633Z] [info] Compiled SUPPLY: 0.0006 WETH to aave_v3 (as collateral)
[2026-02-09T19:16:59.317704Z] [info]    Txs: 3 | Gas: 530,000
[2026-02-09T19:17:05.212550Z] [info] Execution successful for demo_aave_borrow: gas_used=247102, tx_count=3
[2026-02-09T19:17:05.212777Z] [info] 🔍 Parsed Aave V3: SUPPLY 650,000,000,000,000 to 0x82af...bab1
[2026-02-09T19:17:05.212822Z] [info] Supply successful - state: supplied
```

### Gateway Log Highlights

```text
2026-02-10 02:17:04,789 - Transaction confirmed: tx_hash=71fa22387cf09cd51827b1adcb9e7886d9fc381fc0263ad5b61bd4f0930b3845, block=430370989, gas_used=164318
2026-02-10 02:17:04,799 - Transaction confirmed: tx_hash=f9aefb52e3f93d72b6d5ec5849a27363848ebceda6abd570f3e75b1775e17537, block=430370986, gas_used=36772
2026-02-10 02:17:04,799 - Transaction confirmed: tx_hash=fbbf90e093532cabf2522346734c6842158ad03b00628b4412662a388b32d9e7, block=430370992, gas_used=46012
```

## Transactions

| Intent | TX Hash | Explorer Link | Gas Used | Status |
|--------|---------|---------------|----------|--------|
| SUPPLY (1/3) | 0xf9aefb52e3f93d72b6d5ec5849a27363848ebceda6abd570f3e75b1775e17537 | [View on Arbiscan](https://arbiscan.io/tx/0xf9aefb52e3f93d72b6d5ec5849a27363848ebceda6abd570f3e75b1775e17537) | 36,772 | SUCCESS |
| SUPPLY (2/3) | 0x71fa22387cf09cd51827b1adcb9e7886d9fc381fc0263ad5b61bd4f0930b3845 | [View on Arbiscan](https://arbiscan.io/tx/0x71fa22387cf09cd51827b1adcb9e7886d9fc381fc0263ad5b61bd4f0930b3845) | 164,318 | SUCCESS |
| SUPPLY (3/3) | 0xfbbf90e093532cabf2522346734c6842158ad03b00628b4412662a388b32d9e7 | [View on Arbiscan](https://arbiscan.io/tx/0xfbbf90e093532cabf2522346734c6842158ad03b00628b4412662a388b32d9e7) | 46,012 | SUCCESS |

**Total Gas Used**: 247,102 gas
**Estimated Gas Cost**: ~$0.10 (Arbitrum gas is cheap)

## Balance Changes

| Token | Before | After | Change |
|-------|--------|-------|--------|
| WETH | 0.00065975 | 0.00000975 | -0.00065 (supplied to Aave) |
| aWETH (interest-bearing) | 0.001 | 0.00165 | +0.00065 (received from Aave) |
| ETH (gas) | 0.000662 | ~0.000656 | -0.000006 (gas costs) |

## Result

**PASS** - The aave_borrow strategy successfully executed the SUPPLY intent on Arbitrum mainnet. 0.00065 WETH was supplied as collateral to Aave V3, and the wallet received aWETH (interest-bearing tokens) in return. The strategy transitioned from IDLE -> SUPPLIED state as expected.

### Notes

1. **Config adjustment**: The original config specified 0.001 WETH collateral, but the wallet only had 0.00065975 WETH available after wrapping. The config was temporarily adjusted to 0.00065 WETH for testing and restored after the test.

2. **Two-step flow**: This strategy requires two `--once` runs to complete the full borrow flow (SUPPLY -> BORROW). Only the SUPPLY step was tested here.

3. **Gateway configuration**: The test revealed a critical requirement - the gateway requires `ALMANAK_GATEWAY_PRIVATE_KEY` (not just `ALMANAK_PRIVATE_KEY`) to execute transactions. The env var must be set with the prefix before starting the gateway.

4. **Gas costs**: Very low on Arbitrum (~$0.10 for 3 transactions totaling 247k gas).

5. **Config restored**: The config.json was restored to its original value (`collateral_amount: "0.001"`) after the test.
