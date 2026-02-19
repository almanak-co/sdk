# E2E Strategy Test Report: sushiswap_lp (Mainnet)

**Date:** 2026-02-10 01:18 PST
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Arbitrum
**Duration:** ~6 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_sushiswap_lp |
| Chain | arbitrum |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Pool | WETH/USDC/3000 |
| Range Width | 10% |
| Amount0 (WETH) | 0.001 |
| Amount1 (USDC) | 3 |
| Fee Tier | 3000 (0.30%) |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | 0.004    | 0.004172   | -      | existing |
| WETH  | 0.001    | 0.000747   | 0.0003 | wrap (deposit) |
| USDC  | 3.0      | 1.284      | ~1.72  | Enso swap (ETH->USDC) |

### Funding Transactions

1. **Wrap ETH → WETH** (0.0003 ETH)
   - TX: [0x670b1d02783f2aad8362501b2c1553d1f64e9cb2cb574d0d3c85c7af07c3f40e](https://arbiscan.io/tx/0x670b1d02783f2aad8362501b2c1553d1f64e9cb2cb574d0d3c85c7af07c3f40e)
   - Gas Used: 41,013
   - Status: SUCCESS

2. **Enso Swap ETH → USDC** (0.0015 ETH → 3.19 USDC)
   - TX: [0xca6a1c0e2ce708df1f1c1d8c3e0ee8f1c4b7afdc23438e7791b9cae22b20118b](https://arbiscan.io/tx/0xca6a1c0e2ce708df1f1c1d8c3e0ee8f1c4b7afdc23438e7791b9cae22b20118b)
   - Gas Used: 314,527
   - Route: ETH → USDC via Odos protocol
   - Output: 3.193837 USDC (min 3.161898)
   - Status: SUCCESS

### Balance Gate Result

After funding:
- WETH: 0.001047 ✓ (required: 0.001)
- USDC: 4.478024 ✓ (required: 3.0)

**GATE: PASS**

## Strategy Execution

Strategy ran with `--network mainnet --once`

### Intents Executed

1. **LP_OPEN** - Open SushiSwap V3 concentrated liquidity position
   - Pool: WETH/USDC/3000 (0.30% fee tier)
   - Amount0: 0.001 WETH
   - Amount1: 3.0 USDC
   - Price Range: 2019.27 - 2231.82 USDC per WETH
   - Tick Range: -200220 to -199200
   - Position ID: **32527**
   - Status: SUCCESS

### Key Log Output

```text
Strategy: SushiSwapLPStrategy
Instance ID: SushiSwapLPStrategy:6e371ddefa14
Chain: arbitrum
Wallet: 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF
Execution: Single run

[info] Forced action: OPEN LP position
[info] LP_OPEN: 0.0010 WETH + 3.0000 USDC, price range [2019.2698 - 2231.8246], ticks [-200220 - -199200]
[info] 📈 SushiSwapLPStrategy:6e371ddefa14 intent: 🏊 LP_OPEN: WETH/USDC/3000 (0.001, 3) [2019 - 2232] via sushiswap_v3
[info] Compiled LP_OPEN intent: WETH/USDC, range [2019.27-2231.82], 1 txs, 350000 gas
[info] Execution successful for SushiSwapLPStrategy:6e371ddefa14: gas_used=514339, tx_count=1
[info] Extracted LP position ID from receipt: 32527

Status: SUCCESS | Intent: LP_OPEN | Gas used: 514339 | Duration: 11537ms
```

### Gateway Log Highlights

```text
[INFO] Transaction submitted: tx_hash=0cff8a3756b4fade3ba48d70fc913232dc42068c2b2ff52b417ce621a911eea7, latency=1432.4ms
[INFO] Waiting for receipt: tx_hash=0cff8a3756b4fade3ba48d70fc913232dc42068c2b2ff52b417ce621a911eea7, timeout=120.0s
[INFO] Transaction confirmed: tx_hash=0cff8a3756b4fade3ba48d70fc913232dc42068c2b2ff52b417ce621a911eea7, block=430356918, gas_used=514339
```

## Transactions

| Intent | TX Hash | Explorer Link | Gas Used | Status |
|--------|---------|---------------|----------|--------|
| LP_OPEN | 0x0cff8a3756b4fade3ba48d70fc913232dc42068c2b2ff52b417ce621a911eea7 | [arbiscan.io](https://arbiscan.io/tx/0x0cff8a3756b4fade3ba48d70fc913232dc42068c2b2ff52b417ce621a911eea7) | 514,339 | SUCCESS ✓ |

## Result

**PASS** - Strategy executed successfully. SushiSwap V3 LP position opened on Arbitrum mainnet.

### Summary

The sushiswap_lp demo strategy successfully:
1. Opened a concentrated liquidity position on SushiSwap V3 (Arbitrum)
2. Deposited 0.001 WETH + 3 USDC into WETH/USDC 0.30% pool
3. Set price range to ±5% around current price (2019.27 - 2231.82)
4. Received position NFT with tokenId 32527
5. Gas used: 514,339 (within expected range for concentrated LP positions)

### Position Details

- **Position ID**: 32527
- **Protocol**: SushiSwap V3 (Uniswap V3 fork)
- **Pool**: WETH/USDC (0.30% fee tier)
- **Tick Lower**: -200220
- **Tick Upper**: -199200
- **Tick Spacing**: 60 (fee tier 3000)
- **Block**: 430356918
- **Timestamp**: 2026-02-10 01:18:36 UTC

### Notes

1. **Result Enrichment**: Position ID (32527) was automatically extracted by the SushiSwap V3 receipt parser and attached to the result object.
2. **Warning in logs**: `'GatewayExecutionResult' object has no attribute 'liquidity'` - The strategy's `on_intent_executed()` callback expects a `liquidity` field that is not currently being extracted. This is a minor issue that doesn't affect execution success.
3. **Config Restoration**: Config.json was restored to original values (`network: "anvil"`, `amount0: "0.01"`, `amount1: "25"`) after test completion.
4. **Funding Strategy**: Used minimal amounts (0.001 WETH + 3 USDC) to demonstrate LP functionality while minimizing cost. Total funding cost: ~$5.50 in assets + ~$0.70 in gas.
