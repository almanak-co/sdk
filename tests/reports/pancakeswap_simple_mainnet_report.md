# E2E Strategy Test Report: pancakeswap_simple (Mainnet)

**Date:** 2026-02-10 01:54
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Arbitrum
**Duration:** 7 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | pancakeswap_simple |
| Chain | arbitrum |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | 0.001    | 0.001220   | 0      | existing |
| WETH  | 0.001    | 0.000547   | 0.000553 | wrap ETH |
| USDC  | 0        | 2.228      | 0      | existing |

**Funding Transactions:**
- Wrap ETH→WETH: [0xcdf0be87945a4ad583b833810bef0f9ad4759594d81bb38d3562aa7a0ac10740](https://arbiscan.io/tx/0xcdf0be87945a4ad583b833810bef0f9ad4759594d81bb38d3562aa7a0ac10740)

**Final Wallet State After Funding:**
- ETH: 0.000667 ETH
- WETH: 0.001100 WETH (~$2.34 at $2127/WETH)
- USDC: 2.228 USDC

## Strategy Execution

Strategy ran with `--network mainnet --once` and executed a simple WETH→USDC swap on PancakeSwap V3.

**Intent Executed:**
- SWAP: 0.0009 WETH → 1.9943 USDC (min: 1.9744 USDC, 1% slippage)

### Key Log Output
```text
Prices: WETH=$2127.98, USDC=$0.999825
Balance: 0.001099610477615206 WETH ($2.34)
Swapping $2 WETH -> USDC via PancakeSwap V3
📈 demo_pancakeswap_simple intent: 🔄 SWAP: $2.00 WETH → USDC (slippage: 1.00%) via pancakeswap_v3
✅ Compiled SWAP: 0.0009 WETH → 1.9943 USDC (min: 1.9744 USDC)
   Slippage: 1.00% | Txs: 2 | Gas: 280,000
Execution successful for demo_pancakeswap_simple: gas_used=204071, tx_count=2
Status: SUCCESS | Intent: SWAP | Gas used: 204071 | Duration: 6748ms
```

### Gateway Log Highlights
```text
Transaction submitted: tx_hash=8f84ad1bf5447e7ef8d09c2e33eb0c2561e3d2b2af4801d49e51199ce5eb70ad, latency=703.7ms
Transaction submitted: tx_hash=4e63200130a2b00af906ca4fd72b946b75ac6e6de9bf482bb4a55caa91a434a3, latency=747.9ms
Transaction confirmed: tx_hash=8f84ad1bf5447e7ef8d09c2e33eb0c2561e3d2b2af4801d49e51199ce5eb70ad, block=430365547, gas_used=53819
Transaction confirmed: tx_hash=4e63200130a2b00af906ca4fd72b946b75ac6e6de9bf482bb4a55caa91a434a3, block=430365550, gas_used=150252
```

## Transactions

| Intent | TX Hash | Explorer Link | Gas Used | Status |
|--------|---------|---------------|----------|--------|
| APPROVE | 0x8f84ad1bf5447e7ef8d09c2e33eb0c2561e3d2b2af4801d49e51199ce5eb70ad | [arbiscan](https://arbiscan.io/tx/0x8f84ad1bf5447e7ef8d09c2e33eb0c2561e3d2b2af4801d49e51199ce5eb70ad) | 53,819 | SUCCESS |
| SWAP | 0x4e63200130a2b00af906ca4fd72b946b75ac6e6de9bf482bb4a55caa91a434a3 | [arbiscan](https://arbiscan.io/tx/0x4e63200130a2b00af906ca4fd72b946b75ac6e6de9bf482bb4a55caa91a434a3) | 150,252 | SUCCESS |

**Total Gas Used:** 204,071 gas

## Final Balances

| Token | Before | After | Change |
|-------|--------|-------|--------|
| ETH   | 0.001220 ETH | 0.000662 ETH | -0.000558 ETH (gas + wrap) |
| WETH  | 0.000547 WETH | 0.000160 WETH | -0.000387 WETH (net after wrap & swap) |
| USDC  | 2.228 USDC | 4.225 USDC | +1.997 USDC |

**Net Swap:** ~0.00094 WETH → ~2.00 USDC (matches $2 target)

## Result

**PASS** - PancakeSwap V3 simple swap strategy executed successfully on Arbitrum mainnet. Strategy swapped $2 worth of WETH for USDC as configured, demonstrating correct price fetching, balance checking, intent compilation, and on-chain execution through the PancakeSwap V3 router.

## Notes

- Gateway startup required explicit `ALMANAK_GATEWAY_ALLOW_INSECURE=true` export (not just in .env)
- Pydantic settings loaded `ALMANAK_GATEWAY_PRIVATE_KEY` from .env automatically
- PancakeSwap V3 adapter correctly compiled 2 transactions (approve + swap)
- Receipt parser successfully parsed swap events
- Config restored to original values (swap_amount_usd: 10, network: anvil) after test
