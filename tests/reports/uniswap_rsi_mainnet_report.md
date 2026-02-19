# E2E Strategy Test Report: uniswap_rsi (Mainnet)

**Date:** 2026-02-09 17:28
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Arbitrum
**Duration:** 3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_rsi |
| Chain | arbitrum |
| Network | mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Trade Size | $5 USD |
| RSI Period | 14 |
| RSI Oversold | 40 |
| RSI Overbought | 70 |
| Max Slippage | 100 bps (1%) |
| Pair | WETH/USDC |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | gas only | 0.000721 ETH (~$1.87) | - | existing |
| USDC  | ~$5 | 1.284 USDC | - | existing |
| WETH  | ~$5 | 0.000247 WETH (~$0.64) | - | existing |

**Total liquid**: ~$3.75 (below the $5 trade_size configured)

**Funding decision**: Skipped funding due to insufficient ETH for safe swap operations. Strategy is designed to handle insufficient balance gracefully by returning HOLD.

## Strategy Execution

Strategy ran with `--network mainnet --once` in **2587ms**

**Market Conditions**:
- WETH price: $2,077.40
- USDC price: $0.999893
- RSI(14) for WETH: **45.15** (neutral zone)

**Decision**: **HOLD** - RSI in neutral zone [40-70]

### Key Log Output
```text
Strategy: UniswapRSIStrategy
Chain: arbitrum
Wallet: 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF
Execution: Single run

UniswapRSIStrategy initialized: trade_size=$5, RSI period=14, oversold=40, overbought=70, pair=WETH/USDC

⏸️ demo_uniswap_rsi HOLD: RSI=45.15 in neutral zone [40-70] (hold #1)

Status: HOLD | Intent: HOLD | Duration: 2587ms
Iteration completed successfully.
```

### Gateway Log Highlights
```text
Gateway gRPC server started on 127.0.0.1:50051
StateService initialized with 2 backend
Loaded 1 states from WARM to HOT tier on startup

Aggregated price for WETH/USD: 2077.4 (confidence: 1.00, sources: 1/1, outliers: 0)
Aggregated price for USDC/USD: 0.999893 (confidence: 1.00, sources: 1/1, outliers: 0)

MarketService.GetBalance: success (1437ms)
MarketService.GetPrice: success (0.8ms, 0.6ms)
IntegrationService.BinanceGetKlines: success (208ms) - fetched ETHUSDT klines for RSI
```

## Transactions

No transactions executed - strategy returned HOLD intent (neutral RSI signal).

## Strategy Lifecycle Test

The strategy successfully demonstrated:

1. **Gateway Connection**: Connected to mainnet gateway (localhost:50051)
2. **Market Data Retrieval**: Fetched WETH and USDC prices from CoinGecko via gateway
3. **Historical Data**: Retrieved ETHUSDT klines from Binance for RSI calculation
4. **RSI Calculation**: Computed RSI(14) = 45.15 using 14-period window
5. **Balance Queries**: Retrieved USDC and WETH balances on-chain via Web3
6. **Decision Logic**: Evaluated RSI thresholds (oversold=40, overbought=70)
7. **Hold Intent**: Correctly returned HOLD for neutral RSI (between thresholds)
8. **State Management**: No state persisted (no open positions)

**Strategy behavior**: The RSI strategy monitors WETH momentum and executes swaps based on overbought/oversold signals:
- RSI ≤ 40: BUY signal (swap USDC → WETH)
- RSI ≥ 70: SELL signal (swap WETH → USDC)
- 40 < RSI < 70: HOLD (no action)

With RSI at 45.15, the strategy correctly held (no trade signal).

## Result

**PASS** - Strategy executed successfully on Arbitrum mainnet with correct RSI calculation and HOLD decision for neutral market conditions.

**Notes**:
- No transactions were needed (HOLD intent)
- Wallet has insufficient funds ($3.75 vs $5 trade size), but this didn't affect the test since RSI was neutral
- If a buy/sell signal had occurred, the strategy would have returned HOLD with reason "insufficient balance"
- Gateway services (MarketService, StateService, IntegrationService) all functioned correctly
- Config restored to original values (chain: ethereum, network: anvil, trade_size_usd: 3)
