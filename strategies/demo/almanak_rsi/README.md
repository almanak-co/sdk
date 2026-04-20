# ALMANAK RSI Strategy

An RSI-based mean reversion strategy for trading ALMANAK/USDC on Uniswap V3 on the Base chain.

## Overview

This strategy implements a classic RSI mean reversion approach:

- **Buy Signal**: When RSI crosses below 30 (oversold)
- **Sell Signal**: When RSI crosses above 70 (overbought)
- **Hold**: When RSI is between 30-70 (neutral zone)

## Trading Pair

| Parameter | Value |
|-----------|-------|
| Base Token | ALMANAK |
| Base Token Address | `0xdefa1d21c5f1cbeac00eeb54b44c7d86467cc3a3` |
| Quote Token | USDC |
| Quote Token Address | `0x833589fcd6edb6e08f4c7c32d4f71b54bda02913` |
| Pool Address | `0xbDbC38652D78AF0383322bBc823E06FA108d0874` |
| Fee Tier | 3000 (0.3%) |
| Chain | Base |
| DEX | Uniswap V3 |

## Data Source

- **Source**: CoinGecko DEX (GeckoTerminal)
- **Data Type**: OHLCV candlestick data
- **Granularity**: 15-minute candles

## Signal Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| RSI Period | 14 | Number of periods for RSI calculation |
| Overbought Level | 70 | RSI above this triggers SELL |
| Oversold Level | 30 | RSI below this triggers BUY |

## Execution & Risk

| Parameter | Value | Description |
|-----------|-------|-------------|
| Position Sizing | 100% | Full allocation on each trade |
| Cooldown | 1 hour | Minimum time between trades |
| Max Slippage | 1.0% | Maximum acceptable slippage |

## Lifecycle

### Initialization

On first run, the strategy:
1. Does **not** rely on existing balances
2. Buys ALMANAK for exactly **half** of initial USDC capital (10 USDC)
3. This happens regardless of market conditions

### Normal Operation

After initialization:
1. Monitors RSI(14) on 15-minute candles
2. Executes BUY when RSI < 30 (all USDC -> ALMANAK)
3. Executes SELL when RSI > 70 (all ALMANAK -> USDC)
4. Respects 1-hour cooldown between trades

### Teardown

When torn down:
- Sells all ALMANAK positions to USDC
- Supports both SOFT (1% slippage) and HARD (5% slippage) modes

## Configuration

Default configuration in `config.json`:

```json
{
    "initial_capital_usdc": 20,
    "rsi_period": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
    "cooldown_hours": 1,
    "max_slippage_pct": 1.0
}
```

## Dashboard

The strategy includes a custom Streamlit dashboard showing:

1. **RSI Indicator Panel**
   - Current RSI value with gauge
   - Oversold/Overbought levels
   - Zone indicator (Buy/Sell/Hold)

2. **Price Chart**
   - Price over time as line chart
   - RSI chart
   - Buy/Sell signal markers

3. **Performance Metrics**
   - Net P&L (USD)
   - Net P&L (ETH)
   - Net P&L (%)
   - Total Trades

4. **Current Position State**
   - Token balances
   - Initialization status
   - Cooldown status

## Usage

### Dry Run (No Transactions)

```bash
almanak strat run -d strategies/demo/almanak_rsi --once --dry-run
```

### Run on Anvil Fork

```bash
almanak strat run -d strategies/demo/almanak_rsi --network anvil --once
```

### Run Live

```bash
almanak strat run -d strategies/demo/almanak_rsi --once
```

### Run Continuously (Every 15 Minutes)

```bash
almanak strat run -d strategies/demo/almanak_rsi --interval 900
```

## Technical Notes

1. **Token Resolution**: ALMANAK is resolved by address since it may not be in standard token registries.

2. **Data Availability**: GeckoTerminal needs sufficient trading history to provide OHLCV data. New pools may have limited data.

3. **Pool Liquidity**: Trading success depends on available liquidity in the ALMANAK/USDC pool.

4. **Fee Tier**: The pool uses 0.3% fee tier, which is standard for less volatile pairs.

## Files

```text
strategies/demo/almanak_rsi/
├── __init__.py           # Package exports
├── strategy.py           # Main strategy logic
├── config.json           # Default configuration
├── README.md             # This file
└── dashboard/
    ├── metadata.json     # Dashboard metadata
    └── ui.py             # Streamlit dashboard
```

## Dependencies

- Almanak Framework v2
- GeckoTerminal API (free tier)
- Uniswap V3 on Base
