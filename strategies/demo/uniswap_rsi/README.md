# Uniswap RSI Strategy (Demo)

A tutorial strategy demonstrating how to build an RSI-based trading strategy that executes swaps on Uniswap V3.

## What This Strategy Does

This strategy implements a simple **mean reversion** approach using the RSI (Relative Strength Index) indicator:

1. **When RSI < 30 (Oversold)**: Buys ETH with USDC
2. **When RSI > 70 (Overbought)**: Sells ETH for USDC
3. **When RSI 30-70 (Neutral)**: Holds, no action

## RSI Explained

RSI is a momentum indicator that measures the speed and magnitude of recent price changes. It oscillates between 0 and 100:

- **RSI < 30**: The asset has dropped significantly recently and may be "oversold" (undervalued). This can be a buy signal.
- **RSI > 70**: The asset has risen significantly recently and may be "overbought" (overvalued). This can be a sell signal.
- **RSI 30-70**: The asset is in neutral territory. No strong signal.

## Quick Start

### Test on Anvil (Recommended)

```bash
# Prerequisites: Foundry installed, ALCHEMY_API_KEY in .env

# Run with default settings (forces a BUY signal)
python strategies/demo/uniswap_rsi/run_anvil.py

# Force a SELL signal
python strategies/demo/uniswap_rsi/run_anvil.py --action sell
```

> **Tip: Funding the Anvil Wallet**
>
> If using Claude Code, ask it to fund your wallet with the required tokens:
> ```
> "cast send 100 USDC and 0.05 WETH to Anvil wallet on Arbitrum"
> ```
> Claude Code will use `anvil_setStorageAt` to set token balances for testing.

### Run with CLI

```bash
# Set required environment variables
export ALMANAK_CHAIN=arbitrum
export ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_PRIVATE_KEY=0x...

# Dry run (no real transactions)
almanak strat run --once --dry-run

# Continuous execution every 60 seconds
almanak strat run --interval 60
```

## Configuration

Edit `config.json` to customize the strategy:

```json
{
    "trade_size_usd": 100,      // Amount to trade per signal ($)
    "rsi_period": 14,           // RSI calculation period (candles)
    "rsi_oversold": 30,         // RSI threshold for buy signal
    "rsi_overbought": 70,       // RSI threshold for sell signal
    "max_slippage_bps": 50,     // Max slippage (50 = 0.5%)
    "base_token": "WETH",       // Token to trade
    "quote_token": "USDC"       // Quote token
}
```

## How It Works

### 1. Strategy Initialization

```python
@almanak_strategy(
    name="demo_uniswap_rsi",
    supported_chains=["arbitrum", "ethereum"],
    supported_protocols=["uniswap_v3"],
)
class UniswapRSIStrategy(IntentStrategy):
    def __init__(self, config, chain, wallet_address):
        # Extract config values
        self.trade_size_usd = config.get("trade_size_usd", 100)
        self.rsi_oversold = config.get("rsi_oversold", 30)
        ...
```

### 2. Decision Logic

```python
def decide(self, market: MarketSnapshot) -> Optional[Intent]:
    # Get RSI
    rsi = market.rsi("WETH", period=14)

    if rsi.value < 30:
        # Oversold - BUY
        return Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount_usd=100,
        )
    elif rsi.value > 70:
        # Overbought - SELL
        return Intent.swap(
            from_token="WETH",
            to_token="USDC",
            amount_usd=100,
        )
    else:
        # Neutral - HOLD
        return Intent.hold(reason="RSI in neutral zone")
```

### 3. Intent Execution

The framework handles:
1. Compiling the Intent to transactions (approve + swap)
2. Routing through Uniswap V3
3. Executing with slippage protection
4. Verifying the swap completed

## File Structure

```
strategies/demo/uniswap_rsi/
├── __init__.py      # Package exports
├── strategy.py      # Main strategy logic (heavily commented!)
├── config.json      # Default configuration
├── run_anvil.py     # Test script for Anvil fork
└── README.md        # This file
```

## Key Concepts for Strategy Developers

### 1. The @almanak_strategy Decorator

Registers your strategy and defines metadata:
- `name`: Unique identifier for CLI
- `supported_chains`: Where this strategy can run
- `supported_protocols`: Which DEXs/protocols it uses
- `intent_types`: What actions it can take

### 2. The decide() Method

The core of every strategy. Called each iteration with:
- `market`: Contains prices, RSI, balances, and more
- Returns: An `Intent` (what to do) or `None` (hold)

### 3. Intents

High-level descriptions of what you want to do:
- `Intent.swap(from_token, to_token, amount_usd)`
- `Intent.hold(reason="...")`

The framework compiles these to actual transactions.

### 4. Error Handling

Catch specific exceptions where recovery is possible. Let unexpected errors
propagate to the framework's built-in `STRATEGY_ERROR` handler:
```python
try:
    rsi = market.rsi("WETH")
except ValueError:
    return Intent.hold(reason="RSI data unavailable")
```

## Limitations

This is a **demo strategy** for educational purposes:

- Simple RSI logic may not be profitable in real markets
- No position sizing or portfolio management
- No stop-loss or take-profit logic
- Real strategies need backtesting and risk management

## Next Steps

1. Read the heavily-commented `strategy.py` file
2. Run on Anvil to see it in action
3. Modify parameters and see how behavior changes
4. Use this as a template for your own strategies!

## Support

- Issues: https://github.com/almanak/stack/issues
- Docs: https://docs.almanak.co
