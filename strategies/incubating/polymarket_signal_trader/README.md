# Polymarket Signal Trader Strategy (Demo)

A tutorial strategy demonstrating how to build a signal-based trading strategy for prediction markets on Polymarket.

## What This Strategy Does

This strategy implements a **signal-based trading approach** for prediction markets:

1. **Monitors external signals** for a configured market
2. **BULLISH signal + high confidence**: Buys YES shares
3. **BEARISH signal + high confidence**: Buys NO shares
4. **NEUTRAL signal or low confidence**: Holds, no action

## Signal Integration

The strategy uses the Almanak signal framework to aggregate signals from multiple sources:

```python
from almanak.framework.connectors.polymarket.signals import (
    SignalDirection,
    SignalResult,
    aggregate_signals,
)

# Get aggregated signal
signal = aggregate_signals([
    news_provider.get_signal(market_id),
    social_provider.get_signal(market_id),
    model_provider.get_signal(market_id),
])

# Trade based on signal
if signal.direction == SignalDirection.BULLISH and signal.confidence > 0.6:
    return Intent.prediction_buy(market_id=market_id, outcome="YES", ...)
```

## Quick Start

### Test with Mock Signals

```bash
# Run with default settings (bullish signal)
python strategies/demo/polymarket_signal_trader/run_anvil.py

# Force a bearish signal
python strategies/demo/polymarket_signal_trader/run_anvil.py --signal bearish

# Force a neutral signal (should hold)
python strategies/demo/polymarket_signal_trader/run_anvil.py --signal neutral
```

### Run with CLI (Dry Run)

```bash
# Set required environment variables
export ALMANAK_CHAIN=polygon
export ALMANAK_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_PRIVATE_KEY=0x...

# For Polymarket CLOB authentication (optional for dry-run)
export POLYMARKET_API_KEY=your_api_key
export POLYMARKET_SECRET=your_secret
export POLYMARKET_PASSPHRASE=your_passphrase

# Dry run (no real transactions)
python -m src.cli.run --strategy demo_polymarket_signal_trader --once --dry-run
```

## Configuration

Edit `config.json` to customize the strategy:

```json
{
    "market_id": "will-bitcoin-exceed-100000-by-2025",
    "trade_size_usd": 10,
    "min_confidence": 0.6,
    "min_edge": 0.05,
    "order_type": "market",
    "time_in_force": "GTC",
    "stop_loss_pct": 0.20,
    "take_profit_pct": 0.30,
    "exit_before_resolution_hours": 24
}
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `market_id` | Polymarket market ID or slug | Required |
| `trade_size_usd` | USDC amount per trade | 10 |
| `min_confidence` | Minimum signal confidence (0-1) | 0.6 |
| `min_edge` | Minimum edge vs market price | 0.05 (5%) |
| `order_type` | "market" or "limit" | "market" |
| `time_in_force` | "GTC", "IOC", or "FOK" | "GTC" |
| `stop_loss_pct` | Stop-loss percentage | 0.20 (20%) |
| `take_profit_pct` | Take-profit percentage | 0.30 (30%) |
| `exit_before_resolution_hours` | Exit this many hours before resolution | 24 |

## How It Works

### 1. Strategy Initialization

```python
@almanak_strategy(
    name="demo_polymarket_signal_trader",
    supported_chains=["polygon"],
    supported_protocols=["polymarket"],
)
class PolymarketSignalTraderStrategy(IntentStrategy):
    def __init__(self, config, chain, wallet_address):
        self.market_id = config.get("market_id", "")
        self.trade_size_usd = config.get("trade_size_usd", 10)
        ...
```

### 2. Decision Logic

```python
def decide(self, market: MarketSnapshot) -> Intent | None:
    # Get external signal
    signal = self._get_aggregated_signal(self.market_id)

    # Get current market price
    yes_price = market.prediction_price(self.market_id, "YES")

    # Calculate edge (signal probability vs market price)
    signal_prob = self._signal_to_probability(signal)
    edge = abs(signal_prob - yes_price)

    # Check trading conditions
    if signal.confidence < self.min_confidence:
        return Intent.hold(reason="Low confidence")

    if edge < self.min_edge:
        return Intent.hold(reason="Insufficient edge")

    # Generate trading intent
    if signal.direction == SignalDirection.BULLISH:
        return Intent.prediction_buy(
            market_id=self.market_id,
            outcome="YES",
            amount_usd=self.trade_size_usd,
            exit_conditions=self._create_exit_conditions(yes_price),
        )
    elif signal.direction == SignalDirection.BEARISH:
        return Intent.prediction_buy(
            market_id=self.market_id,
            outcome="NO",
            amount_usd=self.trade_size_usd,
            exit_conditions=self._create_exit_conditions(yes_price),
        )

    return Intent.hold(reason="Neutral signal")
```

### 3. Exit Conditions

The strategy sets up automatic exit conditions for position monitoring:

```python
exit_conditions = PredictionExitConditions(
    stop_loss_price=entry_price * 0.80,      # Exit if down 20%
    take_profit_price=entry_price * 1.30,    # Exit if up 30%
    exit_before_resolution_hours=24,          # Exit 24h before resolution
)
```

### 4. Intent Execution

The framework handles:
1. Compiling the Intent to CLOB order parameters
2. Building and signing the order
3. Submitting to Polymarket CLOB API
4. Tracking order status and fills
5. Setting up position monitoring

## File Structure

```
strategies/demo/polymarket_signal_trader/
    __init__.py      - Package exports
    strategy.py      - Main strategy logic
    config.json      - Default configuration
    run_anvil.py     - Test script with mock signals
    README.md        - This file
```

## Implementing Custom Signal Providers

To use real signals, implement the `PredictionSignal` protocol:

```python
from almanak.framework.connectors.polymarket.signals import (
    PredictionSignal,
    SignalResult,
    SignalDirection,
)

class MyCustomSignalProvider:
    """Custom signal provider example."""

    def get_signal(self, market_id: str, **kwargs) -> SignalResult:
        # Your signal logic here
        # Could use: news API, social sentiment, ML models, etc.

        probability = self._predict_probability(market_id)

        if probability > 0.65:
            direction = SignalDirection.BULLISH
            confidence = (probability - 0.5) * 2
        elif probability < 0.35:
            direction = SignalDirection.BEARISH
            confidence = (0.5 - probability) * 2
        else:
            direction = SignalDirection.NEUTRAL
            confidence = 0.5

        return SignalResult(
            direction=direction,
            confidence=confidence,
            source="my_custom_signal",
            metadata={"probability": probability},
        )
```

## Key Concepts for Strategy Developers

### 1. Prediction Market Prices

Prices on Polymarket represent implied probabilities:
- YES at $0.65 = market thinks 65% chance of YES
- NO at $0.35 = market thinks 35% chance of NO
- YES price + NO price = $1.00 (always)

### 2. Edge Calculation

Edge is the difference between your signal's probability and the market price:
- Signal says 80% probability, market says 65% = 15% edge
- Higher edge = more potential profit (but also more risk)

### 3. Order Types

- **Market orders**: Execute immediately at best available price
- **Limit orders**: Only execute at your specified price or better
- **GTC**: Good Till Cancelled (stays open until filled)
- **IOC**: Immediate or Cancel (fills what it can, cancels rest)
- **FOK**: Fill or Kill (must fill entirely or is cancelled)

### 4. Exit Conditions

The PredictionExitConditions class supports:
- `stop_loss_price`: Exit if price drops below this
- `take_profit_price`: Exit if price rises above this
- `trailing_stop_pct`: Dynamic stop that follows price up
- `exit_before_resolution_hours`: Exit before market resolves

## Limitations

This is a **demo strategy** for educational purposes:

- Uses mock signals (not real external data)
- Does not connect to real signal providers
- No backtesting with historical data
- Real strategies need thorough testing and risk management

## Next Steps

1. Read the commented `strategy.py` file
2. Run with mock signals to see decision flow
3. Implement your own signal provider
4. Backtest with historical market data
5. Test with small amounts on real markets

## Support

- Issues: https://github.com/almanak/stack/issues
- Docs: https://docs.almanak.co
