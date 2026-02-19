# Polymarket Arbitrage Strategy (Demo)

A tutorial strategy demonstrating how to detect and trade price discrepancies between related prediction markets on Polymarket.

## What This Strategy Does

This strategy implements a **cross-market arbitrage approach** for prediction markets:

1. **Monitors a pair of related markets** (mutually exclusive outcomes)
2. **Detects when prices are mispriced** relative to each other
3. **YES prices sum > $1.00**: Sells the overpriced outcome (buys NO)
4. **YES prices sum < $1.00**: Buys the underpriced outcome (buys YES)
5. **Prices are fair**: Holds, no action

## Arbitrage Explained

On Polymarket, some markets have mutually exclusive outcomes that together should equal 100% probability. For example:

| Market | YES Price | Fair Price |
|--------|-----------|------------|
| "Bitcoin > $100k by Jan 31" | $0.45 | - |
| "Bitcoin NOT > $100k by Jan 31" | $0.58 | - |
| **Sum** | **$1.03** | $1.00 |

In this example, the markets are **3% overpriced**. An arbitrage opportunity exists!

### Types of Arbitrage

1. **Sum-to-one arbitrage** (this strategy): Mutually exclusive outcomes should sum to 1
2. **Cross-market arbitrage**: Same event priced differently across markets
3. **Time-decay arbitrage**: Markets that resolve to the same outcome

## Quick Start

### Test with Mock Prices

```bash
# Run with default settings (overpriced scenario)
python strategies/demo/polymarket_arbitrage/run_anvil.py

# Test different scenarios
python strategies/demo/polymarket_arbitrage/run_anvil.py --scenario overpriced
python strategies/demo/polymarket_arbitrage/run_anvil.py --scenario underpriced
python strategies/demo/polymarket_arbitrage/run_anvil.py --scenario fair
python strategies/demo/polymarket_arbitrage/run_anvil.py --scenario large_arb
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
python -m src.cli.run --strategy demo_polymarket_arbitrage --once --dry-run
```

## Configuration

Edit `config.json` to customize the strategy:

```json
{
    "market_pair": [
        "market-a-will-happen",
        "market-a-wont-happen"
    ],
    "min_arb_pct": 0.02,
    "trade_size_usd": 10,
    "max_exposure_usd": 100,
    "order_type": "market"
}
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `market_pair` | List of mutually exclusive market IDs | Required |
| `min_arb_pct` | Minimum arbitrage to trade (0.02 = 2%) | 0.02 |
| `trade_size_usd` | USDC amount per trade | 10 |
| `max_exposure_usd` | Maximum total exposure | 100 |
| `order_type` | "market" or "limit" | "market" |

## How It Works

### 1. Strategy Initialization

```python
@almanak_strategy(
    name="demo_polymarket_arbitrage",
    supported_chains=["polygon"],
    supported_protocols=["polymarket"],
)
class PolymarketArbitrageStrategy(IntentStrategy):
    def __init__(self, config, chain, wallet_address):
        self.market_pair = config.get("market_pair", [])
        self.min_arb_pct = config.get("min_arb_pct", 0.02)
        ...
```

### 2. Decision Logic

```python
def decide(self, market: MarketSnapshot) -> Intent | None:
    # Get prices for all markets in the pair
    prices = {}
    for market_id in self.market_pair:
        prices[market_id] = market.prediction_price(market_id, "YES")

    # Calculate sum and detect arbitrage
    price_sum = sum(prices.values())
    arb_pct = abs(price_sum - 1.0)

    # Check if arbitrage is profitable
    if arb_pct < self.min_arb_pct:
        return Intent.hold(reason="No arbitrage opportunity")

    # Find the most mispriced market
    best_market_id, best_mispricing, is_overpriced = self._find_mispriced_market(prices)

    # Trade the mispriced market
    if is_overpriced:
        # Buy NO (equivalent to selling YES)
        return Intent.prediction_buy(
            market_id=best_market_id,
            outcome="NO",
            amount_usd=self.trade_size_usd,
        )
    else:
        # Buy YES (underpriced)
        return Intent.prediction_buy(
            market_id=best_market_id,
            outcome="YES",
            amount_usd=self.trade_size_usd,
        )
```

### 3. Intent Execution

The framework handles:
1. Compiling the Intent to CLOB order parameters
2. Building and signing the order
3. Submitting to Polymarket CLOB API
4. Tracking order status and fills

## File Structure

```
strategies/demo/polymarket_arbitrage/
    __init__.py      - Package exports
    strategy.py      - Main strategy logic
    config.json      - Default configuration
    run_anvil.py     - Test script with mock prices
    README.md        - This file
```

## Understanding Arbitrage Scenarios

### Scenario 1: Overpriced (Sum > $1.00)

```
Market A (YES): $0.62
Market A (NO):  $0.43
Sum:            $1.05 (5% overpriced)
```

**Strategy**: Buy NO on the most overpriced market
- NO shares will increase in value as prices correct
- If market resolves, at least one outcome pays $1.00

### Scenario 2: Underpriced (Sum < $1.00)

```
Market A (YES): $0.55
Market A (NO):  $0.40
Sum:            $0.95 (5% underpriced)
```

**Strategy**: Buy YES on the most underpriced market
- YES shares will increase in value as prices correct
- Buying underpriced probability is +EV

### Scenario 3: Fair (Sum = $1.00)

```
Market A (YES): $0.60
Market A (NO):  $0.40
Sum:            $1.00 (fair)
```

**Strategy**: HOLD
- No arbitrage opportunity
- Wait for prices to deviate

## Finding Arbitrage Opportunities

In production, you would scan for arbitrage opportunities:

```python
from almanak.framework.connectors.polymarket import ClobClient

client = ClobClient(config)
markets = await client.get_markets(filters=MarketFilters(active=True))

# Find related markets (e.g., same event, different timeframes)
related_pairs = find_related_markets(markets)

for pair in related_pairs:
    prices = [m.outcomePrices[0] for m in pair]  # YES prices
    price_sum = sum(prices)

    if abs(price_sum - 1.0) > 0.02:  # 2% threshold
        print(f"Arbitrage found: {pair}, sum={price_sum}")
```

## Risk Considerations

1. **Execution Risk**: Prices may move before your order fills
2. **Liquidity Risk**: Large orders may not fill at expected prices
3. **Timing Risk**: Markets may resolve before arbitrage closes
4. **Gas Costs**: Transaction fees eat into small arbitrage profits

## Key Concepts for Strategy Developers

### 1. Mutually Exclusive Markets

Markets that cannot both be true:
- "Bitcoin > $100k" and "Bitcoin NOT > $100k"
- "Team A wins" and "Team B wins" (in a 2-team game)

### 2. Price Interpretation

- YES at $0.65 = market thinks 65% probability
- NO at $0.35 = market thinks 35% probability
- YES + NO should = $1.00 for related markets

### 3. Arbitrage Profit Calculation

```
Profit = |Price Sum - 1.0| - Trading Costs
```

Example:
- Price sum = $1.05
- Arb opportunity = 5%
- After 0.5% trading costs = 4.5% profit

## Limitations

This is a **demo strategy** for educational purposes:

- Uses mock prices (not real market data)
- Does not scan for arbitrage opportunities
- No sophisticated market relationship detection
- Real strategies need thorough testing and risk management

## Next Steps

1. Read the commented `strategy.py` file
2. Run with different scenarios to understand detection logic
3. Implement market scanning for real opportunities
4. Backtest with historical market data
5. Test with small amounts on real markets

## Support

- Issues: https://github.com/almanak/stack/issues
- Docs: https://docs.almanak.co
