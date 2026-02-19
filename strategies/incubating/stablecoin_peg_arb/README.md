# Stablecoin Peg Arbitrage Strategy

A DeFi strategy that monitors stablecoin prices and profits from depeg events by executing Curve swaps when stablecoins trade away from their $1.00 peg.

## Overview

Stablecoins occasionally deviate from their $1.00 peg due to market volatility, liquidity events, or protocol issues. This strategy detects these depeg events and executes trades to profit from the expected peg restoration.

### Strategy Logic

1. **Monitor**: Continuously monitor stablecoin prices (USDC, USDT, DAI, FRAX)
2. **Detect**: Identify depeg events exceeding the threshold (default 50 bps = 0.5%)
3. **Execute**:
   - If price < $1.00: Buy the cheap stablecoin
   - If price > $1.00: Sell the expensive stablecoin
4. **Profit**: Capture the spread when the stablecoin returns to peg

### Why Curve?

Curve pools are optimized for stablecoin swaps with:
- Very low fees (~4 bps)
- Minimal slippage for stablecoin pairs
- Deep liquidity in pools like 3pool (DAI/USDC/USDT)

## Configuration

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `strategy_id` | str | Unique identifier for this strategy instance |
| `chain` | str | Target blockchain (ethereum, arbitrum) |
| `wallet_address` | str | Wallet address for transactions |

### Stablecoin Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `stablecoins` | ["USDC", "USDT", "DAI", "FRAX"] | Stablecoins to monitor |
| `curve_pools` | ["3pool", "frax_usdc"] | Curve pools to use |

### Depeg Detection (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `depeg_threshold_bps` | 50 | Basis points deviation to trigger opportunity (0.5%) |
| `min_depeg_bps` | 10 | Minimum depeg to consider (filters noise) |
| `max_depeg_bps` | 500 | Maximum depeg - beyond this is too risky |

### Profit Thresholds (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_profit_usd` | $5 | Minimum profit after gas |
| `min_profit_bps` | 5 | Minimum profit in basis points |
| `estimated_gas_cost_usd` | $15 | Estimated gas cost per trade |

### Position Sizing (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_trade_size_usd` | $1,000 | Minimum trade size |
| `max_trade_size_usd` | $100,000 | Maximum trade size |
| `default_trade_size_usd` | $10,000 | Default trade size |

### Slippage Protection (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_slippage_bps` | 30 | Maximum slippage (0.3%) |
| `max_price_impact_bps` | 50 | Maximum price impact (0.5%) |

### Timing (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_cooldown_seconds` | 60 | Cooldown between trades |
| `price_cache_seconds` | 12 | Price cache TTL |
| `opportunity_expiry_seconds` | 30 | Opportunity validity window |

## Example Configuration

```python
from strategies.stablecoin_peg_arb import (
    StablecoinPegArbStrategy,
    StablecoinPegArbConfig,
)

config = StablecoinPegArbConfig(
    strategy_id="peg_arb_mainnet",
    chain="ethereum",
    wallet_address="0x...",

    # Monitor major stablecoins
    stablecoins=["USDC", "USDT", "DAI", "FRAX"],

    # Trigger on 0.5% depeg
    depeg_threshold_bps=50,

    # Profit requirements
    min_profit_usd=Decimal("5"),
    min_profit_bps=5,

    # Position sizing
    default_trade_size_usd=Decimal("10000"),
    max_trade_size_usd=Decimal("50000"),

    # Conservative slippage for stables
    max_slippage_bps=30,
)

strategy = StablecoinPegArbStrategy(config)
```

## Risk Management

### Depeg Risk
- **Max depeg threshold**: Strategy avoids extreme depegs (>5%) which may indicate systemic issues
- **Counterparty selection**: Only swaps with stablecoins that maintain their peg

### Execution Risk
- **Low slippage**: Curve's stableswap curve ensures minimal slippage
- **Trade sizing**: Configurable min/max trade sizes
- **Cooldown**: Prevents overtrading during volatile periods

### Protocol Risk
- **Curve dependency**: Strategy relies on Curve pool liquidity
- **Oracle dependency**: Requires accurate price feeds to detect depegs

## Supported Curve Pools

| Pool | Tokens | Notes |
|------|--------|-------|
| 3pool | DAI, USDC, USDT | Largest stablecoin pool |
| frax_usdc | FRAX, USDC | FRAX ecosystem |
| frax_3crv | FRAX + 3pool | Meta-pool |
| lusd_3crv | LUSD + 3pool | Liquity stablecoin |
| susd | DAI, USDC, USDT, sUSD | Synthetix stablecoin |

## Monitoring

### Strategy Statistics

```python
stats = strategy.get_stats()
# Returns:
# {
#     "state": "monitoring",
#     "total_trades": 5,
#     "total_profit_usd": "125.50",
#     "last_opportunity_found": "USDC below_peg 52bps",
#     "cooldown_remaining": 0,
# }
```

### Depeg Scanning

```python
depegs = strategy.scan_depegs(market)
# Returns:
# [
#     {"token": "USDC", "price": "0.9952", "depeg_bps": 48, "is_opportunity": False},
#     {"token": "USDT", "price": "1.0001", "depeg_bps": 1, "is_opportunity": False},
#     {"token": "DAI", "price": "0.9948", "depeg_bps": 52, "is_opportunity": True},
# ]
```

## Historical Context

Notable stablecoin depeg events that this strategy could have profited from:

- **March 2023**: USDC depegged to ~$0.87 during SVB crisis
- **May 2022**: UST death spiral (too extreme for this strategy)
- **Various**: Minor depegs during high volatility periods

The strategy is designed for moderate depegs (0.5-5%) that are likely to restore, not catastrophic failures.
