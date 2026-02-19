# LST Basis Trading Strategy

A strategy that captures premium/discount opportunities in Liquid Staking Tokens (LSTs) relative to ETH.

## Overview

Liquid Staking Tokens (stETH, rETH, cbETH) should trade at or near their fair value relative to ETH. However, market dynamics can cause temporary deviations:

- **Premium**: LST trades above fair value (LST/ETH > 1.0 for rebasing tokens)
- **Discount**: LST trades below fair value (LST/ETH < 1.0 for rebasing tokens)

This strategy monitors these spreads and executes trades when they exceed configurable thresholds, profiting from mean reversion.

## Supported LST Tokens

| Token   | Protocol     | Type         | Fair Value | Curve Pool |
|---------|--------------|--------------|------------|------------|
| stETH   | Lido         | Rebasing     | 1.0 ETH    | steth      |
| wstETH  | Lido         | Accumulating | ~1.15 ETH  | -          |
| rETH    | Rocket Pool  | Accumulating | ~1.08 ETH  | reth       |
| cbETH   | Coinbase     | Accumulating | ~1.05 ETH  | cbeth      |
| frxETH  | Frax         | Rebasing     | 1.0 ETH    | frxeth     |

## Strategy Logic

### Premium Trade (LST > Fair Value)
1. Detect stETH trading at $2512.50 when ETH is $2500 (0.5% premium)
2. Swap stETH -> ETH via Curve stETH pool
3. Profit when stETH returns to fair value

### Discount Trade (LST < Fair Value)
1. Detect stETH trading at $2487.50 when ETH is $2500 (0.5% discount)
2. Swap ETH -> stETH via Curve stETH pool
3. Profit when stETH returns to fair value

## Configuration

```python
from strategies.lst_basis import LSTBasisStrategy, LSTBasisConfig

config = LSTBasisConfig(
    # Core settings
    strategy_id="my-lst-basis",
    chain="ethereum",
    wallet_address="0x...",

    # LST tokens to monitor
    lst_tokens=["stETH", "rETH", "cbETH"],

    # Spread thresholds (basis points)
    min_spread_bps=30,      # 0.3% minimum to trigger trade
    min_premium_bps=10,     # 0.1% minimum to consider a premium
    max_spread_bps=500,     # 5% maximum (beyond is too risky)

    # Trade direction
    trade_premium=True,     # Enable selling LST at premium
    trade_discount=True,    # Enable buying LST at discount

    # Profit requirements
    min_profit_usd=10,      # Minimum $10 profit after gas
    min_profit_bps=10,      # Minimum 10 bps profit

    # Position sizing
    default_trade_size_eth=1,   # Default 1 ETH per trade
    max_trade_size_eth=100,     # Maximum 100 ETH

    # Slippage
    max_slippage_bps=50,    # 0.5% max slippage

    # Timing
    trade_cooldown_seconds=120,  # 2 min between trades
)

strategy = LSTBasisStrategy(config=config)
```

## Parameters

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `strategy_id` | str | Unique identifier |
| `chain` | str | Target chain (ethereum) |
| `wallet_address` | str | Wallet for transactions |

### Spread Detection (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_spread_bps` | 30 | Minimum spread to trigger trade |
| `min_premium_bps` | 10 | Minimum to consider a premium/discount |
| `max_spread_bps` | 500 | Maximum spread (beyond is too risky) |

### Trade Direction (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_premium` | True | Enable selling LST at premium |
| `trade_discount` | True | Enable buying LST at discount |

### Profitability (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_profit_usd` | 10 | Minimum USD profit after gas |
| `min_profit_bps` | 10 | Minimum basis points profit |
| `estimated_gas_cost_usd` | 25 | Estimated gas cost |

### Position Sizing (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `default_trade_size_eth` | 1 | Default trade size in ETH |
| `min_trade_size_eth` | 0.1 | Minimum trade size |
| `max_trade_size_eth` | 100 | Maximum trade size |

### Slippage Protection (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_slippage_bps` | 50 | Maximum slippage (0.5%) |
| `max_price_impact_bps` | 100 | Maximum price impact (1%) |

### Timing (Hot-Reloadable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `trade_cooldown_seconds` | 120 | Cooldown between trades |
| `opportunity_expiry_seconds` | 60 | How long an opportunity is valid |

## State Machine

```
MONITORING -> OPPORTUNITY_FOUND -> COOLDOWN -> MONITORING
     ^                                            |
     |____________________________________________|
```

### States

- **MONITORING**: Scanning LST prices for opportunities
- **OPPORTUNITY_FOUND**: Found a tradeable spread
- **EXECUTING**: (Internal) Executing the swap
- **COOLDOWN**: Waiting after a trade

## Risk Management

### Built-in Protections

1. **Spread Limits**: Max spread of 500 bps prevents trading during extreme events
2. **Slippage Protection**: Configurable max slippage (default 50 bps)
3. **Cooldown Period**: Prevents rapid-fire trading
4. **Gas Cost Accounting**: Profit calculation includes gas estimates

### Recommended Settings

For conservative operation:
```python
config = LSTBasisConfig(
    min_spread_bps=50,           # Wait for larger spreads
    min_profit_usd=25,           # Higher profit threshold
    max_trade_size_eth=10,       # Smaller positions
    trade_cooldown_seconds=300,  # 5 min cooldown
)
```

## API Reference

### Methods

```python
# Get current state
state = strategy.get_state()  # LSTBasisState

# Get current opportunity if any
opportunity = strategy.get_current_opportunity()  # Optional[LSTBasisOpportunity]

# Get statistics
stats = strategy.get_stats()  # dict

# Manually scan all LST tokens
basis_data = strategy.scan_basis(market)  # list[dict]

# Clear state and statistics
strategy.clear_state()
```

### Decision Method

```python
# Main decision loop
intent = strategy.decide(market)  # SwapIntent or HoldIntent
```

## Example Output

```python
# Scanning for opportunities
basis_data = strategy.scan_basis(market)
# [
#     {"token": "stETH", "spread_bps": -45, "direction": "discount", "is_opportunity": True},
#     {"token": "rETH", "spread_bps": 12, "direction": "premium", "is_opportunity": False},
#     {"token": "cbETH", "spread_bps": -8, "direction": "discount", "is_opportunity": False},
# ]

# Statistics after trading
stats = strategy.get_stats()
# {
#     "state": "monitoring",
#     "total_trades": 5,
#     "total_profit_usd": "125.50",
#     "total_profit_eth": "0.0502",
#     "cooldown_remaining": 0,
# }
```

## Dependencies

- `src.intents`: Intent framework
- `src.strategies`: Strategy base classes
- `src.models.hot_reload_config`: Hot-reloadable configuration

## Testing

```bash
pytest strategies/lst_basis/tests/ -v
```

## Notes

1. **Fair Value**: For rebasing tokens (stETH), fair value is 1.0. For accumulating tokens (rETH, cbETH), fair value grows over time with staking rewards.

2. **Curve Pools**: Curve pools offer very low slippage (~4 bps) for LST swaps. The strategy prefers Curve when available.

3. **Market Conditions**: Spreads tend to widen during:
   - High volatility
   - Liquidity events (withdrawals enabled, large stakes/unstakes)
   - Market stress (depegging concerns)

4. **Gas Optimization**: Consider batching trades or waiting for lower gas periods.
