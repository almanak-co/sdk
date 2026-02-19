# Flash Loan Triangular Arbitrage Strategy

This strategy identifies triangular arbitrage opportunities across DEXs (e.g., ETH->USDC->WBTC->ETH) and executes them atomically using flash loans.

## Overview

Triangular arbitrage exploits price inefficiencies across three or more token pairs. When the exchange rates between tokens on different DEXs create a cycle where you can start with X tokens and end up with more than X tokens, an arbitrage opportunity exists.

### How It Works

1. **Path Generation**: Generate all valid triangular paths from configured tokens
2. **Opportunity Scanning**: For each path, fetch quotes from multiple DEXs
3. **Profitability Analysis**: Calculate net profit after:
   - Flash loan fees (Balancer: 0%, Aave: 0.09%)
   - Cumulative slippage and price impact
   - Estimated gas costs
4. **Atomic Execution**: If profitable, execute all swaps atomically via flash loan

### Example Trade

```
Path: ETH -> USDC -> WBTC -> ETH

1. Flash loan 4 ETH from Balancer (0% fee)
2. Swap 4 ETH -> 10,000 USDC on Uniswap V3 (best rate)
3. Swap 10,000 USDC -> 0.24 WBTC on Curve (best rate)
4. Swap 0.24 WBTC -> 4.04 ETH on Enso (best rate)
5. Repay 4 ETH to Balancer
6. Profit: 0.04 ETH (~$100)

All steps atomic - if any fails, entire trade reverts
```

## Configuration

```python
from strategies.flash_triangular_arb import FlashTriangularArbConfig

config = FlashTriangularArbConfig(
    strategy_id="my_triangular_arb",
    chain="ethereum",
    wallet_address="0x...",

    # Tokens to include in arbitrage paths
    tokens=["WETH", "USDC", "USDT", "DAI", "WBTC"],

    # DEXs to query for quotes
    dexs=["uniswap_v3", "curve", "enso"],

    # Path configuration
    max_hops=3,        # 3 = triangular, 4 = quadrilateral
    min_hops=3,

    # Profit thresholds
    min_profit_bps=10,              # Minimum 0.1% profit
    min_profit_usd=Decimal("10"),   # Minimum $10 net profit

    # Risk parameters
    max_slippage_bps=50,            # Max 0.5% per swap
    max_total_slippage_bps=150,     # Max 1.5% cumulative

    # Flash loan preferences
    flash_loan_provider="auto",     # auto, aave, or balancer
    flash_loan_priority="fee",      # fee, liquidity, reliability, gas
)
```

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tokens` | WETH, USDC, USDT, DAI, WBTC | Tokens for arbitrage paths |
| `dexs` | uniswap_v3, curve, enso | DEXs to query for quotes |
| `max_hops` | 3 | Maximum hops (3=triangular, 4=quadrilateral) |
| `min_profit_bps` | 10 | Minimum profit in basis points (0.1%) |
| `min_profit_usd` | 10 | Minimum profit in USD after gas |
| `max_slippage_bps` | 50 | Maximum slippage per swap (0.5%) |
| `max_total_slippage_bps` | 150 | Maximum cumulative slippage (1.5%) |
| `flash_loan_provider` | auto | Flash loan provider selection |
| `trade_cooldown_seconds` | 60 | Cooldown between trades |

### Hot-Reloadable Parameters

These can be changed at runtime without restarting:
- `min_profit_bps`, `min_profit_usd`
- `max_slippage_bps`, `max_total_slippage_bps`
- `max_gas_gwei`, `estimated_gas_cost_usd`
- `trade_cooldown_seconds`
- `pause_strategy`

## Usage

```python
from strategies.flash_triangular_arb import (
    FlashTriangularArbStrategy,
    FlashTriangularArbConfig,
)
from src.strategies import MarketSnapshot

# Create configuration
config = FlashTriangularArbConfig(
    strategy_id="triangular_arb_1",
    chain="ethereum",
    wallet_address="0x...",
)

# Initialize strategy
strategy = FlashTriangularArbStrategy(config=config)

# Get market snapshot
market = MarketSnapshot(chain="ethereum", wallet_address="0x...")

# Run strategy decision
intent = strategy.decide(market)

# Check strategy state
state = strategy.get_state()
stats = strategy.get_stats()

# Manually scan for opportunities
opportunities = strategy.scan_opportunities()
for opp in opportunities:
    print(f"{opp.path_str}: +{opp.gross_profit_bps}bps, ${opp.net_profit_usd:.2f}")
```

## Strategy States

| State | Description |
|-------|-------------|
| `SCANNING` | Looking for arbitrage opportunities |
| `OPPORTUNITY_FOUND` | Found profitable opportunity |
| `EXECUTING` | Executing flash loan arbitrage |
| `COOLDOWN` | Waiting after trade |

## Path Generation

The strategy generates all valid token paths based on configuration:

### Triangular (3 hops)
With tokens `[ETH, USDC, WBTC]`:
- ETH -> USDC -> WBTC -> ETH
- ETH -> WBTC -> USDC -> ETH
- USDC -> ETH -> WBTC -> USDC
- ... (6 total permutations)

### Quadrilateral (4 hops)
With tokens `[ETH, USDC, WBTC, DAI]` and `max_hops=4`:
- ETH -> USDC -> WBTC -> DAI -> ETH
- ... (24 total permutations)

## Risk Management

1. **Flash Loan Safety**: All trades are atomic - no risk of partial execution
2. **Price Impact Limits**: Maximum cumulative price impact enforced
3. **Slippage Protection**: Per-swap and total slippage limits
4. **Profitability Threshold**: Only executes trades above minimum profit
5. **Trade Cooldown**: Prevents excessive trading
6. **Gas Price Limits**: Can pause if gas exceeds threshold

## Supported Chains

- Ethereum
- Arbitrum
- Optimism
- Polygon
- Base

## Supported Protocols

### DEXs
- Uniswap V3
- Curve
- Enso (aggregator)

### Flash Loan Providers
- Aave V3 (0.09% fee)
- Balancer (0% fee - preferred)

## Testing

```bash
# Run unit tests
pytest strategies/flash_triangular_arb/tests/ -v

# Run specific test class
pytest strategies/flash_triangular_arb/tests/test_strategy.py::TestOpportunityDetection -v
```

## Architecture

```
flash_triangular_arb/
├── __init__.py         # Module exports
├── config.py           # FlashTriangularArbConfig
├── strategy.py         # FlashTriangularArbStrategy
├── README.md           # This file
└── tests/
    ├── __init__.py
    └── test_strategy.py
```

## Performance Considerations

1. **Path Limit**: Use `max_paths_to_evaluate` to limit computation
2. **Cache TTL**: Quotes cached for `opportunity_cache_seconds` (default 12s)
3. **Quote Batching**: Multiple DEXs queried in parallel
4. **Provider Selection**: Auto-select uses fee priority by default

## Common Issues

### No opportunities found
- Check if tokens have sufficient liquidity on configured DEXs
- Lower `min_profit_bps` threshold
- Add more tokens to expand path possibilities

### High slippage rejections
- Increase `max_total_slippage_bps`
- Reduce `default_trade_size_usd`
- Use different token pairs with deeper liquidity

### Flash loan failures
- Verify token is supported by flash loan provider
- Check sufficient liquidity in lending pools
- Try different provider (aave vs balancer)
