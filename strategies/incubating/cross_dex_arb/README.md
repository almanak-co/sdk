# Cross-DEX Spot Arbitrage Strategy

Atomic arbitrage strategy that captures price differences across DEXs using flash loans.

## Overview

This strategy identifies price discrepancies between DEXs (Uniswap V3, Curve, Enso) and executes atomic arbitrage trades using flash loans for capital efficiency. The entire arbitrage sequence executes in a single transaction - if any step fails, the entire trade reverts with no loss.

## How It Works

1. **Scan**: Monitor configured token pairs across all DEXs for price differences
2. **Identify**: Find opportunities where buying on one DEX and selling on another is profitable
3. **Execute**: Use flash loans to execute atomic arbitrage:
   - Flash loan the input token (0% fee with Balancer, 0.09% with Aave)
   - Swap to intermediate token on cheaper DEX
   - Swap back to input token on expensive DEX
   - Repay flash loan
   - Keep profit

## Example Arbitrage

```
USDC/WETH price difference detected:
- Uniswap V3: 1 WETH = 2,500 USDC
- Curve: 1 WETH = 2,525 USDC (1% more)

Execution:
1. Flash loan 10,000 USDC from Balancer (0 fee)
2. Swap 10,000 USDC -> 4 WETH on Uniswap V3
3. Swap 4 WETH -> 10,100 USDC on Curve
4. Repay 10,000 USDC flash loan
5. Profit: 100 USDC (~$80 after gas)
```

## Configuration Parameters

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `strategy_id` | string | Unique identifier for this strategy instance |
| `chain` | string | Target blockchain (ethereum, arbitrum, etc.) |
| `wallet_address` | string | Wallet address for transactions |

### Token & DEX Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tokens` | list[str] | `["WETH", "USDC", "USDT", "DAI", "WBTC"]` | Tokens to monitor for arbitrage |
| `dexs` | list[str] | `["uniswap_v3", "curve", "enso"]` | DEXs to compare prices |

### Flash Loan Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `flash_loan_provider` | string | `"auto"` | Flash loan provider: `"aave"`, `"balancer"`, or `"auto"` |
| `flash_loan_priority` | string | `"fee"` | Selection priority: `"fee"`, `"liquidity"`, `"reliability"`, `"gas"` |

### Profit Thresholds (Hot-Reloadable)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_profit_bps` | int | `10` | Minimum profit threshold (10 = 0.1%) |
| `min_profit_usd` | Decimal | `10` | Minimum USD profit after gas |

### Gas Limits (Hot-Reloadable)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_gas_gwei` | int | `100` | Maximum gas price in gwei |
| `max_gas_limit` | int | `500000` | Maximum gas limit per trade |
| `estimated_gas_cost_usd` | Decimal | `20` | Estimated gas cost in USD |

### Trade Sizing (Hot-Reloadable)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_trade_size_usd` | Decimal | `1000` | Minimum trade size |
| `max_trade_size_usd` | Decimal | `100000` | Maximum trade size |
| `default_trade_size_usd` | Decimal | `10000` | Default trade size for scanning |

### Slippage Protection (Hot-Reloadable)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_slippage_bps` | int | `50` | Maximum slippage (50 = 0.5%) |
| `max_price_impact_bps` | int | `100` | Maximum price impact (100 = 1%) |

### Cooldown (Hot-Reloadable)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `trade_cooldown_seconds` | int | `60` | Minimum time between trades |
| `opportunity_cache_seconds` | int | `12` | How long to cache price quotes |

## Example Configuration

```python
from strategies.cross_dex_arb import CrossDexArbStrategy, CrossDexArbConfig

config = CrossDexArbConfig(
    strategy_id="cross-dex-arb-eth-1",
    chain="ethereum",
    wallet_address="0x...",

    # Tokens to arbitrage
    tokens=["WETH", "USDC", "USDT", "DAI"],

    # DEXs to use
    dexs=["uniswap_v3", "curve", "enso"],

    # Flash loan settings
    flash_loan_provider="auto",  # Auto-select cheapest
    flash_loan_priority="fee",   # Prefer 0-fee Balancer

    # Profit requirements
    min_profit_bps=15,           # 0.15% minimum gross profit
    min_profit_usd=Decimal("25"), # $25 minimum net profit

    # Trade sizing
    default_trade_size_usd=Decimal("50000"),

    # Gas limits
    max_gas_gwei=80,
    estimated_gas_cost_usd=Decimal("30"),
)

strategy = CrossDexArbStrategy(config=config)
```

## Supported Chains

- Ethereum
- Arbitrum
- Optimism
- Polygon
- Base

## Supported DEXs

| DEX | Strengths |
|-----|-----------|
| **Uniswap V3** | Deep liquidity, concentrated positions |
| **Curve** | Optimal for stablecoin pairs, low slippage |
| **Enso** | Aggregator, splits orders across DEXs |

## Flash Loan Providers

| Provider | Fee | Liquidity | Notes |
|----------|-----|-----------|-------|
| **Balancer** | 0% | Moderate | Preferred when available |
| **Aave** | 0.09% | High | Battle-tested, most liquidity |

## Risk Considerations

1. **MEV/Frontrunning**: Arbitrage is competitive. Consider private mempools or flashbots.
2. **Gas Spikes**: High gas prices can make opportunities unprofitable. Use `max_gas_gwei`.
3. **Liquidity**: Large trades may have excessive slippage. Monitor `max_price_impact_bps`.
4. **Revert Cost**: Failed transactions still cost gas for the revert.

## Monitoring

The strategy tracks these metrics:
- `total_trades`: Number of executed arbitrages
- `total_profit_usd`: Cumulative profit in USD
- `last_opportunity_found`: Description of last opportunity
- `cooldown_remaining`: Seconds until next trade allowed

Access via `strategy.get_stats()`.

## Dry-Run Testing

```python
# Initialize with dry-run mode
strategy = CrossDexArbStrategy(config=config)

# Manually scan for opportunities
opportunities = strategy.scan_opportunities()

for opp in opportunities:
    print(f"{opp.token_in}/{opp.token_out}: {opp.gross_profit_bps}bps profit")
    print(f"  Buy on {opp.buy_dex}, sell on {opp.sell_dex}")
    print(f"  Net profit: ${opp.net_profit_usd}")
```

## Dependencies

- `src.data.price.MultiDexPriceService` - Multi-DEX price comparison
- `src.connectors.flash_loan.FlashLoanSelector` - Flash loan provider selection
- `src.intents.Intent.flash_loan()` - Flash loan intent creation
